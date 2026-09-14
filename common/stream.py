"""RocketMQ 直连消费（worker 流模式）：消息 → clean → enrich → index，index 成功才 ack。

与文件路径（批次文件夹三阶段）的关系：两条到达路径共用同一套处理核心
（extract_record / dedup_enrich / build_node + upsert），区别只在状态载体——
文件路径以目录为状态（阶段可独立重跑），流路径全程内存、以 MQ 重投为恢复机制：

    消费回调线程：JSON 解析 + clean（毫秒级，就地完成；失败即终态进死信）
                 → 记录入波缓冲，回调阻塞等待本波处理结果
    波线程：      取走当前全部待处理记录 → URL 去重 → enrich 并发池 → index upsert
                 → 逐条唤醒回调
    回调返回：    处理完成 CONSUME_SUCCESS；基础设施故障 RECONSUME_LATER → RocketMQ
                 重投（node_id 去重 + upsert 幂等保证重投无害，at-least-once）

积压与背压：处理速度受 LLM 限制，消费线程阻塞等波即天然背压——线程池满后客户端
停止拉取，积压留在 broker 上（MQ 本身就是持久缓冲）。水平扩展 = 同 group 多实例。

坑（rocketmq-client-python 2.0.0）：回调必须返回 ConsumeStatus 枚举，返回 bool 会
被按 0/1 转换且语义相反（True→RECONSUME_LATER，False→CONSUME_SUCCESS=丢数据）。
"""
import itertools
import json
import logging
import threading
import time
from collections import Counter

from clean.extractor import DROP_REASONS, ExtractError, extract_record
from clean.loader import canon_row
from common.config import SETTINGS
from common.store import get_store
from common.tags import get_tagstore
from common.utils import dead_letter, ensure_dirs, now_stamp
from enrich.run import apply_verdict, dedup_enrich
from index.run import build_node

log = logging.getLogger("stream")


def _slim(rec: dict) -> dict:
    """死信里截掉超长 html，保留定位信息（流路径没有 inbox/_done 可回溯全量）。"""
    rec = dict(rec)
    if rec.get("html") and len(str(rec["html"])) > 2000:
        rec["html"] = str(rec["html"])[:2000] + "...(截断)"
    return rec


def _dead(entry: dict) -> bool:
    """单条终态失败落死信。返回是否成功（失败则该消息转重投）。"""
    try:
        p = dead_letter(SETTINGS.dead_letter_dir, entry["stage"],
                        f"stream_{now_stamp()}", [entry])
        log.warning("流死信 1 条（%s）→ %s", entry["reason"], p)
        return True
    except Exception as e:
        log.error("死信落盘失败（本条转重投）: %s", e)
        return False


# ---------------------------------------------------------------- 凑波缓冲

class _Wave:
    """跨回调凑波：回调提交记录后阻塞，波线程处理完这一波再逐条唤醒。

    ack 后置的核心：波内任何一条没处理完，相关回调就不返回（MQ 不会重投）；
    处理失败的条目唤醒时告知 False，由回调返回重投。回调线程数即并发上限，
    pending 天然有界（≤ 消费线程数），无需额外的波大小护栏。
    """

    def __init__(self):
        self._cv = threading.Condition()
        self._ctr = itertools.count()
        self._pending = []   # [(seq, record)]
        self._done = {}      # seq -> bool（True=已处理可 ack / False=转重投）

    def submit(self, item: dict) -> bool:
        with self._cv:
            seq = next(self._ctr)
            self._pending.append((seq, item))
            self._cv.notify_all()
        while True:
            with self._cv:
                if seq in self._done:
                    return self._done.pop(seq)
                self._cv.wait(timeout=5)

    def take(self) -> list:
        """取走当前全部待处理记录（空则阻塞等待；低流量时波小、高流量时自然攒大）。"""
        while True:
            with self._cv:
                if self._pending:
                    items = self._pending
                    self._pending = []
                    return items
                self._cv.wait(timeout=5)

    def settle(self, items: list, results: list) -> None:
        with self._cv:
            for (seq, _), ok in zip(items, results):
                self._done[seq] = ok
            self._cv.notify_all()


# ---------------------------------------------------------------- 波处理

def _process_wave(items: list) -> list:
    """一波记录：URL 去重 → enrich 并发池 → index upsert。返回每条 bool（True=已处理）。

    基础设施故障（tag_collection / Milvus / embedding 不可达）→ 整波 False 转重投，
    服务恢复后重处理（已入库的会被去重跳过，不重复花 LLM 钱）；
    单条级失败（LLM 判定失败等）→ 死信后视为已处理。
    """
    stats = Counter(records=len(items))
    tag_list = get_tagstore().get()
    if not tag_list:
        log.error("tag_collection 为空或不可达，本波 %d 条转重投", len(items))
        return [False] * len(items)
    try:
        store = get_store()
    except Exception as e:
        log.error("连接 Milvus/TEI 失败，本波 %d 条转重投: %s", len(items), e)
        return [False] * len(items)

    existing, fresh, verdicts = dedup_enrich(items, tag_list)
    stats["dedup_skipped"] = len(items) - len(fresh)

    rows, deads = [], []
    vi = 0  # verdicts 与 fresh 同序推进
    for it in items:
        if it["node_id"] in existing:
            continue
        v = verdicts[vi]
        vi += 1
        if isinstance(v, BaseException):
            deads.append({"stage": "enrich", "reason": str(v), "item": it})
        else:
            if not v.keep:
                stats[v.drop_reason] += 1
            rows.append((it, v))

    nodes = [build_node(apply_verdict(it, v)) for it, v in rows if v.keep]
    try:
        stats["inserted"] = store.upsert(nodes)
    except Exception as e:
        log.error("index 入库失败，本波 %d 条转重投: %s", len(items), e)
        return [False] * len(items)

    ok = [True] * len(items)
    if deads:
        stats["dead_letter"] = len(deads)
        try:
            p = dead_letter(SETTINGS.dead_letter_dir, "enrich",
                            f"stream_{now_stamp()}", deads)
            log.warning("enrich 死信 %d 条 → %s", len(deads), p)
        except Exception as e:
            # 死信落盘失败：相关条目转重投（其余条目已入库/已处理）
            log.error("死信落盘失败（相关条目转重投）: %s", e)
            dead_ids = {d["item"]["node_id"] for d in deads}
            ok = [False if it["node_id"] in dead_ids else o for it, o in zip(items, ok)]
    log.info("[stream] %s", _fmt(stats))
    return ok


def _fmt(c: Counter) -> str:
    order = ["records", "dedup_skipped", "inserted",
             "ad", "not_news", "irrelevant", "dead_letter"]
    parts = [f"{k}={c[k]}" for k in order if c[k]]
    return " ".join(parts) or "nothing"


def _wave_loop(wave: _Wave) -> None:
    while True:
        items = wave.take()
        try:
            results = _process_wave([it for _, it in items])
        except Exception:
            log.exception("波处理意外异常（整波转重投）")
            results = [False] * len(items)
        wave.settle(items, results)


# ---------------------------------------------------------------- 消费回调

def _make_callback(cs, wave: _Wave):
    def on_msg(msg):
        # binding 单条投递为 Message；若版本支持批量投递则是 list——统一处理
        msgs = msg if isinstance(msg, (list, tuple)) else [msg]
        ok = True
        for m in msgs:
            body = m.body
            # ---- clean（轻量、就地完成；失败即终态，不进波）----
            try:
                rec = canon_row(json.loads(body.decode("utf-8")))
            except Exception:
                if not _dead({"stage": "ingest", "reason": "invalid_json",
                              "body": body.decode("utf-8", "replace")[:2000]}):
                    ok = False
                continue
            try:
                item = extract_record(rec)
            except ExtractError as e:
                if e.reason in DROP_REASONS:
                    log.debug("[stream] 丢弃无价值记录: %s", e.reason)
                    continue
                if not _dead({"stage": "clean", "reason": e.reason, "record": _slim(rec)}):
                    ok = False
                continue
            # ---- 进入处理波，阻塞到 index 完成 ----
            if not wave.submit(item):
                ok = False
        return cs.CONSUME_SUCCESS if ok else cs.RECONSUME_LATER

    return on_msg


def run() -> None:
    """流模式入口（常驻）。Ctrl-C 退出时未 ack 的消息由 RocketMQ 重投。"""
    from rocketmq.client import ConsumeStatus, PushConsumer  # C++ binding，仅 Linux

    ensure_dirs(SETTINGS.dead_letter_dir)
    wave = _Wave()
    threading.Thread(target=_wave_loop, args=(wave,), name="wave", daemon=True).start()

    consumer = PushConsumer(SETTINGS.rocketmq_group)
    consumer.set_name_server_address(SETTINGS.rocketmq_namesrv)
    # 消费线程数 = 在飞上限：回调阻塞等波处理，线程池满即背压
    try:
        consumer.set_thread_count(max(32, SETTINGS.llm_concurrency * 4))
    except AttributeError:
        pass  # binding 版本没有的调优项，静默跳过
    consumer.subscribe(SETTINGS.rocketmq_topic, _make_callback(ConsumeStatus, wave))
    consumer.start()
    log.info("流模式启动: topic=%s group=%s namesrv=%s → collection=%s",
             SETTINGS.rocketmq_topic, SETTINGS.rocketmq_group, SETTINGS.rocketmq_namesrv,
             SETTINGS.collection)
    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("收到中断，退出（未 ack 的消息由 RocketMQ 重投）")
    finally:
        consumer.shutdown()
