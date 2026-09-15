"""RocketMQ 5.x gRPC 直连消费（worker 流模式）：SimpleConsumer 收一批 → clean →
enrich → index，index 成功才 ack。

与文件路径（批次文件夹三阶段）共用同一套处理核心（extract_record /
dedup_enrich / build_node + upsert），区别只在状态载体——文件路径以目录为状态
（阶段可独立重跑），流路径全程内存、以 MQ 重投为恢复机制。

    消费循环（单线程串行，一批即一波）：
        receive(max_message_num, invisible_duration)   # 长轮询，空转等待
        clean（毫秒级，就地完成；失败即终态进死信）
        波处理：URL 去重 → enrich 并发池 → index upsert（_process_wave）
        ack(message)  # 处理成功才确认；不 ack 的消息在 invisible_duration
                      # 到期后重新可见 = at-least-once 重投

可靠性（与 4.x 回调模式等价，机制不同）：
- 重投无 %RETRY%/%DLQ% 跳转，靠不可见时间到期重新可见。invisible_duration 必须
  大于最慢一波的处理时长（默认 600s），否则处理中的消息提前重见造成重复处理
  （无害：node_id 去重 + upsert 幂等，只是白花 LLM 钱）；
- 数据级终态失败（非法 JSON / clean 失败 / LLM 判定失败 / 字段超长）写死信后
  仍会 ack——不 ack 就永远重投同一具尸体（4.x 时代有 maxReconsumeTimes 兜底，
  SimpleConsumer 没有）；
- 基础设施故障（Milvus/TEI/LLM 不可达）不 ack，等重新可见即天然重试。

背压：串行 receive——上一波处理完才拉下一波，积压留在 broker 上（MQ 是持久
缓冲）。水平扩展 = 同 group 多实例（pop 消费服务端自动分配队列，无需协调）。

客户端：官方 rocketmq-python-client（纯 gRPC，无 C++ 依赖，Windows/Linux 均可
运行；开发可本地直跑，生产容器化）。
"""
import json
import logging
import time
from collections import Counter

from clean.extractor import DROP_REASONS, ExtractError, extract_record
from clean.loader import canon_row
from common.config import SETTINGS
from common.store import get_store
from common.tags import get_tagstore
from common.utils import dead_letter, ensure_dirs, now_stamp
from enrich.run import apply_verdict, dedup_enrich
from index.run import build_node, upsert_isolated

log = logging.getLogger("stream")

# 消息不可见时长：须大于最慢一波的处理时间（LLM 并发 16 × 一波 64 条最坏几分钟）
INVISIBLE_SECONDS = 600
# 长轮询等待秒数。5.1.3 proxy 实测 bug：>10s（客户端默认 20s）receive 报
# 50001 NullPointerException（ReceiveMessageActivity）——钉在 10 以内，空批即回。
AWAIT_SECONDS = 10


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


def _ack(consumer, m) -> None:
    """ack 单条。失败不必处理：消息会在 invisible 到期后重投（去重+幂等，无害）。"""
    try:
        consumer.ack(m)
    except Exception as e:
        log.warning("ack 失败（等不可见到期重投）: %s", e)


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

    kept = [(it, v) for it, v in rows if v.keep]
    nodes = [build_node(apply_verdict(it, v)) for it, v in kept]
    try:
        inserted, bad_nodes = upsert_isolated(store, nodes)
        stats["inserted"] = inserted
    except Exception as e:
        log.error("index 入库失败，本波 %d 条转重投: %s", len(items), e)
        return [False] * len(items)
    for n in bad_nodes:
        # 数据级坏行（字段超长等，重投无解）：终态死信，不拖累整波
        it, _ = kept[nodes.index(n)]
        deads.append({"stage": "index", "reason": "upsert_rejected", "item": it})

    ok = [True] * len(items)
    if deads:
        stats["dead_letter"] = len(deads)
        try:
            p = dead_letter(SETTINGS.dead_letter_dir, "wave",
                            f"stream_{now_stamp()}", deads)
            log.warning("波内死信 %d 条 → %s", len(deads), p)
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


# ---------------------------------------------------------------- 消费主循环

def run() -> None:
    """流模式入口（常驻）。Ctrl-C 退出时未 ack 的消息由不可见到期重投。"""
    from rocketmq import ClientConfiguration, Credentials, SimpleConsumer

    ensure_dirs(SETTINGS.dead_letter_dir)
    creds = Credentials(SETTINGS.rocketmq_access_key, SETTINGS.rocketmq_secret_key)
    consumer = SimpleConsumer(
        ClientConfiguration(SETTINGS.rocketmq_endpoint, creds),
        SETTINGS.rocketmq_group,
        await_duration=AWAIT_SECONDS,
    )
    consumer.startup()
    consumer.subscribe(SETTINGS.rocketmq_topic)
    log.info("流模式启动: topic=%s group=%s endpoint=%s → collection=%s",
             SETTINGS.rocketmq_topic, SETTINGS.rocketmq_group, SETTINGS.rocketmq_endpoint,
             SETTINGS.collection)
    try:
        while True:
            try:
                # 一批即一波：batch_size 限在飞量，串行 receive 即背压
                msgs = consumer.receive(SETTINGS.batch_size, INVISIBLE_SECONDS)
            except Exception as e:
                # proxy/网络瞬断：退避重试，积压在 broker 上不受影响
                log.warning("receive 失败（5s 后重试）: %s", e)
                time.sleep(5)
                continue
            if not msgs:
                continue

            items, done = [], []  # (msg, item) 进波处理 / msg 就地终态待 ack
            for m in msgs:
                body = m.body
                # ---- clean（轻量、就地完成；失败即终态，不进波）----
                try:
                    rec = canon_row(json.loads(body.decode("utf-8")))
                except Exception:
                    if _dead({"stage": "ingest", "reason": "invalid_json",
                              "body": body.decode("utf-8", "replace")[:2000]}):
                        done.append(m)
                    continue
                try:
                    item = extract_record(rec)
                except ExtractError as e:
                    if e.reason in DROP_REASONS:
                        log.debug("[stream] 丢弃无价值记录: %s", e.reason)
                        done.append(m)  # 丢弃也是终态
                    elif _dead({"stage": "clean", "reason": e.reason,
                                "record": _slim(rec)}):
                        done.append(m)
                    continue
                items.append((m, item))

            results = _process_wave([it for _, it in items]) if items else []
            # ack：就地终态的 + 波处理成功的；其余不 ack，等不可见到期重投
            for m in done:
                _ack(consumer, m)
            for (m, _), ok in zip(items, results):
                if ok:
                    _ack(consumer, m)
    except KeyboardInterrupt:
        log.info("收到中断，退出（未 ack 的消息由不可见到期重投）")
    finally:
        consumer.shutdown()
