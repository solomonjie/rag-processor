"""阶段3（index）入口：running/<batch>/2_enriched/*.jsonl → Milvus（输入文件移入 2_enriched/_done/）。

- 只入库 verdict.keep == true 的记录（广告/非新闻/不相关在 enrich 阶段已被判弃，这里只过滤计数）；
- text 严格按契约 §4.1：`标题: {title}\n内容: {content}`（消费端按此解析，改格式=跨端同步改）；
- 插入走 llama-index upsert 路径（写 _node_content/_node_type，消费端靠它恢复元数据）；
- upsert 幂等（node_id=md5(url)），写入失败文件留在原地下一轮重试，重试重复写无害。
独立运行：python -m index.run --once
"""
import argparse
import logging
import os
import time
from collections import Counter

from llama_index.core.schema import TextNode

from common.batch import ENRICHED, finish_batches, iter_batches, stage_dir
from common.config import SETTINGS
from common.store import get_store
from common.utils import dead_letter, iter_input_files, move_done, read_jsonl

log = logging.getLogger("index.run")


def _trunc(s: str, limit: int) -> str:
    """按 UTF-8 字节数截断（Milvus VARCHAR max_length 计字节，按字符截会超限，
    实测 500 字符混合中英标题 = 541 字节 > 512 被拒）。解码忽略残缺尾字符。"""
    b = s.encode("utf-8")
    return s if len(b) <= limit else b[:limit].decode("utf-8", "ignore")


# text 列存储预算（字节）：65535 上限减去 _node_content JSON 包装余量（实测纯中文 60000 通过）
TEXT_BUDGET = 60000


def storage_text(title: str, content: str) -> str:
    """契约 §4.1 的存储 text 格式（消费端按 `标题:`/`内容:` 行解析）。"""
    return f"标题: {title}\n内容: {content}"


def build_node(row: dict) -> TextNode:
    """enrich 产物行 → TextNode（metadata 只放契约约定的显式列）。"""
    v = row["verdict"]
    return TextNode(
        id_=row["node_id"],
        # 超长正文直接按字节截尾（Milvus 单行 64KB 物理上限；消费端只用头部，丢的尾巴用不到）
        text=_trunc(storage_text(row["title"], row["content"]), TEXT_BUDGET),
        metadata={
            "url": _trunc(row["url"], 2000),
            "source": _trunc(row["source"], 250),
            "published_date": row["published_date"],  # clean 阶段已定长归一化
            "title": _trunc(row["title"], 2000),
            "region": _trunc(v.get("region") or "", 250),
            "tags": [_trunc(t, 250) for t in v.get("tags") or []][:32],
        },
    )


def upsert_isolated(store, nodes: list) -> tuple:
    """批量 upsert；失败则逐条重试隔离坏行。返回 (成功数, 失败节点列表)。

    单条数据级错误（如字段超长）会让整批 upsert 被拒——若直接整波转重投，
    坏行会随重投反复毒化每一波（实测 54 条循环 20 分钟）。全败视为基础设施
    问题，抛给上层整波重投（重投上限后由 broker %DLQ% 兜底）。
    """
    try:
        return store.upsert(nodes), []
    except Exception as batch_err:
        log.warning("整批入库失败，逐条定位坏行（全败转整波重投）: %s", batch_err)
        ok, bad = 0, []
        for n in nodes:
            try:
                store.upsert([n])
                ok += 1
            except Exception as e1:
                log.error("单条入库失败（数据级问题，转死信）: %s", e1)
                bad.append(n)
        if len(bad) == len(nodes):
            raise batch_err from batch_err
        return ok, bad


def sweep() -> int:
    """处理所有进行中批次的 2_enriched，返回处理文件数。写入失败时文件不动，下轮重试。

    sweep 末尾清理完成批次（三阶段目录全空的批次：删除整个批次目录）。
    """
    batches = iter_batches()
    if not batches:
        return 0
    try:
        store = get_store()
    except Exception as e:  # Milvus 不可达：本阶段跳过，文件保留（与 enrich 的降级一致）
        log.error("连接 Milvus 失败，index 暂不处理（文件保留）: %s", e)
        finish_batches()  # 与入库无关的完成批次（此前已全部 upsert）仍可清理
        return 0

    done = 0
    for bdir in batches:
        batch = os.path.basename(bdir)
        in_dir = stage_dir(bdir, ENRICHED)
        for path in iter_input_files(in_dir, ["*.jsonl"]):
            name = os.path.basename(path)
            rows, bad = read_jsonl(path)
            stats = Counter(records=len(rows), bad_lines=bad)

            nodes, kept_rows = [], []
            for r in rows:
                v = r.get("verdict") or {}
                if v.get("keep"):
                    nodes.append(build_node(r))
                    kept_rows.append(r)
                elif v.get("drop_reason"):
                    stats[v["drop_reason"]] += 1  # enrich 已判弃，这里只核对计数

            try:
                n, bad_nodes = upsert_isolated(store, nodes)
            except Exception as e:
                log.error("[index] %s/%s 入库失败（%d 条待写入，文件保留重试）: %s",
                          batch, name, len(nodes), e)
                finish_batches()  # 其他批次可能已完成（如全被丢弃、无需入库），仍要清理
                return done  # 中止本阶段 sweep，服务恢复后从这里继续
            if bad_nodes:
                # 数据级坏行：终态死信（重试无意义），不阻塞其余行
                deads = [{"stage": "index", "reason": "upsert_rejected",
                          "item": kept_rows[nodes.index(n)]} for n in bad_nodes]
                p = dead_letter(SETTINGS.dead_letter_dir, "index",
                                f"{batch}_{name}", deads)
                log.warning("index 死信 %d 条 → %s", len(deads), p)
                stats["dead_letter"] = len(deads)
            stats["inserted"] = n
            move_done(path)
            done += 1
            log.info("[index] %s/%s | %s", batch, name, _fmt(stats))

    finish_batches()
    return done


def _fmt(c: Counter) -> str:
    order = ["records", "bad_lines", "inserted", "ad", "not_news", "irrelevant"]
    parts = [f"{k}={c[k]}" for k in order if c[k]]
    parts += [f"{k}={v}" for k, v in sorted(c.items()) if k not in order]
    return " ".join(parts) or "nothing"


def main() -> None:
    parser = argparse.ArgumentParser(description="阶段3 index：组装与入库")
    parser.add_argument("--once", action="store_true", help="处理完当前积压后退出")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    while True:
        n = sweep()
        if args.once:
            if n == 0:
                break
        elif n == 0:
            time.sleep(SETTINGS.poll_interval)
    log.info("index 阶段结束")


if __name__ == "__main__":
    main()
