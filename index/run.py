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
from common.utils import iter_input_files, move_done, read_jsonl

log = logging.getLogger("index.run")


def build_node(row: dict) -> TextNode:
    """enrich 产物行 → TextNode（metadata 只放契约约定的显式列）。"""
    v = row["verdict"]
    text = f"标题: {row['title']}\n内容: {row['content']}"
    return TextNode(
        id_=row["node_id"],
        text=text[:65000],
        metadata={
            "url": row["url"][:2000],
            "source": row["source"][:250],
            "published_date": row["published_date"],  # clean 阶段已定长归一化
            "title": row["title"][:500],
            "region": v.get("region") or "",
            "tags": [t for t in v.get("tags") or []][:32],
        },
    )


def sweep() -> int:
    """处理所有进行中批次的 2_enriched，返回处理文件数。写入失败时文件不动，下轮重试。

    sweep 末尾清理完成批次（三阶段目录全空的批次：先删 MinIO 前缀、再删本地目录）。
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

            nodes = []
            for r in rows:
                v = r.get("verdict") or {}
                if v.get("keep"):
                    nodes.append(build_node(r))
                elif v.get("drop_reason"):
                    stats[v["drop_reason"]] += 1  # enrich 已判弃，这里只核对计数

            try:
                n = store.upsert(nodes)
            except Exception as e:
                log.error("[index] %s/%s 入库失败（%d 条待写入，文件保留重试）: %s",
                          batch, name, len(nodes), e)
                finish_batches()  # 其他批次可能已完成（如全被丢弃、无需入库），仍要清理
                return done  # 中止本阶段 sweep，服务恢复后从这里继续
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
