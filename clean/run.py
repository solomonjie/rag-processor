"""阶段1（clean）入口：running/<batch>/inbox/* → running/<batch>/1_cleaned/*.jsonl。

sweep 开始时先收编新批次（本地 data/inbox/ 子目录或散文件）。
文件消费成功移入 inbox/_done/；丢弃项只计数；死信落全局 data/dead_letter/。
本阶段完全本地（trafilatura，无外部服务依赖），可随时安全重跑。
独立运行：python -m clean.run --once
"""
import argparse
import logging
import os
import time
from collections import Counter

from clean.extractor import DROP_REASONS, ExtractError, extract_record
from clean.loader import INPUT_PATTERNS, LoaderError, load_file
from common.batch import CLEANED, INBOX, adopt_arrivals, iter_batches, stage_dir
from common.config import SETTINGS
from common.utils import (dead_letter, ensure_dirs, iter_input_files, move_done,
                          move_unique, now_stamp, write_jsonl)

log = logging.getLogger("clean.run")


class Stats:
    def __init__(self):
        self.c = Counter()

    def add(self, key, n=1):
        self.c[key] += n

    def summary(self) -> str:
        order = ["records", "bad_lines", "items", "dropped_junk", "dead_letter"]
        parts = [f"{k}={self.c[k]}" for k in order if self.c[k]]
        parts += [f"{k}={v}" for k, v in sorted(self.c.items()) if k not in order]
        return " ".join(parts) or "nothing"


def sweep() -> int:
    """处理所有进行中批次的 inbox，返回处理文件数。文件级故障隔离，互不影响。"""
    adopt_arrivals()
    ensure_dirs(SETTINGS.dead_letter_dir)
    done = 0
    for bdir in iter_batches():
        batch = os.path.basename(bdir)
        inbox = stage_dir(bdir, INBOX)
        out_dir = stage_dir(bdir, CLEANED)
        ensure_dirs(inbox, out_dir)
        for path in iter_input_files(inbox, INPUT_PATTERNS):
            name = os.path.basename(path)
            try:
                records, bad = load_file(path)
            except LoaderError as e:
                # 整个文件不可解析：移入死信目录保留原始字节，不影响其他文件
                move_unique(path, SETTINGS.dead_letter_dir)
                log.error("%s/%s 无法解析（%s），已移入死信目录", batch, name, e)
                continue

            stats = Stats()
            stats.add("records", len(records))
            stats.add("bad_lines", bad)
            items, deads = [], []
            for rec in records:
                try:
                    items.append(extract_record(rec))
                except ExtractError as ex:
                    if ex.reason in DROP_REASONS:
                        stats.add("dropped_junk")
                    else:
                        deads.append({"stage": "clean", "reason": ex.reason,
                                      "record": _slim(rec)})
                        stats.add("dead_letter")

            if items:
                write_jsonl(os.path.join(out_dir, f"{now_stamp()}_{name}.jsonl"), items)
                stats.add("items", len(items))
            if deads:
                p = dead_letter(SETTINGS.dead_letter_dir, "clean", f"{batch}_{name}", deads)
                log.warning("clean 死信 %d 条 → %s", len(deads), p)
            move_done(path)
            done += 1
            log.info("[clean] %s/%s | %s", batch, name, stats.summary())
    return done


def _slim(rec: dict) -> dict:
    """死信里截掉超长 html，保留定位信息（原始文件在 inbox/_done 可找回全量）。"""
    rec = dict(rec)
    if rec.get("html") and len(str(rec["html"])) > 2000:
        rec["html"] = str(rec["html"])[:2000] + "...(截断)"
    return rec


def main() -> None:
    parser = argparse.ArgumentParser(description="阶段1 clean：抽取与归一化")
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
    log.info("clean 阶段结束")


if __name__ == "__main__":
    main()
