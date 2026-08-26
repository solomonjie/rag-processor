"""流水线总入口（默认单 worker，D1）：顺序驱动三个阶段，批次文件夹 + 目录即队列。

    MinIO raw/<batch>/（可选）──┐
    data/inbox/<batch>/（本地）─┤
                               ▼
    data/running/<batch>/inbox ──[clean]→ 1_cleaned ──[enrich]→ 2_enriched ──[index]→ Milvus
                               │  三阶段目录全部消费完（文件都在 _done/）
                               ▼
                    删除 MinIO 批次前缀 → 删除 running/<batch>/ 整个目录（死信不删）

状态机（文件位置即状态）：文件在阶段目录=待处理；在 _done/=该阶段成功；
enrich/index 失败文件原地保留、批次不完成；批次完成才整批清理（先远端后本地，崩溃安全）。
阶段可独立运行：python -m clean.run|enrich.run|index.run --once（回放/补跑）。

用法：
    python worker.py          # 常驻
    python worker.py --once   # 排空当前所有批次后退出（cron 用）
"""
import argparse
import logging
import sys
import time

from clean import run as s1
from common.config import SETTINGS, validate
from common.utils import ensure_dirs
from enrich import run as s2
from index import run as s3

log = logging.getLogger("worker")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="知识库构建流水线（clean/enrich/index 三阶段单 worker，批次文件夹制）")
    parser.add_argument("--once", action="store_true", help="排空当前全部批次后退出")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    validate()
    ensure_dirs(SETTINGS.inbox_dir, SETTINGS.running_dir, SETTINGS.dead_letter_dir)

    log.info("worker 启动 inbox=%s running=%s collection=%s%s",
             SETTINGS.inbox_dir, SETTINGS.running_dir, SETTINGS.collection,
             "（--once 模式）" if args.once else "")

    try:
        while True:
            # 顺序：先抽完存量再判定、判完再入库；单轮内多次循环让本轮产物流到底
            progressed = 0
            for stage in (s1, s2, s3):
                try:
                    progressed += stage.sweep()
                except Exception as e:
                    log.exception("阶段 sweep 异常（%s）: %s", stage.__name__, e)
            if args.once:
                if progressed == 0:
                    break
            elif progressed == 0:
                time.sleep(SETTINGS.poll_interval)
    except KeyboardInterrupt:
        log.info("收到中断，退出")
        sys.exit(0)

    log.info("worker 结束")


if __name__ == "__main__":
    main()
