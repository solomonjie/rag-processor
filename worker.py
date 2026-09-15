"""流水线总入口：两条到达路径，共用同一套 clean/enrich/index 处理核心。

    流模式（生产）：配置了 Rocketmq_Endpoint（Proxy gRPC 地址）时直连消费——
        消息 → clean → enrich → index，index 成功才 ack（at-least-once；重投靠
        node_id 去重 + upsert 幂等消化）。积压留在 broker 上，串行 receive 即
        背压；水平扩展 = 同 group 多实例。客户端为官方 gRPC SDK（纯 Python），
        任意 OS 可直跑，生产再容器化。

    文件模式（开发/手工导入）：本地 data/inbox/ → running/<batch>/ 三阶段目录，
        文件位置即状态（目录=待处理，_done/=成功），阶段可独立重跑：
        python -m clean.run|enrich.run|index.run --once

用法：
    python worker.py          # 按配置进流模式（常驻）或文件模式（常驻轮询）
    python worker.py --once   # 文件模式：排空当前所有批次后退出（cron 用）
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


def run_file_mode(once: bool) -> None:
    """文件模式：顺序驱动三阶段 sweep，目录即队列。"""
    log.info("worker 文件模式 inbox=%s running=%s collection=%s%s",
             SETTINGS.inbox_dir, SETTINGS.running_dir, SETTINGS.collection,
             "（--once 模式）" if once else "")
    while True:
        # 顺序：先抽完存量再判定、判完再入库；单轮内多次循环让本轮产物流到底
        progressed = 0
        for stage in (s1, s2, s3):
            try:
                progressed += stage.sweep()
            except Exception as e:
                log.exception("阶段 sweep 异常（%s）: %s", stage.__name__, e)
        if once:
            if progressed == 0:
                break
        elif progressed == 0:
            time.sleep(SETTINGS.poll_interval)
    log.info("worker 结束")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="知识库构建流水线（clean/enrich/index，流模式直连 RocketMQ / 文件模式本地目录）")
    parser.add_argument("--once", action="store_true", help="文件模式：排空当前全部批次后退出")
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s",
    )
    validate()
    ensure_dirs(SETTINGS.inbox_dir, SETTINGS.running_dir, SETTINGS.dead_letter_dir)

    if SETTINGS.rocketmq_endpoint and SETTINGS.rocketmq_topic:
        if args.once:
            raise SystemExit("--once 仅支持文件模式；流模式常驻（不配置 Rocketmq_Endpoint 即文件模式）")
        from common import stream
        stream.run()
        return
    try:
        run_file_mode(args.once)
    except KeyboardInterrupt:
        log.info("收到中断，退出")
        sys.exit(0)


if __name__ == "__main__":
    main()
