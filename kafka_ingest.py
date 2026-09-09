"""Kafka→MinIO 落盘器（独立进程，与 worker 分离部署，可各自横向扩缩）。

职责唯一：消费 Kafka，把消息按批次上传到 MinIO {bucket}/{Minio_Prefix}/<batch_id>/。
worker 三阶段只依赖 MinIO 上的文件；本进程不落任何本地状态（唯一状态=Kafka offset）。

持久性链：Kafka（offset）→ MinIO（暂存，批次入 Milvus 后由 worker 删除）→ Milvus（最终库）。
崩溃语义：消息**上传 MinIO 成功才允许提交其 offset**，且只提交"连续已上传前沿"——
直通消息立即提交，窗口缓冲消息在 flush 后提交；重放覆盖写同名对象（对象名含
partition+offset），重复消费幂等。

- 消息体：JSON，单对象或数组（自动识别）；解析失败 → deadletter/ 前缀并继续
- batch_id：消息带 batch_id 字段 → 立即上传并提交（假定上游批次已封闭、不再追加）；
  无 batch_id → 按时间窗口聚合（Ingest_Window_Minutes，默认 5 分钟），窗口关闭统一上传后才
  commit（窗口内缓冲在内存，崩溃未 flush 的消息 offset 未提交，由 Kafka 重放）
- --test-write：不连 Kafka，直接写一个样例批次（验证 sink 与 worker 拉取链路）

用法：python kafka_ingest.py            # 常驻消费
      python kafka_ingest.py --test-write [批次名]
"""
import argparse
import json
import logging
import time

from common import minio_source
from common.batch import safe_batch_id
from common.config import SETTINGS

log = logging.getLogger("kafka_ingest")


# ---------------------------------------------------------------- sink

def _put(batch_id: str, name: str, payload: bytes) -> None:
    obj = f"{SETTINGS.minio_prefix.strip('/')}/{safe_batch_id(batch_id)}/{name}"
    minio_source.put_bytes(obj, payload)


def _dead(msg_desc: str, payload: bytes) -> None:
    obj = (f"{SETTINGS.minio_deadletter_prefix.strip('/')}/"
           f"{time.strftime('%Y%m%dT%H%M%S')}_ingest_{safe_batch_id(msg_desc)}.json")
    minio_source.put_bytes(obj, payload)
    log.warning("消息无法解析为 JSON，已入死信: %s", obj)


# ---------------------------------------------------------------- offset 前沿

class Frontier:
    """按 partition 维护"连续已上传 offset"前沿，只提交前沿。

    直通与窗口缓冲可能交错（o=6 缓冲、o=7 直通）：提交 8 会跳过未上传的 6，
    崩溃后 6 永久丢失——所以只有当前沿连续推进时才 commit。
    """

    def __init__(self):
        self.uploaded = {}   # (topic, partition) -> {已上传但尚未越过前沿的 offset}
        self.frontier = {}   # (topic, partition) -> 下一个待消费 offset（已提交）

    def mark_uploaded(self, key, offset: int, consumer) -> None:
        self.uploaded.setdefault(key, set()).add(offset)
        f = self.frontier.get(key)
        if f is None:
            f = offset  # 首见：前沿从该 offset 起
            self.frontier[key] = f
        s = self.uploaded[key]
        while f in s:
            s.discard(f)
            f += 1
        self.frontier[key] = f
        self._commit(consumer)

    def _commit(self, consumer) -> None:
        # kafka-python 的 TopicPartition 只有 (topic, partition) 两字段（带 offset 的是
        # confluent-kafka 的 API）；按分区提交要用 {TopicPartition: OffsetAndMetadata}
        from kafka import TopicPartition
        from kafka.structs import OffsetAndMetadata
        offsets = {TopicPartition(t, p): OffsetAndMetadata(f, "")
                   for (t, p), f in self.frontier.items()}
        try:
            consumer.commit(offsets=offsets)
        except Exception as e:
            log.warning("Kafka commit 失败（重启会重放，覆盖写幂等）: %s", e)


# ---------------------------------------------------------------- 消息处理

def handle_message(value: bytes, msg_desc: str, window_buf: dict,
                   key, offset: int, frontier: Frontier, consumer) -> None:
    """一条消息：JSON 校验失败→死信（视为已处理）；带 batch_id→直传；否则窗口缓冲。"""
    try:
        data = json.loads(value.decode("utf-8", "replace"))  # 只校验可解析；结构由 clean 阶段校验
    except Exception:
        _dead(msg_desc, value)
        frontier.mark_uploaded(key, offset, consumer)  # 死信也上传了，可安全提交
        return
    batch_id = data.get("batch_id") if isinstance(data, dict) else None
    if batch_id:
        _put(str(batch_id), f"{msg_desc}.json", value)  # 直通：假定上游批次已封闭
        frontier.mark_uploaded(key, offset, consumer)
    else:
        window_buf[(key, offset)] = (f"{msg_desc}.json", value)


# ---------------------------------------------------------------- 主循环

def _window_id(ts: float) -> str:
    """当前窗口 id（时间下取整到窗口分钟）。"""
    w = SETTINGS.ingest_window_minutes * 60
    return time.strftime("%Y%m%dT%H%M", time.localtime(ts // w * w))


def run() -> None:
    from kafka import KafkaConsumer

    consumer = KafkaConsumer(
        SETTINGS.kafka_topic,
        bootstrap_servers=SETTINGS.kafka_bootstrap,
        group_id=SETTINGS.kafka_group,
        enable_auto_commit=False,
        auto_offset_reset="earliest",
        consumer_timeout_ms=1000,
    )
    log.info("落盘器启动: topic=%s group=%s → %s/%s/<batch>/",
             SETTINGS.kafka_topic, SETTINGS.kafka_group,
             SETTINGS.minio_bucket, SETTINGS.minio_prefix)

    frontier = Frontier()
    window, buf = None, {}

    def maybe_flush() -> None:
        """窗口翻转：统一上传缓冲并推进前沿。任一上传失败则不提交（Kafka 重放）。"""
        nonlocal window, buf
        wid = _window_id(time.time())
        if window is not None and wid != window and buf:
            for (key, offset), (name, payload) in buf.items():
                _put(window, name, payload)
            log.info("窗口 %s 关闭，上传 %d 个对象", window, len(buf))
            for (key, offset), _ in buf.items():
                frontier.mark_uploaded(key, offset, consumer)
            buf = {}
        window = wid

    try:
        while True:
            for msg in consumer:
                maybe_flush()
                handle_message(msg.value, f"{msg.topic}-{msg.partition}-{msg.offset}",
                               buf, (msg.topic, msg.partition), msg.offset, frontier, consumer)
            maybe_flush()  # 空轮询时也检查（低流量时段冲掉残留缓冲）
    except KeyboardInterrupt:
        if buf:
            log.info("退出前 flush 缓冲（%d 条）", len(buf))
            for (key, offset), (name, payload) in buf.items():
                _put(window, name, payload)
            for (key, offset), _ in buf.items():
                frontier.mark_uploaded(key, offset, consumer)
    finally:
        consumer.close()


# ---------------------------------------------------------------- 测试模式

SAMPLE = [
    {"url": "https://ingest.test/1", "title": "某银行因数据违规被处罚",
     "content": "监管部门对某银行开出罚单，因客户信息保护不到位违反相关规定，罚款金额数百万元，并要求限期整改，这是该行年内第二次收到同类罚单。",
     "publishTime": 1757366400000, "webName": "示例金融报", "region": "北京市"},
    {"url": "https://ingest.test/2", "title": "清仓大甩卖最后三天",
     "content": "亏本清仓全场一折起，老板跑路前最后三天，抓紧时间抢购，走过路过不要错过，买不了吃亏买不了上当。",
     "publishTime": "2026-09-08 09:00:00", "webName": "路边广告"},
    {"url": "https://ingest.test/3", "title": "暴雨致多个小区供水供电中断",
     "content": "强降雨造成城区多处积水，部分小区供水供电临时中断，应急部门已调派排涝车辆抢修，预计今晚逐步恢复，受影响居民可通过热线报修。",
     "publishTime": "2026/09/08 11:20", "webName": "示例晚报", "region": "河南省"},
]


def test_write(batch: str = "") -> None:
    """不连 Kafka，写一个样例批次到 MinIO，验证 sink 与 worker 全链路。"""
    if not minio_source.ensure_bucket():
        raise SystemExit("MinIO 不可用，检查 Minio_* 配置")
    batch = batch or f"test_{time.strftime('%Y%m%dT%H%M%S')}"
    payload = json.dumps(SAMPLE, ensure_ascii=False).encode("utf-8")
    _put(batch, "sample.json", payload)
    print(f"已写入 {SETTINGS.minio_bucket}/{SETTINGS.minio_prefix}/{batch}/sample.json"
          f"（{len(SAMPLE)} 条）")
    print("worker 侧确认 Minio_Endpoint 已配置后运行: python worker.py --once")


def main() -> None:
    parser = argparse.ArgumentParser(description="Kafka→MinIO 落盘器（独立于 worker）")
    parser.add_argument("--test-write", metavar="BATCH", nargs="?", const="",
                        help="不连 Kafka，直接写样例批次到 MinIO")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")

    if args.test_write is not None:
        test_write(args.test_write)
        return
    if not (SETTINGS.kafka_bootstrap and SETTINGS.kafka_topic):
        raise SystemExit("缺少 Kafka_Bootstrap / Kafka_Topic 配置（.env）")
    if not minio_source.ensure_bucket():
        raise SystemExit("MinIO 不可用，检查 Minio_* 配置")
    run()


if __name__ == "__main__":
    main()
