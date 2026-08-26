"""预建/校验 Milvus collection（显式 schema + BM25 + HNSW + TTL）。

用法：
    python create_collection.py          # 不存在则建，存在则校验字段
    python create_collection.py --drop   # 删除重建（清空数据！）

注意：collection 与消费端连接配置强绑定（名称/维度/analyzer，契约 §2/§3.1），
重建前确认消费端同步切换。
"""
import argparse
import logging
import sys

from pymilvus import MilvusClient

from common import schema
from common.config import SETTINGS

logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
log = logging.getLogger("create_collection")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--drop", action="store_true",
                        help="删除已存在的 collection 后重建（数据清空）")
    args = parser.parse_args()

    client = MilvusClient(uri=SETTINGS.milvus_url, token=SETTINGS.milvus_token)
    name = SETTINGS.collection

    if args.drop and client.has_collection(name):
        client.drop_collection(name)
        log.warning("已删除 collection %s", name)

    schema.ensure_collection(client)

    desc = client.describe_collection(name)
    print(f"collection: {name}")
    print(f"fields    : {', '.join(f['name'] for f in desc['fields'])}")
    print(f"ttl_days  : {SETTINGS.ttl_days}  (dim={SETTINGS.embed_dim})")
    print("OK")


if __name__ == "__main__":
    sys.exit(main())
