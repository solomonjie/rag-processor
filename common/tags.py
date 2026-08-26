"""tag_collection 只读访问（带缓存）。

契约 §2.4：内容库节点 tags 的值域必须与 tag_collection 中 is_active=true 的
tag_name 同步，否则用户在 UI 上选不到该标签。LLM 打标的候选集与白名单都来自这里。
"""
import logging
import time
from typing import List, Tuple

from pymilvus import MilvusClient

from common.config import SETTINGS

log = logging.getLogger("tags")

# (tag_name, description)
TagList = List[Tuple[str, str]]


class TagStore:
    def __init__(self, refresh_seconds: int | None = None):
        self.client: MilvusClient | None = None  # 惰性创建：MilvusClient 构造即连接
        self.refresh_seconds = (
            refresh_seconds if refresh_seconds is not None else SETTINGS.tag_refresh_seconds
        )
        self._tags: TagList = []
        self._fetched_at = 0.0
        self.refresh()

    def refresh(self) -> None:
        try:
            if self.client is None:
                self.client = MilvusClient(uri=SETTINGS.milvus_url, token=SETTINGS.milvus_token)
            rows = self.client.query(
                SETTINGS.tag_collection,
                filter="is_active == true",
                output_fields=["tag_name", "description"],
                limit=16384,
            )
            self._tags = [
                (r["tag_name"], (r.get("description") or "").strip()) for r in rows
            ]
            self._fetched_at = time.time()
            log.info("标签缓存刷新：%d 个有效标签", len(self._tags))
        except Exception as e:  # 连接失败保留旧缓存，由调用方判断空标签
            log.error("读取 tag_collection 失败: %s", e)

    def get(self) -> TagList:
        if time.time() - self._fetched_at > self.refresh_seconds:
            self.refresh()
        return self._tags


_tagstore: TagStore | None = None


def get_tagstore() -> TagStore:
    """进程内单例（worker 与独立运行共用）。"""
    global _tagstore
    if _tagstore is None:
        _tagstore = TagStore()
    return _tagstore
