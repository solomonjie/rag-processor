"""Milvus 存取封装。

两条访问路径，职责分开：
- MilvusClient（pymilvus）：schema 管理、URL 去重查询；
- MilvusVectorStore（llama-index）：插入。只有这条路径会写 _node_content/_node_type，
  消费端靠它们恢复节点元数据（契约 §2.1），所以必须走 llama-index 插入。
- upsert_mode=True：同一 node_id（md5(url)）重复处理时覆盖而非报错/重复（契约 §9.7）。
"""
import json
import logging
from typing import List, Set

from llama_index.core import StorageContext, VectorStoreIndex
from llama_index.core.schema import TextNode
from llama_index.vector_stores.milvus import MilvusVectorStore
from llama_index.vector_stores.milvus.utils import BM25BuiltInFunction
from pymilvus import MilvusClient

from common import schema
from common.config import SETTINGS
from common.embed import get_embed_model

log = logging.getLogger("store")


class VectorStore:
    def __init__(self):
        self.client = MilvusClient(uri=SETTINGS.milvus_url, token=SETTINGS.milvus_token)
        schema.ensure_collection(self.client)

        self.vs = MilvusVectorStore(
            uri=SETTINGS.milvus_url,
            token=SETTINGS.milvus_token,
            collection_name=SETTINGS.collection,
            dim=SETTINGS.embed_dim,
            enable_sparse=True,
            embedding_field="embedding",
            text_key="text",
            sparse_embedding_function=BM25BuiltInFunction(
                analyzer_params=schema.ANALYZER_PARAMS
            ),
            overwrite=False,
            upsert_mode=True,
        )
        self.embed_model = get_embed_model()
        self.index = VectorStoreIndex.from_vector_store(
            self.vs,
            embed_model=self.embed_model,
            storage_context=StorageContext.from_defaults(vector_store=self.vs),
        )

    def exists(self, node_ids: List[str]) -> Set[str]:
        """批量查已入库的 node_id（URL 去重，在 LLM 之前执行以省调用）。"""
        found: Set[str] = set()
        for i in range(0, len(node_ids), 100):
            chunk = node_ids[i:i + 100]
            expr = "id in [" + ",".join(json.dumps(n) for n in chunk) + "]"
            rows = self.client.query(
                SETTINGS.collection, filter=expr,
                output_fields=["id"], limit=len(chunk),
            )
            found.update(r["id"] for r in rows)
        return found

    def upsert(self, nodes: List[TextNode]) -> int:
        if not nodes:
            return 0
        self.index.insert_nodes(nodes)  # 无 embedding 的节点由 embed_model 自动向量化
        return len(nodes)


_store: VectorStore | None = None


def get_store() -> VectorStore:
    """进程内单例（worker 与独立运行共用）。"""
    global _store
    if _store is None:
        _store = VectorStore()
    return _store
