from typing import List, Dict, Any, Optional
from llama_index.vector_stores.milvus import MilvusVectorStore
from llama_index.core import VectorStoreIndex, StorageContext
from llama_index.core.vector_stores.types import VectorStoreQueryMode
from constants import  VectorDatabaseConst
from .interfaces import HybridStoreInterface

class MilvusHybridStore(HybridStoreInterface):
    """基于 Milvus 的混合检索存储实现类"""

    def __init__(self, config: Dict[str, Any], embed_model: Any):        
        self.embed_model = embed_model
        self.vector_store: Optional[MilvusVectorStore] = None
        self.index: Optional[VectorStoreIndex] = None
        
        # 初始调用，利用 connect 的校验逻辑
        self.connect(config)

    def connect(self, config: Dict[str, Any]):
        """
        公开的连接方法。
        增加了对 config 结构的深度校验。
        """
        # 1. 严格校验必填字段
        if not config or not isinstance(config, dict):
             raise ValueError("Connect failed: 'config' must be a non-empty dictionary.")
        
        if "uri" not in config or not config["uri"]:
            raise ValueError("Connect failed: 'uri' is required and cannot be empty.")

        # 2. 读取功能开关
        enable_sparse = config.get("enable_sparse", True)
        enable_dense = config.get("enable_dense", True) 

        if not enable_sparse and not enable_dense:
            raise ValueError("Configuration Error: At least one of 'enable_sparse' or 'enable_dense' must be True.")

        # 3. 初始化 Milvus 存储组件
        try:
            self.vector_store = MilvusVectorStore(
                uri=config["uri"],
                token=config.get("token", ""),
                collection_name=config.get("collection_name", VectorDatabaseConst.MilvusDefaultCollectionName.value),
                dim=config.get("dim", 512) if enable_dense else None,
                enable_sparse=enable_sparse,
                enable_dense=enable_dense,
                overwrite=config.get("overwrite", False)
            )
            
            # 4. 设置 LlamaIndex 存储上下文与索引
            storage_context = StorageContext.from_defaults(vector_store=self.vector_store)
            self.index = VectorStoreIndex.from_vector_store(
                self.vector_store, 
                embed_model=self.embed_model,
                storage_context=storage_context
            )
            
            # 更新内部配置记录
            self.config = config
            
        except Exception as e:
            # 捕获连接异常，例如网络不通或 Milvus 服务未启动
            raise ConnectionError(f"Failed to establish Milvus connection at {config['uri']}: {str(e)}")

    def insert(self, nodes: List[Any]) -> bool:
        """执行数据插入"""
        try:
            if self.index:
                self.index.insert_nodes(nodes)
                return True
            return False
        except Exception as e:
            print(f"Insert failed: {e}")
            return False

    def search(self, query_text: str, mode: str = "hybrid", top_k: int = 5) -> List[Any]:
        """
        统一检索接口
        mode 支持: "dense" (语义), "sparse" (关键词), "hybrid" (混合)
        """
        # 映射业务模式到 LlamaIndex 检索模式
        mode_map = {
            "dense": VectorStoreQueryMode.DEFAULT,
            "sparse": VectorStoreQueryMode.SPARSE,
            "hybrid": VectorStoreQueryMode.HYBRID
        }
        
        query_mode = mode_map.get(mode, VectorStoreQueryMode.HYBRID)
        
        # 构建检索器
        if query_mode != VectorStoreQueryMode.HYBRID:
            retriever = self.index.as_retriever(
                vector_store_query_mode=query_mode,
                similarity_top_k=top_k
                )
            return retriever.retrieve(query_text)
        else:
            dense_retriver = self.index.as_retriever(vector_store_query_mode=VectorStoreQueryMode.DEFAULT, similarity_top_k=top_k * 2)
            sparse_retriver = self.index.as_retriever(vector_store_query_mode=VectorStoreQueryMode.SPARSE, similarity_top_k=top_k * 2)
            dense_results = dense_retriver.retrieve(query_text)
            sparse_results = sparse_retriver.retrieve(query_text)

            # 合并并去重
            combined_dict = {node.id_: node for node in dense_results + sparse_results}
            candidate_nodes = list(combined_dict.values())

            return candidate_nodes

    def delete_batch(self, ids: List[str]):
        """执行批量删除"""
        if self.vector_store:
            self.vector_store.delete_nodes(ids)