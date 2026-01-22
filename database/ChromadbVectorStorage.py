import logging
from database.interfaces import VectorStoreInterface
from llama_index.vector_stores.chroma import ChromaVectorStore
from llama_index.core import VectorStoreIndex,StorageContext
import chromadb
import os
from typing import Any, List, Dict
from dotenv import load_dotenv

class ChromadbServices(VectorStoreInterface):
    def __init__(self, embed_model: Any):
        load_dotenv()
        self.logger = logging.getLogger(__name__)
        self.remote_db = chromadb.HttpClient(host=os.getenv('CHROMA_SERVER_URL'), port=7000)
        self._collection_cache: Dict[str, chromadb.Collection] = {}
        self._reload_all_collections()
        self._embed_model = embed_model

    def connect(self, config: Dict[str, Any]):
        chroma_collection = self._get_collection(config["collection_name"])
        storage = ChromaVectorStore(chroma_collection=chroma_collection)
        self.index = self._safe_initialize_index(storage, self._embed_model)

    def delete_batch(self, ids: List[str]): 
        """必须在接口中声明，以支持 Manager 的补偿回滚逻辑"""
        pass

    def insert(self, chunks: List[Any]) -> bool:
        try:
            self.index.insert_nodes(chunks)
            return True
        except Exception as e:
            self.logger.error(f"Error Insert not into vector index: {str(e)}")
            return False       

    def search_by_vector(self, query_vector: List[float], top_k: int = 5):
        """语义搜索"""
        pass
 
    def _safe_initialize_index(self, vector_store, embed_model) -> VectorStoreIndex:
        try:
            # 尝试加载现有索引
            return VectorStoreIndex.from_vector_store(
                vector_store=vector_store,
                embed_model=embed_model
                )
        except ValueError as e:
            if "Cannot initialize from a vector store that does not store text" in str(e):
            # 创建新索引
              storage_context = StorageContext.from_defaults(vector_store=vector_store)
              return VectorStoreIndex(
                  nodes=[],
                  storage_context=storage_context,
                  embed_model=embed_model
                  )

    def _get_collection(self, collection_name: str):
        """Helper to get or create a collection instance, with caching."""
        
        # 1. 尝试从缓存中读取
        if collection_name in self._collection_cache:
            self.logger.info(f"Collection '{collection_name}' 从缓存中读取。")
            return self._collection_cache[collection_name]

        # 2. 缓存中没有，执行远程调用获取或创建
        self.logger.info(f"Collection '{collection_name}' 远程获取/创建。")
        try:
            # 执行远程 HTTP 调用
            chroma_collection = self.remote_db.get_or_create_collection(collection_name)
            
            # 3. 将新的 Collection 对象存入缓存
            self._collection_cache[collection_name] = chroma_collection
            
            return chroma_collection
            
        except Exception as e:
            self.logger.info(f"获取或创建 Chroma Collection '{collection_name}' 失败: {e}")
            raise

    def _reload_all_collections(self):
        """
        重新加载缓存：从远程同步所有 Collection。
        - 删除缓存中已在远程 Collection List 中消失的项。
        - 添加远程 Collection List 中存在但缓存中没有的项。
        """
        self.logger.info("--- 启动 Chroma Cache 重新加载/同步 ---")
        
        try:
            # 1. 远程调用: 获取所有 Collection 的名称列表
            remote_collections_info = self.remote_db.list_collections()
            remote_collection_names = {info.name for info in remote_collections_info}
            
            cached_collection_names = set(self._collection_cache.keys())
            
            # --- 2. 处理缓存失效/删除 (Eviction) ---
            collections_to_delete = cached_collection_names - remote_collection_names
            delete_count = 0
            for name in collections_to_delete:
                del self._collection_cache[name]
                self.logger.info(f"  [Evicted] 远程已删除 Collection '{name}'，从缓存中移除。")
                delete_count += 1
                
            # --- 3. 处理新增/预热 (Warming) ---
            collections_to_add = remote_collection_names - cached_collection_names
            add_count = 0
            for name in collections_to_add:
                # 获取 Collection 句柄（远程调用）
                chroma_collection = self.remote_db.get_collection(name)
                self._collection_cache[name] = chroma_collection
                self.logger.info(f"  [Added] 新 Collection '{name}' 已添加到缓存。")
                add_count += 1
                
            self.logger.info(f"--- Chroma Cache 重新加载完成: 移除 {delete_count} 个, 添加 {add_count} 个, 现有 {len(self._collection_cache)} 个 ---")
            
        except Exception as e:
            self.logger.info(f"Chroma Cache 重新加载失败: {e}")
            # 允许继续运行，依赖 _get_collection 在运行时恢复