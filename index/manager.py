import logging
from typing import List, Dict, Any, Optional
from database.interfaces import VectorStoreInterface, KeywordStoreInterface, BaseStatusRegistry
from llama_index.core.schema import BaseNode

class IngestionManager:
    def __init__(
        self, 
        vector_store: Optional[VectorStoreInterface] = None, 
        keyword_store: Optional[KeywordStoreInterface] = None,
        registry: Optional[BaseStatusRegistry] = None,
        strict_consistency: bool = True
    ):
        self.v_store = vector_store
        self.k_store = keyword_store
        self.registry = registry
        self.strict_consistency = strict_consistency
        self.logger = logging.getLogger(__name__)

    def process_file_batches(self, index_name: str, file_name: str, file_hash: str, chunks: List[BaseNode], batch_size: int = 50):
        """处理文件级别的批量入库逻辑"""
        # 1. 检查文件是否整体已完成
        if self.registry and self.registry.is_file_processed(file_name):
            self.logger.info(f"File {file_name} already fully processed.")
            return
        
        config = {"index_name": index_name}
        self.v_store.connect(config)

        # 获取当前文件已处理的 chunk 集合以实现续传
        processed_chunk_ids = self.registry.get_processed_chunks(file_name) if self.registry else set()

        for i in range(0, len(chunks), batch_size):
            batch = chunks[i : i + batch_size]
            # 2. 过滤已处理的 chunk
            to_process = [c for c in batch if c.id_ not in processed_chunk_ids]
            if not to_process: continue

            v_success_ids = []
            try:
                # 3. 批量执行双写
                if self.v_store:
                    if self.v_store.insert(to_process):
                        v_success_ids = [c.id_ for c in to_process]
                
                if self.k_store and not self.k_store.insert(to_process) and self.strict_consistency:
                    raise RuntimeError("Keyword store insert failed")

                # 4. 记录当前 batch 的 chunk 进度
                if self.registry:
                    self.registry.mark_chunks_processed(file_name, [c.id_ for c in to_process])

            except Exception as e:
                self.logger.error(f"Batch failed: {e}")
                if v_success_ids and self.strict_consistency and self.v_store:
                    self.v_store.delete_batch(v_success_ids)
                raise e

        # 5. 文件所有 chunk 处理完，标记文件完成并清理 chunk 记录
        if self.registry:
            self.registry.mark_file_complete(file_name, file_hash)