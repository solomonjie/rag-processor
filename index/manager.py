import logging
import time
from typing import List, Dict, Any, Optional
from Files.ContentLoaderFactory import ContentLoader
from database.interfaces import MessageQueueInterface, VectorStoreInterface, KeywordStoreInterface, BaseStatusRegistry
from database.message import IngestionTaskSchema
from llama_index.core.schema import BaseNode
from llama_index.core import Document
from llama_index.core.node_parser import SentenceSplitter

class IngestionManager:
    def __init__(
        self, 
        mq: MessageQueueInterface,
        loader: ContentLoader,
        vector_store: Optional[VectorStoreInterface] = None, 
        keyword_store: Optional[KeywordStoreInterface] = None,
        registry: Optional[BaseStatusRegistry] = None,
        strict_consistency: bool = True
    ):
        self.logger = logging.getLogger(__name__)
        self.v_store = vector_store
        self.k_store = keyword_store
        self.registry = registry
        self.strict_consistency = strict_consistency
        self.mq = mq
        self.loader = loader
        

    def start_listening(self, mq_config: dict):
        self.mq.connect(mq_config)
        self.logger.info("IngestionManager 正在运行...")
        
        try:
            while True:
                raw_msg = self.mq.consume()
                if raw_msg:
                    self._handle_task(raw_msg)
                else:
                    time.sleep(1) # 无消息时避免占用过多 CPU
        except KeyboardInterrupt:
            self.mq.close()

    def _handle_task(self, raw_message: str):
        try:
            # 1. 使用 Schema 自动验证并解析消息内容
            task = IngestionTaskSchema.model_validate_json(raw_message)
            self.logger.info(f"收到合法任务: {task.file_name}")

            # 2. 从消息指定的路径读取真实的内容文件,并转换为chunks
            raw_content =self.loader.load_content(task.file_path)
            documents = []
            for block in raw_content:
                doc = Document(
                    text=block["content"],
                    metadata={
                    "block_id": int(block["block_id"]),
                    "block_type": str(block["block_type"]),
                    "title": str(block["title"]),
                    "keywords": "|".join(block.get("keywords", []))
                    }
                    )
                documents.append(doc)
            
            parser = SentenceSplitter(
                chunk_size = 10000,
                chunk_overlap=0        
            )
            nodes = parser.get_nodes_from_documents(documents)

            # 3. 将数据插入数据库中
            success = self._process_file_batches(
                index_name=task.index_name,
                file_name=task.file_name,
                file_hash=task.file_hash,
                chunks=nodes
            )

            if success:
                self.logger.info(f"任务完成: {task.file_name}")
                # 后续可以在这里通过 mq 确认消息（ACK）或删除临时文件

        except Exception as e:
            self.logger.error(f"任务处理异常: {str(e)}")

    def _process_file_batches(self, index_name: str, file_name: str, file_hash: str, chunks: List[BaseNode], batch_size: int = 50):
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