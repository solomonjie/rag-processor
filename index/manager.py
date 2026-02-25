from contextlib import closing
import hashlib
import logging
import time
from typing import List, Dict, Any, Optional
import uuid
from files.ContentLoaderFactory import ContentLoader
from database.interfaces import MessageQueueInterface, BaseStore, BaseStatusRegistry
from database.message import TaskMessage
from files.ParserFactory import ParserFactory
from logfilter.logging_context import trace_id_var
from llama_index.core.schema import TextNode

class IngestionManager:
    def __init__(
        self, 
        mq: MessageQueueInterface,
        vector_store: Optional[BaseStore] = None, 
        registry: Optional[BaseStatusRegistry] = None,
        strict_consistency: bool = True
    ):
        self.logger = logging.getLogger(__name__)
        self.v_store = vector_store
        self.registry = registry
        self.strict_consistency = strict_consistency
        self.mq = mq
        

    def start_listening(self):
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
            task = TaskMessage.from_json(raw_message)
            self.logger.info(f"收到合法任务: {task.file_path}")

            # 2. 从消息指定的路径读取真实的内容文件,并转换为chunks
            raw_content = ContentLoader.load_content(task.file_path)
            with closing(raw_content) as stream:
                # 拿到流后，交给 Factory 和 Parser 
                parser = ParserFactory.get_parser(task.file_path)
                raw_data = parser.parse(stream)

            self.logger.info("loaded %d chunks", len(raw_data))

            nodes = self._build_nodes(raw_data, task)
            self.logger.info("built %d nodes", len(nodes))

            # 3. 将数据插入数据库中
            success = self._process_file_batches(
                file_name=task.file_path,
                chunks=nodes
            )

            if success:
                self.logger.info(f"任务完成: {task.file_path}")
                # 后续可以在这里通过 mq 确认消息（ACK）或删除临时文件
        except Exception as e:
            self.logger.error(f"任务处理异常: {str(e)}")

    def _build_nodes(self, raw_content: Dict[str, Any], task: TaskMessage) -> List[TextNode]:
        nodes = []
        
        # 提取实际的节点列表，新结构在 content -> nodes 下
        content_data = raw_content.get("content", {})
        blocks = content_data.get("nodes", [])
        
        for block in blocks:
            # 1. 过滤掉 page_content 为空的无效节点
            if not block.get("page_content"):
                continue
                
            # 2. 获取稳定 ID：优先使用 internal_id，若无则使用 page_content 的哈希
            inner_id = block.get("metadata").get("internal_id")
            if not inner_id:
                inner_id = hashlib.md5(block["page_content"].encode()).hexdigest()
                
            # 构造全局唯一的 chunk_id
            chunk_id = f"{task.file_path}:{inner_id}"
            
            # 3. 提取元数据（适配新 JSON 字段）
            metadata = block.get("metadata", {})
            
            node = TextNode(
                id_=chunk_id,          
                text=block["page_content"], 
                metadata={
                    "file_name": task.file_path,
                    "internal_id": inner_id,
                    "author": metadata.get("author", ""),
                    "title": metadata.get("title", ""), # 如果 metadata 里没有 title，可以从 text 第一行截取
                    "keywords": "|".join(metadata.get("keywords", [])),
                    "summary": metadata.get("summary", ""),
                    "insert_date": metadata.get("insert_date", "")
                }
            )
        
            nodes.append(node)
        
        return nodes

    def _process_file_batches(self, file_name: str, chunks: List[TextNode], batch_size: int = 50) -> bool:
        """处理文件级别的批量入库逻辑"""
        namespace = uuid.NAMESPACE_DNS
        file_hash = str(uuid.uuid5(namespace, file_name))
        # 1. 检查文件是否整体已完成
        if self.registry and self.registry.is_file_processed(file_name):
            self.logger.info(f"File {file_name} already fully processed.")
            return

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

        return True