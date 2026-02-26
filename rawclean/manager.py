from contextlib import closing
import logging
import os
import time
from typing import Any, Dict, List
import uuid
from constants import ChunkMethod
from database.interfaces import MessageQueueInterface
from database.message import TaskMessage,QueueMessage
from files.ContentSaverFactory import ContentSaver
from files.ContentLoaderFactory import ContentLoader
from files.DocumentFormat import ContentBody, Node, PipelineInstructions, RAGTaskPayload
from files.interfaces import BaseParser
from files.ParserFactory import ParserFactory
from .CleanerFactory import CleanerFactory


class CleanManager:
    """
    RAG 系统第一阶段管理器
    职责：协调 Loader, Parser, Cleaner, Saver 和 Publisher 完成数据标准化。
    """
    def __init__(
        self, 
        consumer: MessageQueueInterface,  # 监听队列：接收来自 Clean 的消息
        publisher: MessageQueueInterface, # 发送队列：发送给 Chunk 的消息
        poll_interval: float = 1.0
    ):
        self.logger = logging.getLogger(__name__)
        self.consumer = consumer
        self.publisher = publisher
        self.poll_interval = poll_interval

    def start(self):
        """启动持续监听循环"""
        self.running = True
        self.logger.info("CleanManager 已启动，正在监听消息队列...")
        
        try:
            while self.running:
                # 尝试处理单条消息
                success = self.process_document()
                
                # 如果没有消息被处理，则进入休眠，避免 CPU 占用过高
                if not success:
                    time.sleep(self.poll_interval)
        except KeyboardInterrupt:
            self.stop()
        except Exception as e:
            self.logger.critical(f"Manager 遇到崩溃性错误: {e}")
            self.stop()

    def stop(self):
        """停止监听"""
        self.running = False
        self.logger.info("正在停止 ChunkingManager...")

    def process_document(self) -> bool:
        """
        处理单个文档的完整生命周期
        """
        message = self.consumer.consume()
        if not message:
            return False

        task = TaskMessage.from_json(message.data)
        self.logger.info(f"监听到新消息，处理路径: {task.file_path}")

        try:
            # 1. 依赖 Loader 获取原始字节流 (BytesIO)
            # 根据 storage_type 调用不同的加载逻辑
            raw_stream = ContentLoader.load_content(task.file_path)
            target_root, _ = os.path.splitext(task.file_path)
            target_ext = ".json"

            # 2. 核心：使用 closing 确保 stream 无论成功失败都会被关闭
            with closing(raw_stream) as stream:
                
                # 3. 通过 Factory 获取解析器
                # Parser 只负责将流转化为 Python 原生对象 (Dict/List)
                parser = ParserFactory.get_parser(task.file_path)
                raw_data = parser.parse(stream)
                
                # 4. 业务逻辑：从解析后的数据中提取并清洗文本
                # 注意：这里我们假设 raw_data 包含业务需要的字段，或直接是文本
                source_ext = os.path.splitext(task.file_path)[1].lower()
                cleaner = CleanerFactory.get_cleaner(source_ext)
            
                for idx, nodes_data in enumerate(cleaner.clean(raw_data)):
                    # 构造不同的保存路径，例如 test_part0.json, test_part1.json
                    fragment_path = f"{target_root}_part{idx}{target_ext}"
    
                    new_nodes: List[Node] = []
                    for c in nodes_data:
                        # 构造新 Node，继承并合并元数据
                        new_nodes.append(Node(
                            page_content=c["page_content"],
                            metadata={**c.get("metadata", {})}
                        ))
                        
                    payload = RAGTaskPayload(
                        content=ContentBody(
                            pipeline_instructions=PipelineInstructions(
                            chunk_method=ChunkMethod.NONE # 第一阶段默认不分块
                            ),
                            nodes=new_nodes
                        ),
                        metadata={
                            "fragment_index": idx,
                            "source": task.file_path
                            }
                    )
                    
                    # 保存 (ContentSaver 只管存 dict)
                    #saved_uri = ContentSaver.save_content(payload, fragment_path)
                    ContentSaver.save_content(
                        content=payload.model_dump_json(ensure_ascii=False),
                        path=fragment_path,
                        metadata=payload.metadata # 传入可选的 metadata
                    )
                    
                    # 每一部分都发送一条独立的消息到 MQ
                    # # 下游 Worker 会并行处理这些分片，效率极高
                    output_message = TaskMessage(
                        file_path=fragment_path,
                        stage="clean_complete",
                        trace_id=str(uuid.uuid4())
                    )
    
                    self.publisher.produce(output_message.to_json())

            self.consumer.ack(message.id)   
            self.logger.info(f"文档处理成功: {task.file_path} -> {fragment_path}")
        except Exception as e:
            self.logger.error(f"处理文档 {task.file_path} 时发生异常: {str(e)}", exc_info=True)
            raise