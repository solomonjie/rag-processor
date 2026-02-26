from contextlib import closing
import json
import logging
import os
import time
from typing import Dict, Any, List

# 导入工具类
from constants import ChunkMethod, EnrichmentMethod
from database.message import TaskMessage,QueueMessage
from files.ContentLoaderFactory import ContentLoader
from files.ContentSaverFactory import ContentSaver
from database.interfaces import MessageQueueInterface
from .chunker_factory import ChunkerFactory
from files.DocumentFormat import Node, RAGTaskPayload

class ChunkingManager:
    def __init__(
        self, 
        consumer: MessageQueueInterface,  # 监听队列：接收来自 Clean 的消息
        publisher: MessageQueueInterface, # 发送队列：发送给 Enrich 的消息
        poll_interval: float = 1.0
    ):
        self.logger = logging.getLogger(__name__)
        self.consumer = consumer
        self.publisher = publisher
        self.poll_interval = poll_interval

    def start(self):
        """启动持续监听循环"""
        self.running = True
        self.logger.info("ChunkingManager 已启动，正在监听消息队列...")
        
        try:
            while self.running:
                # 尝试处理单条消息
                success = self.process_task()
                
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

    def process_task(self) -> bool:
        """
        处理单个分块任务
        返回 True 表示处理了消息，False 表示队列为空
        """
        # 1. 从 MQ 获取消息
        message = self.consumer.consume()
        if not message:
            return False

        task = TaskMessage.from_json(message.data)
        self.logger.info(f"监听到新消息，处理路径: {task.file_path}")

        try:
            # 1. 加载内容
            raw_stream = ContentLoader.load_content(task.file_path)

            # 2. 核心：使用 closing 确保 stream 无论成功失败都会被关闭
            with closing(raw_stream) as stream:
                payload = RAGTaskPayload.model_validate_json(raw_stream.getvalue())

            # 3. 解析与分块 (基于 step2_part0.json 结构)
            instr = payload.content.pipeline_instructions
            
            # 使用工厂获取策略
            chunker = ChunkerFactory.get_chunker(instr.chunk_method)
            new_nodes: List[Node] = []

            # 4. 更新数据模型
            for original_node in payload.content.nodes:
                # 对单个 Node 的内容进行切分
                # split 应当返回 List[Dict]，包含 chunk_content 和该块特有的 metadata
                chunks = chunker.split(original_node.page_content, instr.model_dump())
                
                for c in chunks:
                    # 构造新 Node，继承并合并元数据
                    new_nodes.append(Node(
                        page_content=c["chunk_content"],
                        metadata={**original_node.metadata, **c.get("metadata", {})}
                    ))

            # 5. 更新 Payload 数据结构
            payload.content.nodes = new_nodes

            # 6. 状态转换：关闭分块，开启增强（如果需要）
            payload.content.pipeline_instructions.chunk_method = ChunkMethod.NONE
            # 这里可以根据业务逻辑决定下一步要做的 Enrichment
            if payload.content.pipeline_instructions.enrichment_methods == [EnrichmentMethod.NONE]:
                # 示例：默认分块后进行摘要和关键词提取
                payload.content.pipeline_instructions.enrichment_methods = [
                    EnrichmentMethod.SUMMARY, 
                    EnrichmentMethod.KEYWORDS
                ]
            
            # 7. 持久化并发送下一阶段消息
            base_path, ext = os.path.splitext(task.file_path)
            output_path = f"{base_path}_chunked{ext}"
            ContentSaver.save_content(
                content=payload.model_dump_json(ensure_ascii=False), # 序列化整个对象
                path=output_path,
                metadata=payload.metadata
            )

            # 发送下一阶段的消息
            next_msg = TaskMessage(
                file_path=output_path,
                stage="chunking_complete",
                trace_id=task.trace_id
            )
            self.publisher.produce(next_msg.to_json())
            self.consumer.ack(message.id)
            return True
        except Exception as e:
            self.logger.error(f"处理失败: {task.file_path if 'task' in locals() else 'unknown'}, 错误: {e}")
            return True # 坏消息不再重试