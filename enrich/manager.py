import asyncio
from contextlib import closing
import json
import logging
import os
import time
from typing import Dict, Any, List

from constants import EnrichmentMethod
from database.interfaces import MessageQueueInterface
from .EnrichmentMaster import EnrichmentMaster
from files.ContentLoaderFactory import ContentLoader
from files.ContentSaverFactory import ContentSaver
from files.DocumentFormat import RAGTaskPayload
from database.message import TaskMessage 

class EnrichmentManager:
    def __init__(
        self, 
        consumer: MessageQueueInterface,  # 接收来自 Chunk 的消息
        publisher:MessageQueueInterface, # 发送给 Index 的消息
        enrich_master:EnrichmentMaster, # 封装了 LLM 编排逻辑的 Master
        poll_interval: float = 1.0
    ):
        self.logger = logging.getLogger(__name__)
        self.consumer = consumer
        self.publisher = publisher
        self.master = enrich_master
        self.poll_interval = poll_interval
        self.running = False

    def start(self):
        """同步入口：负责启动异步大循环"""
        self.running = True
        self.logger.info("EnrichmentManager 已启动...")
        
        # 核心点 1: 在同步方法中启动 asyncio 事件循环
        try:
            asyncio.run(self._main_loop())
        except KeyboardInterrupt:
            self.stop()

    async def _main_loop(self):
        """异步主循环：负责监听 MQ"""
        while self.running:
            raw_msg = self.consumer.consume()
            if not raw_msg:
                # 核心点 2: 异步循环内必须用 asyncio.sleep，否则会阻塞整个线程
                await asyncio.sleep(self.poll_interval)
                continue
            
            try:
                await self._process_task(raw_msg)
            except Exception as e:
                self.logger.error(f"处理任务过程中发生异常: {e}", exc_info=True)

    async def _process_task(self, raw_msg: Any):
        # 1. 消息解码 (TaskMessage 模式)
        task = TaskMessage.from_json(raw_msg)
        self.logger.info(f"开始丰富化处理: {task.file_path} (TraceID: {task.trace_id})")

        # 2. 加载数据
        stream = ContentLoader.load_content(task.file_path)
        with closing(stream) as stream:
                payload = RAGTaskPayload.model_validate_json(stream.getvalue())

        # 3. 提取待处理的方法和节点
        methods = payload.content.pipeline_instructions.enrichment_methods
        if EnrichmentMethod.NONE in methods or not methods:
            self.logger.info("无需丰富化，跳过执行")
            self._finish_stage(task, payload)
            return

        # 4. 调用 Master 进行批量/并发丰富化 (此处逻辑在 Master 中实现)
        # 注意：这里会修改 payload.content.nodes
        await self.master.process_payload(payload)

        # 5. 状态转换：清空方法列表防止重复执行
        payload.content.pipeline_instructions.enrichment_methods = [EnrichmentMethod.NONE]

        # 6. 持久化并发送下一阶段消息
        self._finish_stage(task, payload)

    def _finish_stage(self, task, payload:RAGTaskPayload):
        base_path, ext = os.path.splitext(task.file_path)
        output_path = f"{base_path}_enriched{ext}"
        
        ContentSaver.save_content(
            content=payload.model_dump_json(ensure_ascii=False),
            path=output_path,
            metadata=payload.metadata
        )

        next_msg = TaskMessage(
            file_path=output_path,
            stage="enrichment_complete",
            trace_id=task.trace_id
        )
        self.publisher.produce(next_msg.to_json())

    def stop(self):
        self.running = False