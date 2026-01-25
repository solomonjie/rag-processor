import json
import logging
import time
from typing import Dict, Any

# 导入工具类
from files.ContentLoaderFactory import ContentLoader
from files.ContentSaverFactory import ContentSaver
from database.interfaces import MessageQueueInterface
from chunker_factory import ChunkerFactory

class ChunkingManager:
    def __init__(self, mq: MessageQueueInterface, poll_interval: float = 1.0):
        """
        Args:
            mq: 消息队列实例
            poll_interval: 无消息时的轮询间隔时间（秒）
        """
        self.logger = logging.getLogger(__name__)
        self.mq = mq
        self.poll_interval = poll_interval
        self.running = False

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
        message = self.mq.consume()
        if not message:
            return False

        input_path = message.get("file_path")
        self.logger.info(f"监听到新消息，处理路径: {input_path}")

        try:
            # 2. 加载内容
            raw_bytes = ContentLoader.load_content(input_path)
            full_data = json.loads(raw_bytes.getvalue().decode("utf-8"))

            # 3. 解析与分块 (基于 step2_part0.json 结构)
            content_body = full_data.get("content", {})
            # 注意：此处 raw_text 需确保在第一阶段已存入 content 
            text_to_chunk = content_body.get("raw_text", "") 
            instructions = content_body.get("pipeline_instructions", {})
            
            # 使用工厂获取策略
            chunker = ChunkerFactory.get_chunker(instructions)
            nodes = chunker.split(text_to_chunk, instructions)

            # 4. 更新数据模型
            content_body["nodes"] = nodes
            
            # 5. 保存结果
            output_path = input_path.replace("step2_part0", "step2_chunked")
            ContentSaver.save_content(
                content=json.dumps(content_body, ensure_ascii=False),
                path=output_path,
                metadata=full_data.get("metadata")
            )

            # 6. 下游通知
            self.mq.produce({
                "file_path": output_path,
                "stage": "chunking_complete"
            })
            
            return True

        except Exception as e:
            self.logger.error(f"处理任务 {input_path} 失败: {e}")
            return True # 返回 True 表示已经尝试处理过该消息