import queue
import logging
from typing import Any, Optional, Dict
from .interfaces import MessageQueueInterface

class MemoryMessageQueue(MessageQueueInterface):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._topic_queues: Dict[str, queue.Queue] = {}
        self._active_topic: Optional[str] = None
        self._is_connected = False

    def connect(self, config: Dict[str, Any]):
        """绑定指定的 Topic 并初始化队列"""
        topic = config.get("topic", "default_ingestion")
        if topic not in self._topic_queues:
            self._topic_queues[topic] = queue.Queue()
        
        self._active_topic = topic
        self._is_connected = True
        self.logger.info(f"MemoryMQ 已连接到 Topic: {self._active_topic}")

    def produce(self, message: Any):
        """向当前激活的 Topic 发送消息"""
        if not self._is_connected:
            raise ConnectionError("请先调用 connect() 绑定 Topic")
        
        self._topic_queues[self._active_topic].put(message)
        self.logger.debug(f"已存入消息到 {self._active_topic}")

    def consume(self) -> Optional[Any]:
        """从当前绑定的 Topic 消费消息"""
        if not self._is_connected or not self._active_topic:
            return None
            
        try:
            # block=False 保证 Manager 在轮询时不会卡死
            return self._topic_queues[self._active_topic].get(block=False)
        except queue.Empty:
            return None

    def close(self):
        """清理资源并重置状态"""
        self._is_connected = False
        self._active_topic = None
        self.logger.info("MemoryMQ 连接已关闭")