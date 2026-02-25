import redis
import json
import logging
from typing import Any, Optional, Dict
from .interfaces import MessageQueueInterface

class RedisMessageQueue(MessageQueueInterface):
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def connect(self, config: Dict[str, Any]):
        """绑定指定的 Topic 并初始化队列"""
        self.client = redis.Redis(
            host=config.get('host', 'localhost'),
            port=config.get('port', 6379),
            decode_responses=True
        )
        self.stream = config.get('topic', 'default_stream')
        self.group = config.get('group', 'default_group')
        self.consumer_name = config.get('consumer_name', 'worker_1')
        
        # 尝试创建消费者组（如果已存在则忽略错误）
        try:
            self.client.xgroup_create(self.stream, self.group, id='0', mkstream=True)
        except redis.exceptions.ResponseError:
            pass

    def produce(self, message: Any):
        """向当前激活的 Topic 发送消息"""
        data = {"payload": json.dumps(message) if not isinstance(message, str) else message}
        return self.client.xadd(self.stream, data)

    def consume(self) -> Optional[Any]:
        """从当前绑定的 Topic 消费消息"""
        # 使用 '>' 读取组内未分配的新消息，阻塞时间为 1 秒
        messages = self.client.xreadgroup(self.group, self.consumer_name, {self.stream: '0'}, count=1, block=1000)
        if not messages:
            return None
        
        msg_id, content = messages[0][1][0]
        # 自动确认消息（ACK），若需更严格保障可手动调用 XACK
        #self.client.xack(self.stream, self.group, msg_id)
        return json.loads(content['payload'])

    def close(self):
        """清理资源并重置状态"""
        self.client.close()
        self.logger.info("RedisMQ 连接已关闭")