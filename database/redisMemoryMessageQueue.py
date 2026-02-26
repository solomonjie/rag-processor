import redis
import json
import logging
from typing import Any, Optional, Dict
from .interfaces import MessageQueueInterface

class RedisMessageQueue(MessageQueueInterface):
    def __init__(self):
        self.logger = logging.getLogger(__name__)
        self._check_pending = True

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

    def consume(self) -> Optional[Dict[str, Any]]:
        """
        自动判定读取逻辑：
        1. 只要 _check_pending 为 True，就一直尝试读 ID '0'。
        2. 如果 '0' 返回空，说明当前消费者的 Pending 队列清空了，切换到 '>'。
        3. 一旦 ACK 失败或者发生重启，逻辑会自动回归到优先检查 Pending。
        """
        
        # 步骤 1: 优先处理 Pending 队列
        if self._check_pending:
            msg = self._read_from_redis(last_id='0', block=None)
            if msg:
                # 仍在处理 Pending 消息，保持标记为 True
                return msg
            
            # 只有当 '0' 返回空，才允许尝试读取新消息
            self._check_pending = False
            self.logger.debug(f"Consumer {self.consumer_name} PEL is empty.")

        # 步骤 2: 读取新消息
        msg = self._read_from_redis(last_id='>', block=1000)
        
        if msg:
            # 一旦拿到新消息，标记改为 True
            # 这样如果消息处理失败（未 ACK），下次 consume 会回到步骤 1
            self._check_pending = True
            
        return msg

    def _read_from_redis(self, last_id: str, block: Optional[int]) -> Optional[Dict[str, Any]]:
        """底层封装 XREADGROUP 调用"""
        try:
            # result 格式: [[b'stream_name', [(b'id', {b'key': b'value'})]]]
            result = self.client.xreadgroup(
                self.group, 
                self.consumer_name, 
                {self.stream: last_id}, 
                count=1, 
                block=block
            )

            # 1. 检查 result 是否为空 (None 或 [])
            if not result:
                return None

            # 2. 深入解析: result[0] 是第一个 stream 的数据, result[0][1] 是消息列表
            try:
                stream_data = result[0]
                messages = stream_data[1]
                
                if not messages:
                    return None
                
                msg_id, content = messages[0]
                return {
                    "id": msg_id,
                    "data": json.loads(content['payload'])
                }
            except (IndexError, KeyError, TypeError):
                return None

        except Exception as e:
            self.logger.error(f"Read error from Redis: {e}")
            return None

    def ack(self, message_id: str) -> bool:
        """
        确认消息并更新状态
        """
        try:
            res = self.client.xack(self.stream, self.group, message_id)
            if res > 0:
                # 确认成功后，允许尝试读取新消息（或者再次探测 PEL 是否真的空了）
                self._check_pending = False
                return True
            return False
        except Exception as e:
            # ACK 失败时，必须保持或恢复 _check_pending 为 True
            self._check_pending = True
            self.logger.error(f"ACK failed: {e}")
            return False

    def close(self):
        """清理资源并重置状态"""
        self.client.close()
        self.logger.info("RedisMQ 连接已关闭")