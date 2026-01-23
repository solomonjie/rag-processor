from abc import ABC, abstractmethod
from typing import List, Dict, Any, Optional, Set

class BaseStore(ABC):
    """底层存储的抽象基类"""
    
    @abstractmethod
    def connect(self, config: Dict[str, Any]):
        """根据配置文件初始化连接"""
        pass

    @abstractmethod
    def insert(self, chunks: List[Any]) -> bool:
        """执行数据插入，返回布尔值以配合一致性校验"""
        pass

class VectorStoreInterface(BaseStore):
    """向量数据库专用的高级检索接口"""
    def __init__(self,embed_model: Any):
        pass

    @abstractmethod
    def delete_batch(self, ids: List[str]): 
        """必须在接口中声明，以支持 Manager 的补偿回滚逻辑"""
        pass

    @abstractmethod
    def search_by_vector(self, query_vector: List[float], top_k: int = 5):
        """语义搜索"""
        pass

class KeywordStoreInterface(BaseStore):
    """关键词数据库专用的精确匹配接口"""
    
    @abstractmethod
    def search_by_keyword(self, query_text: str, top_k: int = 5):
        """全文检索"""
        pass

class BaseStatusRegistry(ABC):
    @abstractmethod
    def is_file_processed(self, file_name: str) -> bool:
        """检查整个文件是否已完成"""
        pass

    @abstractmethod
    def mark_chunks_processed(self, file_name: str, chunk_ids: List[str]):
        """标记某个文件已完成的 chunk_id 集合"""
        pass

    @abstractmethod
    def get_processed_chunks(self, file_name: str) -> Set[str]:
        """获取某个文件已完成的 chunk_id 集合"""
        pass

    @abstractmethod
    def mark_file_complete(self, file_name: str, file_hash: str):
        """当所有 chunk 完成后，记录文件级索引并清理 chunk 记录"""
        pass

class MessageQueueInterface(ABC):
    @abstractmethod
    def connect(self, config: Dict[str, Any]):
        """建立连接并绑定 Topic/Queue"""
        pass

    @abstractmethod
    def consume(self) -> Optional[Any]:
        """获取消息"""
        pass

    @abstractmethod
    def produce(self, message: Any):
        """发送消息"""
        pass

    @abstractmethod
    def close(self):
        """
        显式关闭连接。
        对于 Kafka：触发 Offset 提交并停止心跳。
        对于内存队列：清空缓存数据。
        """
        pass