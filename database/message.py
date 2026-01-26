import time
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional

class IngestionTaskSchema(BaseModel):
    """定义入库任务的消息标准格式"""
    file_name: str = Field(..., description="原始文件名")
    file_path: str = Field(..., description="JSON 文件的存储路径")
    file_hash: Optional[str] = Field(None, description="文件的唯一哈希，可选")
    index_name: str = Field(..., description="向量数据库存储的索引名称")
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict, description="额外的业务元数据")

    class Config:
        # 允许从字典或对象初始化
        from_attributes = True

class TaskMessage(BaseModel):
    """标准任务消息：在各个处理阶段之间流动的轻量级信号"""
    
    # 核心字段：文件的存储路径
    file_path: str = Field(..., description="处理后的 JSON 文件路径")
    
    # 追踪字段
    stage: str = Field(..., description="产生该消息的阶段名称 (如 clean, chunking)")
    timestamp: float = Field(default_factory=time.time, description="消息产生的时间戳")
    
    # 可选：链路追踪 ID，用于串联整个日志流
    trace_id: Optional[str] = Field(None, description="全局唯一追踪 ID")

    def to_json(self) -> str:
        """序列化消息以便存入 MQ"""
        return self.model_dump_json()

    @classmethod
    def from_json(cls, json_str: str):
        """从 MQ 读取字符串并反序列化为对象"""
        return cls.model_validate_json(json_str)