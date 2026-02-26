import time
from pydantic import BaseModel, Field
from typing import Dict, Any, Optional
from dataclasses import dataclass

@dataclass
class QueueMessage:
    """MQ 内部使用的通用包装类"""
    id: str           # Redis Stream 的消息 ID (例如 '170000000-0')
    data: Any         # 解码后的业务数据 (TaskMessage 对象或 Dict)

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
    def from_json(cls, json_str: Any):
        """从 MQ 读取字符串并反序列化为对象"""
        if isinstance(json_str, str):
            return cls.model_validate_json(json_str)
        elif isinstance(json_str, dict):
            # 如果已经是字典，直接验证模型
            return cls.model_validate(json_str)
        else:
            raise ValueError(f"不支持的数据类型: {type(json_str)}")