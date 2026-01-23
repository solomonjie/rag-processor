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