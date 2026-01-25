from pydantic import BaseModel, Field, ConfigDict
from typing import List, Dict, Any, Optional

from constants import ChunkMethod, EnrichmentMethod

class PipelineInstructions(BaseModel):
    # 使用 ConfigDict 允许接收模型中未定义的额外字段
    model_config = ConfigDict(extra='allow')

    # 核心开关设置默认值，变成可选
    chunk_method: ChunkMethod = ChunkMethod.NONE
    
    # 针对分块的具体可选参数
    chunk_size: Optional[int] = 500
    chunk_overlap: Optional[int] = 50
    
    # 下一阶段的占位指令
    enrichment_methods: List[EnrichmentMethod] = Field(
        default_factory=lambda: [EnrichmentMethod.NONE]
    )
    
    def get_param(self, key: str, default: Any = None) -> Any:
        """获取可能存在的额外指令参数"""
        return getattr(self, key, default)

class Node(BaseModel):
    page_content: str
    metadata: Dict[str, Any] = Field(default_factory=dict)

class ContentBody(BaseModel):
    version: str = "1.0"
    pipeline_instructions: PipelineInstructions
    nodes: List[Node] = Field(default_factory=list)

class RAGTaskPayload(BaseModel):
    content: ContentBody
    metadata: Optional[Dict[str, Any]] = Field(default_factory=dict)