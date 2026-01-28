from abc import ABC, abstractmethod
from typing import Dict, Any, List
from constants import EnrichmentMethod

class BaseEnrichmentStrategy(ABC):
    @property
    @abstractmethod
    def method_type(self) -> EnrichmentMethod:
        pass

    @abstractmethod
    def task_name(self) -> str:
        """逻辑任务名，用于 prompt 描述"""
        pass

    @abstractmethod
    def task_description(self) -> str:
        """这项 enrich 是干嘛的（语义定义）"""
        pass

    @abstractmethod
    def output_field(self) -> str:
        """JSON 中对应的字段名"""
        pass

    @abstractmethod
    def output_schema(self) -> Dict[str, Any]:
        """
        该字段的结构约束（给 LLM 看）
        e.g.
        {
            "type": "array",
            "items": "string",
            "min_items": 5,
            "max_items": 8
        }
        """
        pass

    def quality_rules(self) -> List[str]:
        """可选：质量约束 / 风格约束"""
        return []

    def failure_fallback(self) -> Any:
        """信息不足时的兜底返回"""
        return ""