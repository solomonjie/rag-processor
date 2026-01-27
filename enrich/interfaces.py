from abc import ABC, abstractmethod
from typing import Dict, Any
from constants import EnrichmentMethod

class BaseEnrichmentStrategy(ABC):
    @property
    @abstractmethod
    def method_type(self) -> EnrichmentMethod:
        pass

    @property
    @abstractmethod
    def instruction(self) -> str:
        """该策略的具体 LLM 指令"""
        pass

    @property
    @abstractmethod
    def output_field(self) -> str:
        """在返回的 JSON 中对应的 key"""
        pass