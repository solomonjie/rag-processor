from abc import ABC, abstractmethod
from typing import List, Dict, Any

class ChunkerInterface(ABC):
    """分块处理器接口"""
    @abstractmethod
    def split(self, text: str, options: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        返回格式化的 Node 列表
        每个 Node 包含: {"chunk_content": str, "metadata": dict}
        """
        pass