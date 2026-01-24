from abc import ABC, abstractmethod
from typing import Any

class BaseCleaner(ABC):
    """所有清洗器的基类"""
    @abstractmethod
    def clean(self, raw_data: Any) -> str:
        """
        输入: Parser 解析出的原始对象 (dict, list, str)
        输出: 清洗后的标准化文本
        """
        pass