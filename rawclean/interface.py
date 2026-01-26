from abc import ABC, abstractmethod
from typing import Generator, Iterable

class BaseCleaner(ABC):
    """所有清洗器的基类"""
    @abstractmethod
    def clean(self, raw_rows: Iterable) -> Generator:
        """
        输入: Parser 解析出的原始对象 (dict, list, str)
        输出: 清洗后的标准化文本
        """
        pass