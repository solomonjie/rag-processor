from abc import ABC, abstractmethod
from io import BytesIO

# --- 接口定义 ---
class BaseParser(ABC):
    """所有解析器的基类"""
    @abstractmethod
    def parse(self, stream: BytesIO) -> str:
        pass