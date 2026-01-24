from abc import ABC, abstractmethod
from io import BytesIO
from typing import Any

# --- 接口定义 ---
class BaseParser(ABC):
    """所有解析器的基类"""
    @abstractmethod
    def parse(self, stream: BytesIO) -> Any:
        pass