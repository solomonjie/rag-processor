from io import BytesIO
import io
import json
import logging
from typing import Any, Dict, List
from files.interfaces import BaseParser


class JsonParser(BaseParser):
    """
    针对 JSON 文件的解析器。
    它将字节流转化为字典，并提取核心文本内容。
    """
    
    def __init__(self):
        self.logger = logging.getLogger(__name__)

    def parse(self, stream: BytesIO, encoding: str = 'utf-8') -> List[Dict[str, Any]]:
        try:
            stream.seek(0)
            text_reader = io.TextIOWrapper(stream, encoding=encoding)
            data = json.load(text_reader)
            text_reader.detach()
            return data
        except json.JSONDecodeError as e:
            logging.error(f"Invalid JSON format: {e}")
            raise ValueError(f"Failed to parse JSON stream: {e}")
        except UnicodeDecodeError as e:
            logging.error(f"Encoding error: {e}")
            raise ValueError(f"Failed to decode stream using {encoding}")