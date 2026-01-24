import os
import logging
from typing import Dict, Type
from .interfaces import BaseParser
from .JsonFileParser import JsonParser
from .ExcelParser import ExcelParser

class ParserFactory:
    """
    解析器工厂类：根据文件后缀名路由到具体的解析器。
    """
    
    # 后缀名与解析器类的映射表
    # 将类作为值存储，在需要时才实例化
    _REGISTERED_PARSERS: Dict[str, Type[BaseParser]] = {
        ".json": JsonParser,
        ".xlsx": ExcelParser,
        # ".docx": DocxParser,
        # ".doc": DocxParser,
        # ".txt": None, # 待实现
    }

    @classmethod
    def get_parser(cls, filename: str) -> BaseParser:
        """
        获取解析器实例
        :param filename: 文件全名或路径
        :return: 对应的解析器实例
        """
        # 1. 提取后缀并转化为小写（例如: '.PDF' -> '.pdf'）
        ext = os.path.splitext(filename)[1].lower()
        
        if not ext:
            raise ValueError(f"无法识别文件后缀: {filename}")

        # 2. 从映射表中检索对应的解析器类
        parser_class = cls._REGISTERED_PARSERS.get(ext)
        
        if not parser_class:
            logging.error(f"不支持的文件格式: {ext}")
            raise NotImplementedError(f"目前尚未支持 {ext} 格式的解析器。")
            
        # 3. 实例化并返回
        return parser_class()

    @classmethod
    def register_parser(cls, extension: str, parser_class: Type[BaseParser]):
        """
        允许动态注册新的解析器（扩展性支持）
        """
        cls._REGISTERED_PARSERS[extension.lower()] = parser_class