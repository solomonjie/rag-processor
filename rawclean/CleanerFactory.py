from typing import Any, Dict, Type

from .strategies.ExcelClean import ExcelCleaner
from .strategies.JsonClean import JsonCleaner
from .interface import BaseCleaner

class CleanerFactory:
    _CLEANERS: Dict[str, Type[BaseCleaner]] = {
        ".xlsx": ExcelCleaner,
        ".xls": ExcelCleaner,
        ".json":JsonCleaner,
    }

    @classmethod
    def get_cleaner(cls, file_extension: str) -> BaseCleaner:
        cleaner_class = cls._CLEANERS.get(file_extension.lower())
        if not cleaner_class:
            # 如果没有专门的清洗器，返回一个通用的或者默认不做处理
            return DefaultCleaner() 
        return cleaner_class()

class DefaultCleaner(BaseCleaner):
    def clean(self, raw_data: Any) -> str:
        return str(raw_data).strip()