import json
from rawclean.interface import BaseCleaner


class JsonCleaner(BaseCleaner):
    def clean(self, raw_data: list) -> str:
        """假设 Json Parser 返回的是 List[List] 结构"""
        json_string = json.dumps(raw_data, ensure_ascii=False, indent=2)
        return json_string