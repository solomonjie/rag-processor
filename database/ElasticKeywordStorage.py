from database.interfaces import KeywordStoreInterface
import os
from typing import List, Dict,Any
from dotenv import load_dotenv


class ElasticServices(KeywordStoreInterface):
    def __init__(self):
        load_dotenv()

    def connect(self, config: Dict[str, Any]):
        pass

    def insert(self, chunks: List[Any]) -> bool:
        return True

    def search_by_keyword(self, query_vector: List[float], top_k: int = 5):
        """语义搜索"""
        return ""