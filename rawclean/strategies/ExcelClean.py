import os
from typing import Any, Dict, Generator, Iterable, List
from ..interface import BaseCleaner
from newspaper import Article

class ExcelCleaner(BaseCleaner):
    """
    Excel 清洗器：负责将原始行数据按分片大小切割。
    注意：它不再负责构建 Payload，只输出原始的 Node 数据字典列表。
    """
    def __init__(self, rows_per_file: int = 100):
        self.rows_per_file = rows_per_file

    def clean(self, raw_rows: Iterable) -> Generator:
        content_cols = ["title", "summary", "content"]
        meta_cols = ["author", "keyWord", "contentMentionRegionList", "insertDate"]
        
        nodes_data = []
        chunk_idx = 0    
        for i, row in enumerate(raw_rows):
            # 1. 构造 Node 字典
            raw_content = " | ".join([f"{k}: {v}" for k, v in row.items() if k in content_cols])
            article = Article(url='', language='zh')
            article.set_html(raw_content)
            article.parse()
            
            nodes_data.append({
                "page_content": article.text,
                "metadata": {
                    **{k: row[k] for k in meta_cols if k in row},
                    "internal_id": f"part{chunk_idx}_{len(nodes_data)}"
                }
            })    
            # 2. 达到分片阈值时产出 (yield)
            if len(nodes_data) >= self.rows_per_file:
                yield nodes_data
                nodes_data = []
                chunk_idx += 1    
        
        # 3. 产出剩余不足一个分片的数据
        if nodes_data:
            yield nodes_data