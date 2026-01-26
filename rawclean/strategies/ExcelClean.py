import os
from typing import Any, Dict, List
from ..interface import BaseCleaner
from newspaper import Article

class ExcelCleaner(BaseCleaner):
    """
    Excel 清洗器：负责将原始行数据按分片大小切割。
    注意：它不再负责构建 Payload，只输出原始的 Node 数据字典列表。
    """
    def __init__(self, rows_per_file: int = 50):
        self.rows_per_file = rows_per_file

    def clean(self, raw_rows: List[Dict[str, Any]]) -> List[List[Dict[str, Any]]]:
        """
        返回结构: List[List[Dict]]
        外层 List: 代表不同的分片文件
        内层 List: 该分片包含的所有 Node 字典
        """
        total_rows = len(raw_rows)
        total_chunks = (total_rows + self.rows_per_file - 1) // self.rows_per_file
        all_fragments = []
        content_cols = ["title","summary","content"]
        meta_cols =["author","keyWord","tag","contentMentionRegionList","insertDate"]

        for i in range(total_chunks):
            start_idx = i * self.rows_per_file
            end_idx = start_idx + self.rows_per_file
            chunk_rows = raw_rows[start_idx:end_idx]
            
            # 仅构造原始 Node 字典，使用约定的 page_content 键名
            nodes_data = []
            for j, row in enumerate(chunk_rows):
                raw_content = " | ".join([f"{k}: {v}" for k, v in row.items() if k in content_cols])
                article = Article(url='', language='zh')
                article.set_html(raw_content)
                article.parse()
                nodes_data.append({
                    "page_content": article.text,
                    "metadata": {
                        **{k: row[k] for k in meta_cols if k in row},
                        "internal_id": f"part{i}_{j}"
                    }
                })
            
            all_fragments.append(nodes_data)
            
        return all_fragments