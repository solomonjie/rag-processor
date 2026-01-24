import os
from typing import Any, Dict, List
from rawclean.interface import BaseCleaner


class ExcelCleaner(BaseCleaner):
    def __init__(self, rows_per_file: int = 50):
        self.rows_per_file = rows_per_file

    def clean(self, raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        注意：现在返回的是一个 List[Dict]，每个 Dict 代表一个要保存的文件内容
        """
        total_rows = len(raw_rows)
        # 计算总分片数
        total_chunks = (total_rows + self.rows_per_file - 1) // self.rows_per_file
        
        all_file_payloads = []

        for i in range(total_chunks):
            start_idx = i * self.rows_per_file
            end_idx = start_idx + self.rows_per_file
            chunk_rows = raw_rows[start_idx:end_idx]
            
            # 构造 nodes
            nodes = []
            for j, row in enumerate(chunk_rows):
                nodes.append({
                    "node_id": f"part{i}_{j}",
                    "text": " | ".join([f"{k}: {v}" for k, v in row.items()]),
                    "metadata": {**row}
                })

            # 构造单个分片的 Payload
            payload = {
                "version": "1.0",
                "source_info": {
                    "chunk_index": i,
                    "total_chunks": total_chunks,
                    "is_fragment": True
                },
                "pipeline_instructions": {
                    "need_chunking": False,
                    "is_multi_node": True
                },
                "nodes": nodes
            }
            all_file_payloads.append(payload)
            
        return all_file_payloads