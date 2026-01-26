import os
from typing import Any, Dict, List
# 假设你已经将之前的模型定义放在了 models.py 和 constants.py 中
from constants import ChunkMethod, EnrichmentMethod

class ExcelCleaner: # 继承自你的 BaseCleaner
    def __init__(self, rows_per_file: int = 50):
        self.rows_per_file = rows_per_file

    def clean(self, raw_rows: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        total_rows = len(raw_rows)
        total_chunks = (total_rows + self.rows_per_file - 1) // self.rows_per_file
        all_file_payloads = []

        for i in range(total_chunks):
            start_idx = i * self.rows_per_file
            end_idx = start_idx + self.rows_per_file
            chunk_rows = raw_rows[start_idx:end_idx]
            
            # --- 契约对齐 ---
            nodes = []
            for j, row in enumerate(chunk_rows):
                nodes.append({
                    "page_content": " | ".join([f"{k}: {v}" for k, v in row.items()]),
                    "metadata": {
                        **row, 
                        "internal_id": f"part{i}_{j}"
                    }
                })

            # --- 指令集对齐 ---
            payload = {
                "content": {
                    "version": "1.0",
                    "pipeline_instructions": {
                        "chunk_method": ChunkMethod.NONE, # 默认设为 NONE，第二阶段根据需求修改
                        "chunk_size": 500,
                        "chunk_overlap": 50,
                        "enrichment_methods": [EnrichmentMethod.NONE] # 默认不做增强
                    },
                    "nodes": nodes
                },
                "metadata": {
                    "source_type": "excel",
                    "chunk_index": i,
                    "total_chunks": total_chunks
                }
            }
            all_file_payloads.append(payload)
            
        return all_file_payloads