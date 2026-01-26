from typing import Generator, Iterable
from ..interface import BaseCleaner

class JsonCleaner(BaseCleaner):
    def __init__(self, nodes_per_file: int = 10):
        self.nodes_per_file = nodes_per_file

    def clean(self, raw_rows: Iterable) -> Generator:
        """
        Excel 清洗器：负责将原始行数据按分片大小切割。
        注意：它不再负责构建 Payload，只输出原始的 Node 数据字典列表。
        """
        if not isinstance(raw_data, list):
            # 如果不是列表，强行包装成列表处理
            raw_data = [raw_data]

        nodes_data = []
        chunk_idx = 0    
        for i, row in enumerate(raw_rows):
            # 1. 构造 Node 字典
            content_str = " ".join([str(v) for v in row.values()])
            
            nodes_data.append({
                "page_content": content_str,
                "metadata": {
                    "internal_id": f"part{chunk_idx}_{len(nodes_data)}"
                }
            })    
            # 2. 达到分片阈值时产出 (yield)
            if len(nodes_data) >= self.nodes_per_file:
                yield nodes_data
                nodes_data = []
                chunk_idx += 1    
        
        # 3. 产出剩余不足一个分片的数据
        if nodes_data:
            yield nodes_data