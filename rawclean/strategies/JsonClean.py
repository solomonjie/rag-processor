import json
from typing import Any, Dict, List
from ..interface import BaseCleaner

class JsonCleaner(BaseCleaner):
    def __init__(self, nodes_per_file: int = 10):
        self.nodes_per_file = nodes_per_file

    def clean(self, raw_data: List[Any]) -> List[Dict[str, Any]]:
        """
        支持将 List 结构转换为多个分片文件，每个分片包含多个 TextNode。
        :param raw_data: Parser 返回的原始列表数据
        :param origin_path: 原始文件路径，用于构造 node_id 和 source_info
        """
        if not isinstance(raw_data, list):
            # 如果不是列表，强行包装成列表处理
            raw_data = [raw_data]

        total_nodes = len(raw_data)
        total_chunks = (total_nodes + self.nodes_per_file - 1) // self.nodes_per_file
        
        all_file_payloads = []

        for i in range(total_chunks):
            start_idx = i * self.nodes_per_file
            end_idx = start_idx + self.nodes_per_file
            chunk_data = raw_data[start_idx:end_idx]
            
            nodes = []
            for j, item in enumerate(chunk_data):
                # 将每一个元素转换为字符串内容
                # 如果元素本身是 Dict，转成 JSON 字符串或拼接值
                if isinstance(item, dict):
                    content_str = " ".join([str(v) for v in item.values()])
                    metadata = item
                else:
                    content_str = str(item)
                    metadata = {}

                nodes.append({
                    "node_id": f"part{i}_{j}",
                    "text": content_str,
                    "metadata": {
                        **metadata,
                        "original_index": start_idx + j
                    }
                })

            # 按照你要求的业务格式组装
            payload = {
                "version": "1.0",
                "source_info": {
                    "chunk_index": i,
                    "total_chunks": total_chunks,
                    "is_fragment": True if total_chunks > 1 else False
                },
                "pipeline_instructions": {
                    "need_chunking": False, # 因为已经是最小单元 Node 了
                    "is_multi_node": True,   # 告诉下游这是一个节点集合
                    "extract_keywords": True
                },
                "nodes": nodes
            }
            all_file_payloads.append(payload)
            
        return all_file_payloads