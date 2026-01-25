from typing import Dict, Any
from interfaces import ChunkerInterface
from strategies.no_split_chunker import NoSplitChunker
from strategies.semantic_chunker import SemanticChunker
# 此处可继续导入新增加的策略类

class ChunkerFactory:
    @staticmethod
    def get_chunker(instructions: Dict[str, Any]) -> ChunkerInterface:
        """
        根据 pipeline_instructions 中的参数决定使用哪种分块器
        """
        need_chunking = instructions.get("need_chunking", True)
        
        if not need_chunking:
            return NoSplitChunker()
        
        # 假设指令中包含具体的算法选择，默认使用 sentence
        method = instructions.get("chunk_method", "sentence")
        
        if method == "semantic":
            return SemanticChunker()
        else:
            return NoSplitChunker() # 默认回退