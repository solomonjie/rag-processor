from ..interfaces import ChunkerInterface
from typing import List, Dict, Any

class NoSplitChunker(ChunkerInterface):
    """不进行任何分块，将全文作为一个 Node"""
    def split(self, text: str, options: Dict[str, Any]) -> List[Dict[str, Any]]:
        return [{
            "chunk_content": text,
            "metadata": {"strategy": "none"}
        }]