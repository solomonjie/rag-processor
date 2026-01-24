from typing import Dict, List, Any, TypedDict
from datetime import datetime

class CleanDocument(TypedDict):
    version: str
    source_info: Dict[str, Any]      # 包含 origin_path, timestamp 等
    pipeline_instructions: Dict[str, bool] # 包含 need_chunking, need_qa 等
    content: str                     # 清洗后的纯文本内容