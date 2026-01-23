import json
import os
from typing import List, Dict, Any, Optional

class ContentLoader:
    def __init__(self):
        # 可以在这里初始化一些连接池或客户端，如 s3 client
        pass

    def load_content(self, path: str, storage_type: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        统一入口：根据路径自动判断并直接返回解析后的内容
        """
        # 1. 确定存储类型
        if not storage_type:
            storage_type = self._guess_storage_type(path)
        
        # 2. 路由到具体的私有处理方法
        if storage_type == "s3":
            return self._load_from_s3(path)
        elif storage_type == "azure":
            return self._load_from_azure(path)
        else:
            return self._load_from_local(path)

    def _guess_storage_type(self, path: str) -> str:
        if path.startswith("s3://"): return "s3"
        if path.startswith("azure://"): return "azure"
        return "local"

    # --- 具体的读取方法 ---

    def _load_from_local(self, path: str) -> List[Dict[str, Any]]:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Local file not found: {path}")
        with open(path, 'r', encoding='utf-8') as f:
            data = json.load(f)
            return data

    def _load_from_s3(self, path: str) -> List[Dict[str, Any]]:
        # 逻辑：使用 boto3 下载并 json.loads
        print(f"DEBUG: Downloading from S3: {path}")
        return [] # 实际实现时返回解析后的列表

    def _load_from_azure(self, path: str) -> List[Dict[str, Any]]:
        # 逻辑：使用 Azure SDK 读取
        print(f"DEBUG: Reading from Azure Blob: {path}")
        return []