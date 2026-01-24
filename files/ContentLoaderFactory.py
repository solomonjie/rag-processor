from io import BytesIO
import logging
import os
from typing import List, Dict, Any, Optional

class ContentLoader:
    logger = logging.getLogger(__name__)
    
    @staticmethod
    def load_content(path: str, storage_type: Optional[str] = None) -> BytesIO:
        """
        统一入口：根据路径自动判断并直接返回解析后的内容
        """
        # 1. 确定存储类型
        if not storage_type:
            storage_type = ContentLoader._guess_storage_type(path)
        
        # 2. 路由到具体的私有处理方法
        if storage_type == "s3":
            return ContentLoader._load_from_s3(path)
        elif storage_type == "azure":
            return ContentLoader._load_from_azure(path)
        else:
            return ContentLoader._load_from_local(path)

    @staticmethod
    def _guess_storage_type(path: str) -> str:
        if path.startswith("s3://"): return "s3"
        if path.startswith("azure://"): return "azure"
        return "local"

    # --- 具体的读取方法 ---
    @staticmethod
    def _load_from_local(path: str) -> BytesIO:
        if not os.path.exists(path):
            raise FileNotFoundError(f"Local file not found: {path}")
        with open(path, "rb") as f:
            return BytesIO(f.read())

    @staticmethod
    def _load_from_s3(path: str) -> BytesIO:
        # 逻辑：使用 boto3 下载并 json.loads
        ContentLoader.logger.info(f"DEBUG: Downloading from S3: {path}")
        return BytesIO(b"dummy s3 data")

    @staticmethod
    def _load_from_azure(path: str) -> BytesIO:
        # 逻辑：使用 Azure SDK 读取
        ContentLoader.logger.info(f"DEBUG: Reading from Azure Blob: {path}")
        return BytesIO(b"dummy azure data")