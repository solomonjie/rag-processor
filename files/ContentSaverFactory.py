import json
import os
import logging
from io import BytesIO
from typing import Any, Optional, Dict

class ContentSaver:
    """
    统一的内容存储器：
    支持自动识别路径并路由到本地、S3 或 Azure。
    """
    logger = logging.getLogger(__name__)

    @staticmethod
    def save_content(content: str, path: str, storage_type: Optional[str] = None, metadata: Optional[Dict] = None) -> str:
        """
        统一入口：将清洗后的文本和元数据封装并存储
        """
        # 1. 自动判定存储介质
        if not storage_type:
            storage_type = ContentSaver._guess_storage_type(path)
        
        ContentSaver.logger.info(f"正在保存内容至 {storage_type}: {path}")

        # 2. 构造标准输出格式 (JSON 封装)

        try:
            # 3. 路由到具体私有处理方法
            if storage_type == "s3":
                return ContentSaver._save_to_s3(path, content)
            elif storage_type == "azure":
                return ContentSaver._save_to_azure(path, content)
            else:
                return ContentSaver._save_to_local(path, content)
        except Exception as e:
            ContentSaver.logger.error(f"保存至 {storage_type} 失败: {e}")
            raise

    @staticmethod
    def _guess_storage_type(path: str) -> str:
        if path.startswith("s3://"): return "s3"
        if path.startswith("azure://"): return "azure"
        return "local"

    # --- 具体的私有存储方法 ---

    @staticmethod
    def _save_to_local(path: str, data: str) -> str:
        """保存到本地磁盘"""
        # 确保目录存在
        os.makedirs(os.path.dirname(path), exist_ok=True)
        
        with open(path, "w", encoding="utf-8") as f:
            f.write(data)
        
        ContentSaver.logger.info(f"本地文件已成功写入: {path}")
        return path

    @staticmethod
    def _save_to_s3(path: str, data: str) -> str:
        """保存到 AWS S3 (示例接口)"""
        # 逻辑：s3_client.put_object(Body=json.dumps(data), ...)
        ContentSaver.logger.info(f"DEBUG: Uploading JSON to S3: {path}")
        return path

    @staticmethod
    def _save_to_azure(path: str, data: str) -> str:
        """保存到 Azure Blob (示例接口)"""
        # 逻辑：blob_client.upload_blob(json.dumps(data))
        ContentSaver.logger.info(f"DEBUG: Uploading JSON to Azure: {path}")
        return path