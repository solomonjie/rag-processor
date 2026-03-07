import asyncio
import logging
import time
from typing import List
from pymilvus import MilvusClient

class TagManager:
    def __init__(self, config: dict, refresh_interval: int = 300):
        """
        专用于 RAG 流程的只读标签管理器
        :param refresh_interval: 自动刷新缓存的间隔（秒），默认 300秒（5分钟）
        """
        self.logger = logging.getLogger(__name__)
        self.client = MilvusClient(uri=config['uri'], token=config.get('token', ""))
        self.col = config.get("collection_name", "tag_collection")
        self.refresh_interval = refresh_interval
        
        # 内部缓存：仅存储标签名称字符串列表
        self._tag_names_cache: List[str] = []
        self._is_running = False
        
        # 1. 启动时同步加载一次，确保服务初始化可用
        self._sync_tags_from_db()

    def _sync_tags_from_db(self):
        """从 Milvus 读取全量标签并更新内存缓存"""
        try:
            self.logger.debug("正在更新标签缓存...")
            # 仅查询标签名称字段
            results = self.client.query(
                self.col, 
                filter="", 
                output_fields=["tag_name"], 
                limit=16384
            )
            # 直接提取为 List[str]
            self._tag_names_cache = [t['tag_name'] for t in results]
            self.logger.info(f"标签缓存更新成功，当前共 {len(self._tag_names_cache)} 个标签")
        except Exception as e:
            self.logger.error(f"同步标签数据库失败: {e}")

    async def start_background_refresh(self):
        """启动后台异步定时刷新协程"""
        if self._is_running:
            return
        self._is_running = True
        self.logger.info(f"开启标签后台刷新，频率: {self.refresh_interval}s")
        while self._is_running:
            await asyncio.sleep(self.refresh_interval)
            self._sync_tags_from_db()

    def stop_background_refresh(self):
        """停止后台刷新"""
        self._is_running = False

    def get_all_tags(self) -> List[str]:
        """
        高性能获取接口：直接返回内存中的字符串列表
        RAG 流程直接调用此方法获取 Prompt 注入内容
        """
        return self._tag_names_cache