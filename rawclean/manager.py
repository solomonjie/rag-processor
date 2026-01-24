from contextlib import closing
import logging
import os
from typing import Any, Dict
import uuid
from database.interfaces import MessageQueueInterface
from files.ContentSaverFactory import ContentSaver
from files.ContentLoaderFactory import ContentLoader
from files.interfaces import BaseParser
from files.ParserFactory import ParserFactory
from .CleanerFactory import CleanerFactory


class CleanManager:
    """
    RAG 系统第一阶段管理器
    职责：协调 Loader, Parser, Cleaner, Saver 和 Publisher 完成数据标准化。
    """

    def __init__(
        self, 
        publisher:MessageQueueInterface,   # 负责分发下游消息
        mq_config: Dict[str, Any] #包含 topic 等配置
    ):
        # 依赖注入：Manager 不关心这些组件的具体实现（本地还是云端）
        self.logger = logging.getLogger(__name__)
        self.publisher = publisher
        self.setup_mq(mq_config)
        

    def setup_mq(self, config: Dict[str, Any]):
        """初始化消息队列连接"""
        try:
            # 调用connect 方法
            self.publisher.connect(config)
        except Exception as e:
            self.logger.error(f"消息队列连接失败: {e}")
            raise

    def process_document(self, source_path: str, storage_path: str):
        """
        处理单个文档的完整生命周期
        :param source_path: 原始文件路径或 URL
        :param storage_type: 告诉 loader 如何读取 (例如 'local', 'azure')
        """
        self.logger.info(f"开始处理文档: {source_path}")

        try:
            # 1. 依赖 Loader 获取原始字节流 (BytesIO)
            # 根据 storage_type 调用不同的加载逻辑
            raw_stream = ContentLoader.load_content(source_path)
            file_ext = os.path.splitext(source_path)[1].lower()

            # 2. 核心：使用 closing 确保 stream 无论成功失败都会被关闭
            with closing(raw_stream) as stream:
                
                # 3. 通过 Factory 获取解析器
                # Parser 只负责将流转化为 Python 原生对象 (Dict/List)
                parser = ParserFactory.get_parser(source_path)
                raw_data = parser.parse(stream)
                
                # 4. 业务逻辑：从解析后的数据中提取并清洗文本
                # 注意：这里我们假设 raw_data 包含业务需要的字段，或直接是文本
                cleaner = CleanerFactory.get_cleaner(file_ext)
                cleaned_content = cleaner.clean(raw_data)

            # --- Stream 此时已关闭，释放内存/句柄 ---

            # 5. 存储标准化后的数据
            # 这里的 saver 可以根据逻辑存到不同地方
            saved_uri = ContentSaver.save_content(cleaned_content, storage_path)

            # 6. 发布消息通知下游（第二阶段：分段任务）
            message_payload = {
                "job_id": str(uuid.uuid4()), # 建议生成唯一 ID 追踪任务
                "file_path": saved_uri,
                "origin_source": source_path,
                "metadata": {
                    "parser_used": parser.__class__.__name__,
                    "file_type": source_path.split('.')[-1]
                }
            }
            self.publisher.produce(message_payload)            
            self.logger.info(f"文档处理成功: {source_path} -> {saved_uri}")

        except Exception as e:
            self.logger.error(f"处理文档 {source_path} 时发生异常: {str(e)}", exc_info=True)
            raise