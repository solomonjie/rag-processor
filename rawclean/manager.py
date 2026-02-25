from contextlib import closing
import logging
import os
from typing import Any, Dict, List
import uuid
from constants import ChunkMethod
from database.interfaces import MessageQueueInterface
from database.message import TaskMessage
from files.ContentSaverFactory import ContentSaver
from files.ContentLoaderFactory import ContentLoader
from files.DocumentFormat import ContentBody, Node, PipelineInstructions, RAGTaskPayload
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
    ):
        # 依赖注入：Manager 不关心这些组件的具体实现（本地还是云端）
        self.logger = logging.getLogger(__name__)
        self.publisher = publisher

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
            target_root, target_ext = os.path.splitext(storage_path)

            # 2. 核心：使用 closing 确保 stream 无论成功失败都会被关闭
            with closing(raw_stream) as stream:
                
                # 3. 通过 Factory 获取解析器
                # Parser 只负责将流转化为 Python 原生对象 (Dict/List)
                parser = ParserFactory.get_parser(source_path)
                raw_data = parser.parse(stream)
                
                # 4. 业务逻辑：从解析后的数据中提取并清洗文本
                # 注意：这里我们假设 raw_data 包含业务需要的字段，或直接是文本
                source_ext = os.path.splitext(source_path)[1].lower()
                cleaner = CleanerFactory.get_cleaner(source_ext)
            
                for idx, nodes_data in enumerate(cleaner.clean(raw_data)):
                    # 构造不同的保存路径，例如 test_part0.json, test_part1.json
                    fragment_path = f"{target_root}_part{idx}{target_ext}"
    
                    new_nodes: List[Node] = []
                    for c in nodes_data:
                        # 构造新 Node，继承并合并元数据
                        new_nodes.append(Node(
                            page_content=c["page_content"],
                            metadata={**c.get("metadata", {})}
                        ))
                        
                    payload = RAGTaskPayload(
                        content=ContentBody(
                            pipeline_instructions=PipelineInstructions(
                            chunk_method=ChunkMethod.NONE # 第一阶段默认不分块
                            ),
                            nodes=new_nodes
                        ),
                        metadata={
                            "fragment_index": idx,
                            "source": source_path
                            }
                    )
                    
                    # 保存 (ContentSaver 只管存 dict)
                    #saved_uri = ContentSaver.save_content(payload, fragment_path)
                    ContentSaver.save_content(
                        content=payload.model_dump_json(ensure_ascii=False),
                        path=fragment_path,
                        metadata=payload.metadata # 传入可选的 metadata
                    )
                    
                    # 每一部分都发送一条独立的消息到 MQ
                    # # 下游 Worker 会并行处理这些分片，效率极高
                    output_message = TaskMessage(
                        file_path=fragment_path,
                        stage="clean_complete",
                        trace_id=str(uuid.uuid4())
                    )
    
                    self.publisher.produce(output_message.to_json())
               
            self.logger.info(f"文档处理成功: {source_path} -> {fragment_path}")
        except Exception as e:
            self.logger.error(f"处理文档 {source_path} 时发生异常: {str(e)}", exc_info=True)
            raise