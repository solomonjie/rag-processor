import logging
import uuid
from constants import VectorDatabaseConst
from llm.llm_client import LLMClient
from chunking.manager import ChunkingManager
from database.MemoryMessageQueue import MemoryMessageQueue
from database.MilvusHybridStore import MilvusHybridStore
from database.message import IngestionTaskSchema, TaskMessage
from enrich.EnrichmentMaster import EnrichmentMaster
from enrich.manager import EnrichmentManager
from index.manager import IngestionManager
from embedding.TextEmbeddingsInference import TextEmbeddingService
from database.memoryRegistry_impl import MemoryStatusRegistry
from logfilter.logging_context import TraceIdFilter
import sys
import asyncio
import nest_asyncio

from rawclean.manager import CleanManager

async def run_ingestion_pipeline(file_path: str):
    # 1. 组装依赖 (DI)
    registry = MemoryStatusRegistry()
    emb_model = TextEmbeddingService()
    mq = MemoryMessageQueue()
    storage_config = {
        "uri":VectorDatabaseConst.MilvusDefaultServer.value,
        "enable_sparse":True,
        "enable_dense":True,
        "dim":512,
        "collection_name":"product_knowledge_base"
    }
    v_storage = MilvusHybridStore(storage_config, emb_model.embed_model)
    manager = IngestionManager(
        mq=mq,
        vector_store=v_storage, 
        registry=registry
    )
    
    mq_config = {"topic": "ingestion_flow"}
    
    # 2. 发送消息
    task_data = {
        "file_name": "data_sample.json",
        "file_path": f"data/data_sample.json",
        "file_hash": "hash_8899_xyz",
        "index_name":"product_knowledge_base",
        "metadata": {"department": "AI_Research", "priority": "high"}
    }

    task = IngestionTaskSchema(**task_data)
    mq.connect(mq_config)
    mq.produce(task.model_dump_json())
    
    # 3. 调用编排逻辑
    manager.start_listening(mq_config)

def run_clean_pipeline(file_path:str):
    mq = MemoryMessageQueue()
    mq_config = {"topic": "clean_flow"}
    manager = CleanManager(
        publisher=mq,
        mq_config=mq_config
    )
    save_path = "data/step2.json"
    manager.process_document('data/公交集团近一周舆情数据.xlsx', save_path)
    print("Clean Done")

def run_chunk_pipeline(file_path:str):
    consume = MemoryMessageQueue()
    consume_config = {"topic": "clean_flow"}

    publish = MemoryMessageQueue()
    publish_config = {"topic": "chunk_flow"}

    manager = ChunkingManager(
        consumer=consume,
        consumer_config=consume_config,
        publisher=publish,
        publisher_config=publish_config
    )

    output_message = TaskMessage(
        file_path="data/step2_part2.json",
        stage="clean_complete",
        trace_id=str(uuid.uuid4())  # 记得加括号生成实例
    )
    consume.produce(output_message.to_json())

    manager.start()

#1,000,000 cost
def run_enrich_pipeline(file_path:str):
    consume = MemoryMessageQueue()
    consume_config = {"topic": "clean_flow"}
    consume.connect(consume_config)

    publish = MemoryMessageQueue()
    publish_config = {"topic": "chunk_flow"}
    publish.connect(publish_config)
    
    master = EnrichmentMaster(LLMClient())

    manager = EnrichmentManager(
        consumer=consume,
        publisher=publish,
        enrich_master=master
    )

    output_message = TaskMessage(
        file_path="data/step2_part238.json",
        stage="chunk_complete",
        trace_id=str(uuid.uuid4())  # 记得加括号生成实例
    )
    consume.produce(output_message.to_json())

    manager.start()

if __name__ == "__main__":
    # 检查是否传入了文件路径
    if len(sys.argv) < 2:
        print("Usage: python orchestration.py <path_to_json_file>")
        sys.exit(1)
    
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(trace_id)s | %(name)s | %(message)s"
    )
    
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.addFilter(TraceIdFilter())
    #run_ingestion_pipeline(sys.argv[1])
    asyncio.run(run_ingestion_pipeline(sys.argv[1]))