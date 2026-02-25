import logging
import uuid
from constants import VectorDatabaseConst
from llm.llm_client import LLMClient
from chunking.manager import ChunkingManager
from database.MemoryMessageQueue import MemoryMessageQueue
from database.redisMemoryMessageQueue import RedisMessageQueue
from database.MilvusHybridStore import MilvusHybridStore
from enrich.EnrichmentMaster import EnrichmentMaster
from enrich.manager import EnrichmentManager
from index.manager import IngestionManager
from embedding.TextEmbeddingsInference import TextEmbeddingService
from database.memoryRegistry_impl import MemoryStatusRegistry
from logfilter.logging_context import TraceIdFilter
import sys
import asyncio

from rawclean.manager import CleanManager

clean_topic = "clean_flow"
chunk_topic = "chunk_flow"
enrich_topic = "enrich_flow"
index_topic = "index_flow"

clean_group = "clean_group"
chunk_group = "chunk_group"
enrich_group = "enrich_group"
index_group = "index_group"

worker_name = "worker"

async def run_clean_pipeline():
    mq = MemoryMessageQueue()
    mq_config = {
        'host': 'localhost',        # Redis 服务器地址
        'port': 6379,               # 端口
        'topic': chunk_topic,       # Stream 名称 (Topic)
        'group': clean_group,       # 消费者组名称
        'consumer_name': f"{worker_name}_clean_{uuid.uuid4().hex}" # 当前消费者标识
    }

    # 2. 实例化并连接
    # 假设 RedisMessageQueue 是您之前实现的类
    mq = RedisMessageQueue()
    mq.connect(mq_config)

    manager = CleanManager(
        publisher=mq
    )
    save_path = "data/pipeline.json"
    manager.process_document('data/pipeline.xlsx', save_path)
    print("Clean Done")

async def run_chunk_pipeline():
    chunk_worker_name = f"{worker_name}_chunk_{uuid.uuid4().hex}"
    consume = RedisMessageQueue()
    consume_config = {
        'host': 'localhost',        # Redis 服务器地址
        'port': 6379,               # 端口
        'topic': chunk_topic,       # Stream 名称 (Topic)
        'group': chunk_group,       # 消费者组名称
        'consumer_name': chunk_worker_name # 当前消费者标识
    }
    consume.connect(consume_config)

    publish = RedisMessageQueue()
    publish_config ={
        'host': 'localhost',        # Redis 服务器地址
        'port': 6379,               # 端口
        'topic': enrich_topic,       # Stream 名称 (Topic)
        'group': chunk_group,       # 消费者组名称
        'consumer_name': chunk_worker_name # 当前消费者标识
    }
    publish.connect(publish_config)

    manager = ChunkingManager(
        consumer=consume,
        publisher=publish,
    )
    manager.start()

#1,000,000 cost
async def run_enrich_pipeline():
    enrich_worker_name = f"{worker_name}_enrich_{uuid.uuid4().hex}"
    consume = RedisMessageQueue()
    consume_config = {
        'host': 'localhost',        # Redis 服务器地址
        'port': 6379,               # 端口
        'topic': enrich_topic,       # Stream 名称 (Topic)
        'group': enrich_group,       # 消费者组名称
        'consumer_name': enrich_worker_name # 当前消费者标识
    }
    consume.connect(consume_config)

    publish = RedisMessageQueue()
    publish_config = {
        'host': 'localhost',        # Redis 服务器地址
        'port': 6379,               # 端口
        'topic': index_topic,       # Stream 名称 (Topic)
        'group': enrich_group,       # 消费者组名称
        'consumer_name': enrich_worker_name # 当前消费者标识
    }
    publish.connect(publish_config)
    
    master = EnrichmentMaster(LLMClient())

    manager = EnrichmentManager(
        consumer=consume,
        publisher=publish,
        enrich_master=master
    )
    await manager.start()

async def run_ingestion_pipeline():
    # 1. 组装依赖 (DI)
    registry = MemoryStatusRegistry()
    emb_model = TextEmbeddingService()
    mq = RedisMessageQueue()
    mq_config = {
        'host': 'localhost',        # Redis 服务器地址
        'port': 6379,               # 端口
        'topic': index_topic,       # Stream 名称 (Topic)
        'group': index_group,       # 消费者组名称
        'consumer_name': "worker_index_479882de8e294a7098a76fc3447164c8" # 当前消费者标识
    }
    mq.connect(mq_config)

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

    manager.start_listening()

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

    asyncio.run(run_ingestion_pipeline())