import argparse
import logging
from constants import VectorDatabaseConst
from llm.llm_client import LLMClient
from chunking.manager import ChunkingManager
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

async def run_clean_pipeline(work_id: str):
    # 实例化并连接
    # 假设 RedisMessageQueue 是您之前实现的类
    clean_worker_name = f"{worker_name}_clean_{work_id}"
    consume = RedisMessageQueue()
    consume_config = {
        'host': 'localhost',        # Redis 服务器地址
        'port': 6379,               # 端口
        'topic': clean_topic,       # Stream 名称 (Topic)
        'group': clean_group,       # 消费者组名称
        'consumer_name': clean_worker_name # 当前消费者标识
    }
    consume.connect(consume_config)

    publish = RedisMessageQueue()
    publish_config ={
        'host': 'localhost',        # Redis 服务器地址
        'port': 6379,               # 端口
        'topic': chunk_topic,       # Stream 名称 (Topic)
        'group': clean_group,       # 消费者组名称
        'consumer_name': clean_worker_name # 当前消费者标识
    }
    publish.connect(publish_config)

    manager = CleanManager(
        consumer=consume,
        publisher=publish
    )

    manager.start()

async def run_chunk_pipeline(work_id: str):
    chunk_worker_name = f"{worker_name}_chunk_{work_id}"
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
async def run_enrich_pipeline(work_id: str):
    enrich_worker_name = f"{worker_name}_enrich_{work_id}"
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

async def run_ingestion_pipeline(work_id: str):
    # 1. 组装依赖 (DI)
    registry = MemoryStatusRegistry()
    emb_model = TextEmbeddingService()
    mq = RedisMessageQueue()
    index_worker_name = f"{worker_name}_index_{work_id}"
    mq_config = {
        'host': 'localhost',        # Redis 服务器地址
        'port': 6379,               # 端口
        'topic': index_topic,       # Stream 名称 (Topic)
        'group': index_group,       # 消费者组名称
        'consumer_name': index_worker_name # 当前消费者标识
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

async def main():
    # 1. 配置命令行参数解析
    parser = argparse.ArgumentParser(description="RAG Pipeline Worker Orchestrator")
    
    parser.add_argument(
        "--type", 
        choices=['clean', 'chunk', 'enrich', 'index'], 
        required=True, 
        help="指定启动的 Worker 类型"
    )
    
    parser.add_argument(
        "--id", 
        type=int, 
        default=1, 
        help="Worker 的实例 ID，用于区分并发消费者 (默认: 1)"
    )

    args = parser.parse_args()

    # 2. 配置日志
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s | %(levelname)s | %(trace_id)s | %(name)s | %(message)s"
    )
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        handler.addFilter(TraceIdFilter())

    logging.info(f"正在启动 Worker 类型: {args.type}, 实例 ID: {args.id}")

    # 3. 根据类型跳转到对应的 pipeline
    # 注意：你可以修改你的函数接收 args.id，从而动态生成 worker_name
    if args.type == 'clean':
        await run_clean_pipeline(args.id) # 如果需要，可改为 run_clean_pipeline(args.id)
    elif args.type == 'chunk':
        await run_chunk_pipeline(args.id)
    elif args.type == 'enrich':
        await run_enrich_pipeline(args.id)
    elif args.type == 'index':
        await run_ingestion_pipeline(args.id)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Worker 已手动停止")
        sys.exit(0)

if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        logging.info("Worker 已手动停止")
        sys.exit(0)