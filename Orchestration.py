import logging
import uuid
from constants import VectorDatabaseConst
from database.message import TaskMessage
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
    # 实例化并连接
    # 假设 RedisMessageQueue 是您之前实现的类
    clean_worker_name = f"{worker_name}_clean_{1}"
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

    # task = TaskMessage(
    #     file_path="data/pipeline.xlsx",
    #     stage="Clean",
    #     trace_id=str(uuid.uuid4())
    # )
    # consume.produce(task.to_json())

    manager.start()

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

async def mqtest():
    # 基础配置定义
    chunk_topic = "test_chunk_stream"
    chunk_group = "test_group"
    worker_name = "worker_test"
    chunk_worker_name = f"{worker_name}_chunk_1"

    # --- 阶段 1: 初始化生产者并发送消息 ---
    publish = RedisMessageQueue()
    publish_config = {
        'host': 'localhost',
        'port': 6379,
        'topic': chunk_topic,
        'group': chunk_group,
        'consumer_name': "producer_1"
    }
    publish.connect(publish_config)

    # 创建测试消息
    task = TaskMessage(
        file_path="/data/test.json",
        stage="chunking",
        trace_id=str(uuid.uuid4())
    )
    
    print(f"[*] 生产者：发送任务 {task.trace_id}")
    publish.produce(task.to_json())

    # --- 阶段 2: 消费者初次消费，但不 ACK (模拟崩溃) ---
    print("\n[!] 模拟消费者 A 启动并消费消息，但【不执行 ACK】...")
    consume_v1 = RedisMessageQueue()
    consume_config = {
        'host': 'localhost',
        'port': 6379,
        'topic': chunk_topic,
        'group': chunk_group,
        'consumer_name': chunk_worker_name
    }
    consume_v1.connect(consume_config)

    # 第一次 consume：此时 _check_pending 为 True，扫描 PEL 为空，然后读取 '>' 获取新消息
    msg_v1 = consume_v1.consume()
    if msg_v1:
        received_task = TaskMessage.from_json(msg_v1.data)
        print(f"[+] 消费者 A 收到消息: {received_task.trace_id}, 当前 _check_pending: {consume_v1._check_pending}")
        # 注意：这里故意不调用 consume_v1.ack(msg_v1['id'])
    
    consume_v1.close()
    print("[!] 消费者 A 已关闭（未确认消息）。")

    # --- 阶段 3: 消费者重启，验证自动读取 Pending ---
    print("\n[*] 模拟消费者 A 重启，验证 Pending 自动读取逻辑...")
    consume_v2 = RedisMessageQueue()
    # 使用相同的配置（尤其是相同的 consumer_name）
    consume_v2.connect(consume_config)

    # 第二次 consume：此时 _check_pending 默认为 True
    # 它应该从 ID '0' 读到刚才没确认的那条消息，而不是阻塞等待新消息
    msg_v2 = consume_v2.consume()
    
    if msg_v2 and msg_v2.id == msg_v1.id:
        recovered_task = TaskMessage.from_json(msg_v2.data)
        print(f"[SUCCESS] 成功找回 Pending 消息: {recovered_task.trace_id}")
        
        # 此时执行 ACK，验证状态变更
        ack_res = consume_v2.ack(msg_v2.id)
        print(f"[*] 执行 ACK 结果: {ack_res}, 当前 _check_pending: {consume_v2._check_pending}")
    else:
        print("[FAILED] 未能找回 Pending 消息或消息不匹配。")

    # --- 阶段 4: 再次 consume，验证 PEL 为空后转向新消息 ---
    # 此时 _check_pending 应该已经是 False，会尝试读 '>'
    print("\n[*] 验证 PEL 清空后状态...")
    msg_v3 = consume_v2.consume() # 这里会因为没有新消息而阻塞 1s 或返回 None
    if msg_v3 is None:
        print(f"[+] PEL 已空，消费者进入监听新消息状态。当前 _check_pending: {consume_v2._check_pending}")

    consume_v2.close()

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

    asyncio.run(mqtest())