import logging
from database.MemoryMessageQueue import MemoryMessageQueue
from database.ChromadbVectorStorage import ChromadbServices
from database.ElasticKeywordStorage import ElasticServices
from database.message import IngestionTaskSchema
from index.manager import IngestionManager
from embedding.TextEmbeddingsInference import TextEmbeddingService
from database.memoryRegistry_impl import MemoryStatusRegistry
from logfilter.logging_context import TraceIdFilter
import sys

from rawclean.manager import CleanManager

def run_ingestion_pipeline(file_path: str):
    # 1. 组装依赖 (DI)
    registry = MemoryStatusRegistry()
    emb_model = TextEmbeddingService()
    mq = MemoryMessageQueue()
    v_storage = ChromadbServices(emb_model.embed_model)
    manager = IngestionManager(
        mq=mq,
        vector_store=v_storage, 
        keyword_store=ElasticServices(),
        registry=registry
    )
    
    mq_config = {"topic": "ingestion_flow"}
    
    # 2. 发送消息
    task_data = {
        "file_name": "data_sample.json",
        "file_path": f"C:\enlist\\rag-processor\data\data_sample.json",
        "file_hash": "hash_8899_xyz",
        "index_name": "product_knowledge_base",
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
    manager.process_document(file_path, save_path)
    print("Clean Done")
    

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
    run_clean_pipeline(sys.argv[1])