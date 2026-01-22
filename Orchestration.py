from database.ChromadbVectorStorage import ChromadbServices
from database.ElasticKeywordStorage import ElasticServices
from index.manager import IngestionManager
from database.memoryRegistry_impl import MemoryStatusRegistry

def run_ingestion_pipeline(file_data: dict):
    # 1. 组装依赖 (DI)
    registry = MemoryStatusRegistry()
    manager = IngestionManager(
        vector_store=ChromadbServices(), 
        keyword_store=ElasticServices(),
        registry=registry
    )

    # 2. 调用编排逻辑
    file_name = file_data['file_name']
    file_hash = file_data['hash']
    chunks = file_data['sessions']

    manager.process_file_batches(
        file_name=file_name,
        file_hash=file_hash,
        chunks=chunks,
        batch_size=100
    )