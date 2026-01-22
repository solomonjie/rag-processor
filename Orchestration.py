from database.ChromadbVectorStorage import ChromadbServices
from database.ElasticKeywordStorage import ElasticServices
from index.manager import IngestionManager
from embedding.TextEmbeddingsInference import TextEmbeddingService
from embedding.TextEmbeddingsInference import TextEmbeddingsInference
from database.memoryRegistry_impl import MemoryStatusRegistry
from llama_index.core import Document
from llama_index.core.node_parser import SentenceSplitter
import sys
import json

def run_ingestion_pipeline(file_data: dict):
    # 1. 组装依赖 (DI)
    registry = MemoryStatusRegistry()
    emb_model = TextEmbeddingService()
    manager = IngestionManager(
        vector_store=ChromadbServices(emb_model), 
        keyword_store=ElasticServices(),
        registry=registry
    )

    # 2. 调用编排逻辑
    file_name = "testfile"
    file_hash = "textfileguid"
    chunks = file_data

    manager.process_file_batches(
        file_name=file_name,
        file_hash=file_hash,
        chunks=chunks,
        batch_size=5
    )

if __name__ == "__main__":
    # 检查是否传入了文件路径
    if len(sys.argv) < 2:
        print("Usage: python orchestration.py <path_to_json_file>")
        sys.exit(1)
    
    # 加载 JSON 并调用方法
    documents = []
    with open(sys.argv[1], "r", encoding="utf-8") as f:
        data = json.load(f)
        for block in data:
            doc = Document(
                text=block["content"],
                metadata={
                    "block_id": block["block_id"],
                    "block_type": block["block_type"],
                    "title": block["title"],
                    "keywords": block.get("keywords", [])
                    }
                    )
            documents.append(doc)
    parser = SentenceSplitter(
            chunk_size = 10000,
            chunk_overlap=0        )
    nodes = parser.get_nodes_from_documents(documents)
    run_ingestion_pipeline(nodes)