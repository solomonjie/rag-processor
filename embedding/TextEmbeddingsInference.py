
import os
from typing import  List
from dotenv import load_dotenv
from .interfaces import EmbeddingService
from llama_index.core.embeddings import BaseEmbedding
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference 

class TextEmbeddingService(EmbeddingService):
    def __init__(self):
        load_dotenv()
        #print("Embed_API_URL" + os.getenv('Embed_API_URL'))
        self._embed_model = TextEmbeddingsInference(
            model_name="BAAI/bge-small-zh-v1.5",
            base_url=os.getenv('Embed_API_URL'),
            endpoint="/embed"
            )
    
    def get_embeddings(self, docs: List[str]) -> List[List[float]]:
        return self._embed_model.get_text_embedding_batch(docs)

    @property 
    def embed_model(self) -> BaseEmbedding:
        return self._embed_model