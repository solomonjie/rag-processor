from abc import ABC, abstractmethod
from typing import List
from llama_index.core.embeddings import BaseEmbedding

class EmbeddingService:
    def __init__(self):
        pass

    @abstractmethod
    def get_embeddings(self, docs: List[str]) -> List[List[float]]:
        pass

    @property
    @abstractmethod
    def embed_model(self) -> BaseEmbedding:
        """Get the embedding model."""
        pass