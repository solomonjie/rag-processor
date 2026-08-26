"""TEI embedding 服务封装。

契约 §3.1：构建端与消费端必须使用同一模型/服务，否则 dense 检索静默报废。
"""
from llama_index.core.embeddings import BaseEmbedding
from llama_index.embeddings.text_embeddings_inference import TextEmbeddingsInference

from common.config import SETTINGS


def get_embed_model() -> BaseEmbedding:
    return TextEmbeddingsInference(
        model_name=SETTINGS.embed_model_name,
        base_url=SETTINGS.embed_url,
        endpoint="/embed",
    )
