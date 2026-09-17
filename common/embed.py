"""私有化 AI 平台 embedding 服务封装（Bifrost 网关，OpenAI 兼容 /v1/embeddings）。

契约 §3.1：构建端与消费端必须使用同一模型/服务，否则 dense 检索静默报废。
认证：网关 401 实测要求虚拟密钥走 x-bf-vk 头（sk-bf- 前缀）；Bearer 由 SDK 自动附加。
CA：走 SSL_CERT_FILE 桥接（common/config.py 从 AI_CA_Path 注入，httpx 官方机制）。
编码格式：网关仅支持 float（线上实测 400 "only float encoding_format is supported"），
而 openai SDK 未显式传参时默认补 encoding_format=base64——必须显式压成 float。
"""
from llama_index.core.embeddings import BaseEmbedding
from llama_index.embeddings.openai import OpenAIEmbedding

from common.config import SETTINGS


def get_embed_model() -> BaseEmbedding:
    return OpenAIEmbedding(
        model_name=SETTINGS.embed_model_name,
        api_base=SETTINGS.embed_url,
        api_key=SETTINGS.embed_api_key,
        default_headers={"x-bf-vk": SETTINGS.embed_api_key},
        additional_kwargs={"encoding_format": "float"},
        timeout=120,  # 远端网关，不用 SDK 默认 600s（防单次卡死拖垮整波）
    )
