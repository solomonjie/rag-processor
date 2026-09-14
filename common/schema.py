"""Milvus collection 显式 schema（契约 §2.2）+ TTL + 索引。

为什么显式 schema 而不是让 llama-index 自动建表：
- 契约的过滤键 tags/published_date 必须是强类型列（ARRAY / VARCHAR），
  自动建表的 enable_dynamic_field=True 会把元数据全塞进 $meta 动态字段；
- 建库时给 region 占列（D9 规则③：将来当过滤键的字段必须有显式列）；
- llama-index 的 node_to_metadata_dict 插入时会额外写 document_id/doc_id/ref_doc_id
  三列（值均为字符串 "None"），enable_dynamic_field=False 下缺列会插入失败，
  所以这里必须一并声明。
"""
import logging

from pymilvus import DataType, Function, FunctionType, MilvusClient

from common.config import SETTINGS

log = logging.getLogger("schema")

# 契约 §2.3：analyzer 与消费端查询侧一致（jieba + cnalphanumonly）
ANALYZER_PARAMS = {"tokenizer": "jieba", "filter": ["cnalphanumonly"]}

REQUIRED_FIELDS = {
    "id", "text", "embedding", "sparse_embedding",
    "_node_content", "_node_type", "document_id", "doc_id", "ref_doc_id",
    "title", "url", "source", "published_date", "region", "tags",
}


def _build_schema():
    schema = MilvusClient.create_schema(auto_id=False, enable_dynamic_field=False)
    # ---- 契约核心字段 ----
    schema.add_field("id", DataType.VARCHAR, is_primary=True, max_length=65535)
    schema.add_field(
        "text", DataType.VARCHAR, max_length=65535,
        enable_analyzer=True, analyzer_params=ANALYZER_PARAMS,
    )
    schema.add_field("embedding", DataType.FLOAT_VECTOR, dim=SETTINGS.embed_dim)
    schema.add_field("sparse_embedding", DataType.SPARSE_FLOAT_VECTOR)
    # ---- llama-index 插入路径必带的元数据列 ----
    schema.add_field("_node_content", DataType.VARCHAR, max_length=65535)
    schema.add_field("_node_type", DataType.VARCHAR, max_length=64)
    schema.add_field("document_id", DataType.VARCHAR, max_length=65535)
    schema.add_field("doc_id", DataType.VARCHAR, max_length=65535)
    schema.add_field("ref_doc_id", DataType.VARCHAR, max_length=65535)
    # ---- 节点元数据列（消费端过滤键 + url 去重键 + region 预留列）----
    schema.add_field("title", DataType.VARCHAR, max_length=2048)  # 按字节计：512≈170汉字偏紧，真实样例出过541字节标题
    schema.add_field("url", DataType.VARCHAR, max_length=2048)
    schema.add_field("source", DataType.VARCHAR, max_length=255)
    schema.add_field("published_date", DataType.VARCHAR, max_length=64)
    schema.add_field("region", DataType.VARCHAR, max_length=255)  # D9 预留过滤键
    schema.add_field(
        "tags", DataType.ARRAY, element_type=DataType.VARCHAR,
        max_capacity=32, max_length=255,
    )
    # 服务端 BM25：text → sparse_embedding，构建端不预计算稀疏向量
    schema.add_function(Function(
        name="text_bm25",
        function_type=FunctionType.BM25,
        input_field_names=["text"],
        output_field_names=["sparse_embedding"],
    ))
    return schema


def _build_index_params(client: MilvusClient):
    idx = client.prepare_index_params()
    # 契约 §2.2：dense 用 IP（embedding 模型输出已归一化）
    idx.add_index("embedding", index_type="HNSW", metric_type="IP",
                  params={"M": 16, "efConstruction": 200})
    idx.add_index("sparse_embedding", index_type="SPARSE_INVERTED_INDEX",
                  metric_type="BM25")
    return idx


def ensure_collection(client: MilvusClient) -> None:
    """不存在则按显式 schema 建表（含 TTL，D8）；已存在则校验必备字段后加载。"""
    name = SETTINGS.collection
    if client.has_collection(name):
        desc = client.describe_collection(name)
        have = {f["name"] for f in desc["fields"]}
        missing = REQUIRED_FIELDS - have
        if missing:
            raise RuntimeError(
                f"collection {name} 缺少字段 {sorted(missing)}，"
                f"与契约 schema 不符，请用 create_collection.py --drop 重建"
            )
        client.load_collection(name)
        log.info("collection %s 已存在，已加载", name)
        return

    client.create_collection(
        name,
        schema=_build_schema(),
        index_params=_build_index_params(client),
        properties={"collection.ttl.seconds": SETTINGS.ttl_days * 86400},
    )
    client.load_collection(name)
    log.info("collection %s 创建完成（dim=%d, TTL=%d 天）", name, SETTINGS.embed_dim, SETTINGS.ttl_days)
