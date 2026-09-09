"""集中配置：全部来自环境变量（.env），字段说明见 README.md。

架构依据：data/架构决策记录-2026-08-26.md
契约依据：data/知识库构建规范-消费端数据契约.md
"""
import os
from dotenv import load_dotenv

load_dotenv()


def _int(name: str, default: int) -> int:
    try:
        return int(os.getenv(name, str(default)))
    except ValueError:
        return default


class Settings:
    # ---- Milvus ----
    milvus_url = os.getenv("Milvus_Server_URL", "")
    milvus_token = os.getenv("Milvus_Server_TOKEN", "")
    collection = os.getenv("Milvus_Collection_Name", "product_knowledge_base")
    tag_collection = os.getenv("Tag_Collection_Name", "tag_collection")
    ttl_days = _int("Milvus_TTL_Days", 180)  # 契约只需近 90 天，留一倍余量（D8）

    # ---- Embedding（TEI 服务）----
    # 注意：换模型必须与消费端同步（契约 §3.1 查询侧硬绑定），dim 随模型变
    embed_url = os.getenv("Embed_API_URL", "")
    embed_model_name = os.getenv("Embed_Model_Name", "Qwen/Qwen3-Embedding-0.6B")
    embed_dim = _int("Embed_Dim", 1024)

    # ---- 私有 LLM（vllm，OpenAI 兼容 API，其他任务可复用该服务）----
    llm_base_url = os.getenv("LLM_Base_URL", "")
    llm_model = os.getenv("LLM_Model", "Qwen/Qwen3.6-35B-A3B")
    llm_api_key = os.getenv("LLM_Api_Key", "dummy")  # vllm 不校验，SDK 要求非空
    llm_timeout = _int("LLM_Timeout", 120)
    llm_concurrency = _int("LLM_Concurrency", 16)

    # ---- 业务 ----
    # 监测主体（逗号分隔）。为空 = 不做相关性过滤（relevant 恒真）
    monitored_entities = tuple(
        e.strip() for e in os.getenv("Monitored_Entities", "").split(",") if e.strip()
    )

    # ---- 数据目录（批次文件夹制：一个批次 = running/<batch>/{inbox,1_cleaned,2_enriched}）----
    inbox_dir = os.getenv("Inbox_Dir", "data/inbox")              # 本地投递区（子目录/散文件，收编为批次）
    running_dir = os.getenv("Running_Dir", "data/running")        # 进行中批次；完成后整目录删除
    dead_letter_dir = os.getenv("Dead_Letter_Dir", "data/dead_letter")  # 各阶段死信（不随批次清理）

    # ---- MinIO 批次源（可选；Minio_Endpoint 留空 = 纯本地模式）----
    # 约定：bucket 的 Minio_Prefix 下一级前缀 = 一个批次（raw/<batch_id>/**）
    minio_endpoint = os.getenv("Minio_Endpoint", "")              # host:port
    minio_access_key = os.getenv("Minio_Access_Key", "")
    minio_secret_key = os.getenv("Minio_Secret_Key", "")
    minio_bucket = os.getenv("Minio_Bucket", "rag")
    minio_prefix = os.getenv("Minio_Prefix", "raw")
    minio_secure = os.getenv("Minio_Secure", "false").lower() in ("1", "true", "yes")
    # 批次入库后是否删除远端前缀。MinIO 定位是暂存：入 Milvus 即删，worker 无需固定盘
    minio_delete_on_done = os.getenv("Minio_Delete_On_Done", "true").lower() in ("1", "true", "yes")
    # 死信远端前缀（不随批次清理；worker 无固定盘时这是死信的持久层）
    minio_deadletter_prefix = os.getenv("Minio_Deadletter_Prefix", "deadletter")

    # ---- Kafka 落盘器（kafka_ingest.py 独立进程；Kafka_Bootstrap 留空 = 不启用）----
    kafka_bootstrap = os.getenv("Kafka_Bootstrap", "")
    kafka_topic = os.getenv("Kafka_Topic", "")
    kafka_group = os.getenv("Kafka_Group", "rag-ingest")
    ingest_window_minutes = _int("Ingest_Window_Minutes", 5)  # 消息无 batch_id 时的时间窗口聚合

    # ---- 运行参数 ----
    batch_size = _int("Batch_Size", 64)
    poll_interval = _int("Poll_Interval", 5)
    tag_refresh_seconds = _int("Tag_Refresh_Seconds", 300)

    # ---- 文本上限 ----
    content_max_chars = 60000     # text 字段 VARCHAR(65535) 的安全上限
    judge_input_max_chars = 8000  # LLM 输入用全文，8K 上限仅防病态长文（D9 规则②）


SETTINGS = Settings()


def validate() -> None:
    """启动时校验必需的外部服务地址，缺失直接终止并给出明确提示。"""
    missing = [
        (name, value)
        for name, value in [
            ("Milvus_Server_URL", SETTINGS.milvus_url),
            ("Embed_API_URL", SETTINGS.embed_url),
            ("LLM_Base_URL", SETTINGS.llm_base_url),
        ]
        if not value
    ]
    if missing:
        names = ", ".join(n for n, _ in missing)
        raise SystemExit(f"缺少必需环境变量: {names}（检查 .env）")
