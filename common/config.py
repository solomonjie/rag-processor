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
    # 上限而非消耗量：混合推理模型思维链+正文都算在内，给太小思维链
    # 烧光预算后 content 为空串（json.loads 直接失败）
    llm_max_tokens = _int("LLM_Max_Tokens", 4096)

    # ---- 业务 ----
    # 监测主体（逗号分隔）。为空 = 不做相关性过滤（relevant 恒真）
    monitored_entities = tuple(
        e.strip() for e in os.getenv("Monitored_Entities", "").split(",") if e.strip()
    )

    # ---- 数据目录（批次文件夹制：一个批次 = running/<batch>/{inbox,1_cleaned,2_enriched}）----
    inbox_dir = os.getenv("Inbox_Dir", "data/inbox")              # 本地投递区（子目录/散文件，收编为批次）
    running_dir = os.getenv("Running_Dir", "data/running")        # 进行中批次；完成后整目录删除
    dead_letter_dir = os.getenv("Dead_Letter_Dir", "data/dead_letter")  # 各阶段死信（不随批次清理）

    # ---- RocketMQ 直连消费（worker 流模式；Rocketmq_NameSrv 留空 = 文件模式）----
    # 多 NameServer 地址分号分隔；4.x remoting 协议。客户端为 C++ binding（librocketmq，
    # 仅 Linux）——宿主机开发不配置此项，走本地 inbox 文件路径
    rocketmq_namesrv = os.getenv("Rocketmq_NameSrv", "")
    rocketmq_topic = os.getenv("Rocketmq_Topic", "yqms_thirdparty_push")
    rocketmq_group = os.getenv("Rocketmq_Group", "rag-worker")
    # 同 group 多实例 = 横向扩展（队列自动分摊），无需其他协调

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
