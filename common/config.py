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

    # ---- Embedding（私有化 AI 平台 Bifrost 网关，OpenAI 兼容 /v1/embeddings）----
    # 平台由第三方部署维护，本仓库是纯调用方；模型菜单以 /v1/models 为准
    # （litellm-internal/ 前缀）。换模型必须与消费端同步（契约 §3.1），dim 随模型变
    embed_url = os.getenv("Embed_API_URL", "")
    embed_model_name = os.getenv("Embed_Model_Name", "litellm-internal/BAAI/bge-m3")
    embed_dim = _int("Embed_Dim", 1024)
    # 网关虚拟密钥（sk-bf- 前缀）；与 LLM 同一平台，缺省回落 LLM_Api_Key
    embed_api_key = os.getenv("Embed_Api_Key") or os.getenv("LLM_Api_Key", "")

    # ---- 私有 LLM（同一 Bifrost 网关，OpenAI 兼容 API）----
    llm_base_url = os.getenv("LLM_Base_URL", "")
    llm_model = os.getenv("LLM_Model", "litellm-internal/qwen3.8-27b")
    llm_api_key = os.getenv("LLM_Api_Key", "dummy")  # 网关强校验，真实值在 .env
    llm_timeout = _int("LLM_Timeout", 120)
    llm_concurrency = _int("LLM_Concurrency", 16)
    # 上限而非消耗量：混合推理模型思维链+正文都算在内，给太小思维链
    # 烧光预算后 content 为空串（json.loads 直接失败）
    llm_max_tokens = _int("LLM_Max_Tokens", 4096)

    # ---- 私有化平台内部根证书（网关 HTTPS 必需；空 = 公共 CA 校验）----
    # 与 .env 同策略：环境物料运行期注入（docker run -v + -e），不进镜像
    ai_ca_path = os.getenv("AI_CA_Path", "")

    # ---- 业务 ----
    # 监测主体（逗号分隔）。为空 = 不做相关性过滤（relevant 恒真）
    monitored_entities = tuple(
        e.strip() for e in os.getenv("Monitored_Entities", "").split(",") if e.strip()
    )

    # ---- 数据目录（批次文件夹制：一个批次 = running/<batch>/{inbox,1_cleaned,2_enriched}）----
    inbox_dir = os.getenv("Inbox_Dir", "data/inbox")              # 本地投递区（子目录/散文件，收编为批次）
    running_dir = os.getenv("Running_Dir", "data/running")        # 进行中批次；完成后整目录删除
    dead_letter_dir = os.getenv("Dead_Letter_Dir", "data/dead_letter")  # 各阶段死信（不随批次清理）

    # ---- RocketMQ 直连消费（worker 流模式；Rocketmq_Endpoint 留空 = 文件模式）----
    # endpoint 是 Proxy 的 gRPC 地址（默认端口 8081），不是 NameServer 地址。
    # 客户端为官方 rocketmq-python-client（5.x gRPC 协议，纯 Python 无 C++ 依赖，
    # Windows/Linux 均可直跑）
    rocketmq_endpoint = os.getenv("Rocketmq_Endpoint", "")
    rocketmq_topic = os.getenv("Rocketmq_Topic", "yqms_thirdparty_push")
    rocketmq_group = os.getenv("Rocketmq_Group", "rag-worker")
    # ACL（上游开启认证时填；本地/未开启留空即可）
    rocketmq_access_key = os.getenv("Rocketmq_AccessKey", "")
    rocketmq_secret_key = os.getenv("Rocketmq_SecretKey", "")
    # 同 group 多实例 = 横向扩展（pop 消费队列自动分摊），无需其他协调

    # ---- 运行参数 ----
    batch_size = _int("Batch_Size", 64)
    poll_interval = _int("Poll_Interval", 5)
    tag_refresh_seconds = _int("Tag_Refresh_Seconds", 300)

    # ---- 文本上限 ----
    content_max_chars = 60000     # text 字段 VARCHAR(65535) 的安全上限
    judge_input_max_chars = 8000  # LLM 输入用全文，8K 上限仅防病态长文（D9 规则②）


SETTINGS = Settings()

# 内部根证书桥接到 httpx 官方机制：OpenAIEmbedding / AsyncOpenAI 内建的 httpx
# 客户端读 SSL_CERT_FILE（替换语义：进程内隐式 httpx 客户端只信此证书——本进程
# httpx 流量仅 Bifrost 平台，无公网调用）。外部已显式设置时尊重之。
if SETTINGS.ai_ca_path:
    os.environ.setdefault("SSL_CERT_FILE", SETTINGS.ai_ca_path)


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
    if SETTINGS.ai_ca_path and not os.path.exists(SETTINGS.ai_ca_path):
        # 容器漏拷证书 / 路径写错时提前给出明确提示，而不是 TLS 握手玄学报错
        raise SystemExit(f"AI_CA_Path 指向的证书不存在: {SETTINGS.ai_ca_path}")
