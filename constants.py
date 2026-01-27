from enum import Enum

class ChunkMethod(str, Enum):
    NONE = "none"              # 不分块
    SENTENCE = "sentence"      # 句子/字符长度分块
    SEMANTIC = "semantic"      # 语义相似度分块
    LLM = "llm"                # 大模型智能分块
    FIXED_SIZE = "fixed_size"  # 固定窗口分块


class EnrichmentMethod(str, Enum):
    NONE = "none"                # 不做任何操作
    SUMMARY = "summary"          # 生成摘要
    QUESTIONS = "questions"      # 生成相关问题
    ENTITIES = "entities"        # 命名实体识别
    KEYWORDS = "keywords"        # 提取关键词

class LLMClientKey(str, Enum):
    DeepSeek = "deepseek"
    DeepSeekModel = "DeepSeek_Model_Name"
    DeepSeekAPIKey = "DeepSeek_API_Key"