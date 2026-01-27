from llama_index.llms.deepseek import DeepSeek
import os
from dotenv import load_dotenv

from constants import LLMClientKey

class LLMClient:
    _llm_instance = None
    
    @classmethod
    def get_llm(cls, **kwargs):
        if cls._llm_instance is None:
            cls._llm_instance = cls._create_llm(**kwargs)
        return cls._llm_instance
    
    @classmethod
    def _create_llm(cls, client_type=LLMClientKey.DeepSeek, **kwargs):
        try:
            load_dotenv()
            if client_type == LLMClientKey.DeepSeek:
                return DeepSeek(model=os.getenv(LLMClientKey.DeepSeekModel), api_key=os.getenv(LLMClientKey.DeepSeekAPIKey))
            else:
                return DeepSeek(model=os.getenv(LLMClientKey.DeepSeekModel), api_key=os.getenv(LLMClientKey.DeepSeekAPIKey))
        except Exception as e:
            raise e
    
    @classmethod
    def reset_instance(cls):
        """重置实例（主要用于测试）"""
        cls._llm_instance = None