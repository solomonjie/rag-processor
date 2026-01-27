from constants import EnrichmentMethod
from ..interfaces import BaseEnrichmentStrategy


class KeywordStrategy(BaseEnrichmentStrategy):
    method_type = EnrichmentMethod.KEYWORDS
    instruction = "请从文本中提取 5-8 个核心关键词，以列表形式返回。"
    output_field = "keywords"