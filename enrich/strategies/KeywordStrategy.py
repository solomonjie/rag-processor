from constants import EnrichmentMethod
from ..interfaces import BaseEnrichmentStrategy


class KeywordStrategy(BaseEnrichmentStrategy):
    @property
    def method_type(self):
        return EnrichmentMethod.KEYWORDS

    def task_name(self):
        return "keywords"

    def task_description(self):
        return "提取最能代表文本主题的关键词"

    def output_field(self):
        return "keywords"

    def output_schema(self):
        return {
            "type": "array",
            "items": "string",
            "min_items": 5,
            "max_items": 8
        }

    def quality_rules(self):
        return [
            "关键词应为名词或名词短语",
            "避免过于泛化的词"
        ]

    def failure_fallback(self):
        return []