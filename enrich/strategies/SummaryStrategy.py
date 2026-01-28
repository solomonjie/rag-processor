from constants import EnrichmentMethod
from ..interfaces import BaseEnrichmentStrategy


class SummaryStrategy(BaseEnrichmentStrategy):
    @property
    def method_type(self):
        return EnrichmentMethod.SUMMARY

    def task_name(self) -> str:
        return "summary"

    def task_description(self) -> str:
        return "对文本的核心内容进行简要概括，突出事实与结论"

    def output_field(self) -> str:
        return "summary"

    def output_schema(self):
        return {
            "type": "string",
            "max_length": 100
        }

    def quality_rules(self):
        return [
            "不要直接复制原文句子",
            "不要加入评价性或推测性语言",
            "保持客观、中性"
        ]

    def failure_fallback(self):
        return ""