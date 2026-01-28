from constants import EnrichmentMethod
from ..interfaces import BaseEnrichmentStrategy


class QuestionStrategy(BaseEnrichmentStrategy):
    @property
    def method_type(self):
        return EnrichmentMethod.QUESTIONS

    def task_name(self):
        return "suggested_questions"

    def task_description(self):
        return "基于文本内容，生成用户可能会提出的相关问题"

    def output_field(self):
        return "suggested_questions"

    def output_schema(self):
        return {
            "type": "array",
            "items": "string",
            "exact_items": 3
        }

    def quality_rules(self):
        return [
            "问题应具体而非泛问",
            "避免是/否问题"
        ]

    def failure_fallback(self):
        return []