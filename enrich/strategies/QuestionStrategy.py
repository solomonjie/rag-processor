from constants import EnrichmentMethod
from ..interfaces import BaseEnrichmentStrategy


class QuestionStrategy(BaseEnrichmentStrategy):
    method_type = EnrichmentMethod.QUESTIONS
    instruction = "基于文本内容提出 3 个用户可能会问的相关问题。"
    output_field = "suggested_questions"