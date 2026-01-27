from constants import EnrichmentMethod
from ..interfaces import BaseEnrichmentStrategy


class SummaryStrategy(BaseEnrichmentStrategy):
    method_type = EnrichmentMethod.SUMMARY
    instruction = "请为以下文本生成一段 100 字以内的摘要。"
    output_field = "summary"