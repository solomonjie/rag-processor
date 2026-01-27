import asyncio
import json
import logging
from typing import List, Dict, Any
from constants import EnrichmentMethod
from files.DocumentFormat import RAGTaskPayload, Node
from .strategies import SummaryStrategy, KeywordStrategy
from .interfaces import BaseEnrichmentStrategy

class EnrichmentMaster:
    def __init__(self, llm_client):
        self.llm_client = llm_client
        self.logger = logging.getLogger(__name__)
        self._strategy_map = {
            EnrichmentMethod.SUMMARY: SummaryStrategy,
            EnrichmentMethod.KEYWORDS: KeywordStrategy
        }

    async def process_payload(self, payload: RAGTaskPayload):
        """
        供 Manager 调用的核心入口
        """
        methods = payload.content.pipeline_instructions.enrichment_methods
        active_strategies = [self._strategy_map[m] for m in methods if m in self._strategy_map]
        
        if not active_strategies:
            return

        # 并发处理所有节点
        tasks = [
            self._enrich_single_node(node, active_strategies) 
            for node in payload.content.nodes 
            if node.page_content.strip()
        ]
        
        if tasks:
            await asyncio.gather(*tasks)

    async def _enrich_single_node(self, node: Node, strategies: List[BaseEnrichmentStrategy]):
        """
        对单个节点进行 LLM 调用并更新 Metadata
        """
        prompt = self._build_prompt(node.page_content, strategies)
        try:
            # 假设 llm_client.ask 是异步的
            raw_response = await self.llm_client.ask(prompt)
            enriched_json = self._parse_response(raw_response)
            
            # 将结果合并到节点的元数据中
            if enriched_json:
                node.metadata.update(enriched_json)
        except Exception as e:
            self.logger.error(f"Node enrichment failed: {e}")

    def _build_prompt(self, text: str, strategies: List[BaseEnrichmentStrategy]) -> str:
        instructions = "\n".join([f"- {s.instruction}" for s in strategies])
        format_json = {s.output_field: "value" for s in strategies}
        
        return f"""你是一个文档分析专家。请根据提供的文本完成以下任务：
{instructions}

必须以 JSON 格式返回，示例格式：{json.dumps(format_json)}

文本内容：
{text}
"""

    def _parse_response(self, response: str) -> Dict[str, Any]:
        # 简单的 JSON 提取逻辑
        try:
            return json.loads(response)
        except:
            # 容错：处理可能带 Markdown 代码块的情况
            import re
            match = re.search(r'\{.*\}', response, re.DOTALL)
            return json.loads(match.group()) if match else {}