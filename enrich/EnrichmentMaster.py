import asyncio
import json
import logging
import re
from typing import List, Dict, Any, Optional
from llm.llm_client import LLMClient
from constants import EnrichmentMethod
from files.DocumentFormat import RAGTaskPayload, Node
from .strategies.SummaryStrategy import SummaryStrategy
from .strategies.KeywordStrategy import KeywordStrategy
from .strategies.QuestionStrategy import QuestionStrategy
from .interfaces import BaseEnrichmentStrategy

class EnrichmentMaster:
    def __init__(self, llm_client: LLMClient, max_concurrency: int = 5):
        self.llm_client = llm_client
        self.logger = logging.getLogger(__name__)
        self._strategy_map = {
            EnrichmentMethod.SUMMARY: SummaryStrategy(),
            EnrichmentMethod.KEYWORDS: KeywordStrategy(),
            EnrichmentMethod.QUESTIONS:QuestionStrategy()
        }
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def process_payload(self, payload: RAGTaskPayload):
        """
        供 Manager 调用的核心入口：将所有 Node 分发为独立的异步任务
        """
        methods = payload.content.pipeline_instructions.enrichment_methods
        active_strategies = [self._strategy_map[m] for m in methods if m in self._strategy_map]
        
        if not active_strategies:
            return

        nodes = [n for n in payload.content.nodes if n.page_content.strip()]
        
        if not nodes:
            return

        self.logger.info(f"开始处理 {len(nodes)} 个节点的丰富化任务 (并发限制: {self.semaphore._value})...")

        # 1. 为每个节点创建独立的协程任务
        tasks = [self._enrich_single_node(node, active_strategies) for node in nodes]

        # 2. 并发执行并等待全部完成
        await asyncio.gather(*tasks)
        self.logger.info("所有节点丰富化处理完成。")

    async def _enrich_single_node(self, node: Node, strategies: List[BaseEnrichmentStrategy]):
        """
        单节点处理器：负责单个 Node 的 Prompt 构建、调用和结果回填
        """
        async with self.semaphore:
            prompt = self._build_single_prompt(node.page_content, strategies)
            
            try:
                # 调用 LLM
                response = await self.llm_client.get_llm().acomplete(prompt)
                response_text = str(response.text) if hasattr(response, 'text') else str(response)
    
                # 解析返回的 JSON
                data = self._parse_json_response(response_text)
                
                if data and isinstance(data, dict):
                    # 直接回填到 Node 的 metadata 中
                    node.metadata.update(data)
                    self.logger.debug(f"节点 {node.id if hasattr(node, 'id') else ''} 处理成功")
                else:
                    self.logger.warning(f"节点处理返回空数据或格式错误")
                    
            except Exception as e:
                self.logger.error(f"单个节点处理失败: {e}", exc_info=False)

    def _parse_json_response(self, response_text: str) -> Optional[Dict[str, Any]]:
        """
        解析单条 JSON 响应，增加了对 Markdown 标记的过滤
        """
        clean_text = response_text.strip()
        # 移除 Markdown 代码块包裹
        if clean_text.startswith("```json"):
            clean_text = clean_text.split("```json")[1].split("```")[0].strip()
        elif clean_text.startswith("```"):
            clean_text = clean_text.split("```")[1].split("```")[0].strip()

        try:
            return json.loads(clean_text)
        except json.JSONDecodeError:
            self.logger.error(f"JSON 解析失败。原始响应: {response_text}")
            return None

    def _build_single_prompt(self, content: str, strategies: List[BaseEnrichmentStrategy]) -> str:
        """
        构造针对单条文本的精简 Prompt
        """
        task_definitions = []
        output_schema = {}

        for s in strategies:
            # 组装任务定义
            task_definitions.append(f"- {s.task_name()}: {s.task_description()}")
            # 组装期望的 JSON 结构
            output_schema[s.output_field()] = s.output_schema()

        return f"""
你是一个专业的结构化信息抽取系统。请分析以下文本，并提取元数据。

【任务指令】
{chr(10).join(task_definitions)}

【输出格式要求】
1. 必须只返回一个纯 JSON 对象。
2. JSON 结构必须符合以下 Schema:
{json.dumps(output_schema, ensure_ascii=False, indent=2)}
3. 不要输出任何解释性文字或 Markdown 标签。

【待分析文本】
---
{content}
---
"""

    def _parse_batch_response(self, response_text: str) -> List[Dict[str, Any]]:
        """
        健壮的 JSON 列表解析逻辑，支持清洗 Markdown 标签
        """
        try:
            # 1. 尝试直接解析
            return json.loads(response_text.strip())
        except json.JSONDecodeError:
            try:
                # 2. 尝试清洗 Markdown 代码块 ```json ... ```
                # 使用正则提取最外层的 [ ] 结构
                match = re.search(r'\[\s*\{.*\}\s*\]', response_text, re.DOTALL)
                if match:
                    return json.loads(match.group())
                
                # 3. 最后的降级：如果 LLM 返回的是多个 JSON 对象而非数组，尝试手动组装
                # 这种情况较少见，但由于 DeepSeek 等模型有时会“吐”额外文字，需做此防御
                potential_json_objects = re.findall(r'\{.*?\}', response_text, re.DOTALL)
                return [json.loads(obj) for obj in potential_json_objects]
            except Exception as e:
                self.logger.warning(f"JSON 解析二次尝试失败: {e}, response 信息 {response_text}")
                return []

    def _build_batch_prompt(self, nodes: List[Node], strategies: List[BaseEnrichmentStrategy]) -> str:
        """
        构造批量处理的 Prompt，通过 BLOCK_ID 区分不同节点
        """
        # 1. 组装所有策略的指令
        task_definitions = []
        for s in strategies:
            schema = s.output_schema()
            rules = "\n".join([f"- {r}" for r in s.quality_rules()])
    
            task_definitions.append(f"""
任务：{s.task_name()}
含义：{s.task_description()}
输出字段："{s.output_field()}"
结构约束：{json.dumps(schema, ensure_ascii=False)}
质量要求：
{rules if rules else "- 无"}
""")

        # 2. 输出字段 contract
        output_fields = {
            s.output_field(): s.failure_fallback()
            for s in strategies
        }
        output_fields["block_id"] = "number"
    
        # 3. 输入内容
        content_blocks = []
        for idx, node in enumerate(nodes):
            content_blocks.append(
                f"=== BLOCK_ID: {idx} ===\n{node.page_content}"
            )
    
        return f"""
你是一个结构化信息抽取系统，而不是聊天助手。
你的任务是：为每个文本块生成可用于 RAG 检索与排序的元数据。

{chr(10).join(task_definitions)}

【失败兜底规则】
- 若文本信息不足，返回该字段的空值（不要编造）
- 必须仍然返回该 block_id 对应的对象

【输出规则（必须严格遵守）】
- 只返回 JSON
- 最外层必须是数组
- 数组长度必须等于 {len(nodes)}
- 每个对象必须包含以下字段：
{json.dumps(output_fields, ensure_ascii=False, indent=2)}
- 不要额外字段
- 不要 Markdown
- 不要解释文字

在输出前，请自检：
- JSON 是否可被直接解析
- block_id 是否完整且连续
- 数组长度是否正确

【待处理文本】
{chr(10).join(content_blocks)}
"""