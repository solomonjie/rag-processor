import asyncio
import json
import logging
import re
from typing import List, Dict, Any
from llm.llm_client import LLMClient
from constants import EnrichmentMethod
from files.DocumentFormat import RAGTaskPayload, Node
from .strategies.SummaryStrategy import SummaryStrategy
from .strategies.KeywordStrategy import KeywordStrategy
from .strategies.QuestionStrategy import QuestionStrategy
from .interfaces import BaseEnrichmentStrategy

class EnrichmentMaster:
    def __init__(self, llm_client: LLMClient, batch_size: int = 10):
        self.llm_client = llm_client
        self.logger = logging.getLogger(__name__)
        self._strategy_map = {
            EnrichmentMethod.SUMMARY: SummaryStrategy(),
            EnrichmentMethod.KEYWORDS: KeywordStrategy(),
            EnrichmentMethod.QUESTIONS:QuestionStrategy()
        }
        self.batch_size = batch_size
        self.semaphore = asyncio.Semaphore(5)

    async def process_payload(self, payload: RAGTaskPayload):
        """
        供 Manager 调用的核心入口
        """
        methods = payload.content.pipeline_instructions.enrichment_methods
        active_strategies = [self._strategy_map[m] for m in methods if m in self._strategy_map]
        
        if not active_strategies:
            return

        nodes = [n for n in payload.content.nodes if n.page_content.strip()]
        # 并发处理所有节点
        batch_tasks = []
        for i in range(0, len(nodes), self.batch_size):
            batch_nodes = nodes[i : i + self.batch_size]
            # 创建协程对象并放入列表
            task = self._enrich_batch(batch_nodes, active_strategies)
            batch_tasks.append(task)

        # 3. 关键：统一等待所有批次执行完成
        if batch_tasks:
            self.logger.info(f"总共{len(nodes)}个Node，开始并发处理 {len(batch_tasks)} 个批次...")
            await asyncio.gather(*batch_tasks)
            self.logger.info("所有批次丰富化处理完成。")

    async def _enrich_batch(self, batch_nodes: List[Node], strategies: List[Any]):
        """
        异步批次处理器：发送请求并回填结果
        """
        async with self.semaphore:
            prompt = self._build_batch_prompt(batch_nodes, strategies)
            
            try:
                # 调用 LlamaIndex 的异步接口 (acomplete)
                response = await self.llm_client.get_llm().acomplete(prompt)
                response_text = str(response.text) if hasattr(response, 'text') else str(response)
    
                # 解析返回的 JSON 列表
                results = self._parse_batch_response(response_text)
                
                # 建立 ID 映射表，防止顺序错乱
                result_map = {
                    item.get('block_id'): item 
                    for item in results 
                    if isinstance(item, dict) and 'block_id' in item
                }
    
                # 遍历当前批次的节点进行回填
                successCount = 0
                failedCount =0
                for idx, node in enumerate(batch_nodes):
                    # 尝试从映射表中根据 ID 获取结果，如果 ID 不匹配则降级使用索引
                    data = result_map.get(idx) or (results[idx] if idx < len(results) else None)
                    
                    if data and isinstance(data, dict):
                        # 移除辅助用的 block_id，只保留有价值的 metadata
                        data.pop('block_id', None)
                        node.metadata.update(data)
                        successCount = successCount + 1
                    else:
                        failedCount = failedCount + 1
                
                self.logger.info(f"批次处理完成，成功: {successCount}， 失败 {failedCount}")        
            except Exception as e:
                self.logger.error(f"批次处理失败: {e}", exc_info=True)

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