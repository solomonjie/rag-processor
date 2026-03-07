import asyncio
import json
import logging
import re
from typing import List, Dict, Any, Optional
from llm.llm_client import LLMClient
from constants import EnrichmentMethod
from files.DocumentFormat import RAGTaskPayload, Node
from .interfaces import BaseEnrichmentStrategy

class EnrichmentMaster:
    UNIFIED_PROMPT_TEMPLATE = """
你是一个专业的内容分析与结构化信息抽取系统。请仔细阅读【待分析文本】，并一次性完成以下任务。

### 任务指令：
1. **摘要 (summary)**：用一句话准确概括核心内容，字数严格控制在 50 字以内。
2. **关键词 (keywords)**：提取 3-5 个最能代表文本主题的专业词汇，以字符串数组形式返回。
3. **标签分类 (tags)**：
   - 从下方的【候选标签列表】中选出与内容最贴近的标签。
   - **必须且只能从列表中选择**；严禁创造、改写或拼接任何新标签。
   - 若文本对应多个标签，最多选 3 个，并按相关度由高到低排列。
   - 若内容与任何标签都不匹配，则输出 ["其他"]。

### 候选标签列表（JSON）：
{labels_info}

### 输出要求：
- 必须只返回一个纯 JSON 对象，不要包含 Markdown 标签或任何多余文字。
- JSON 结构必须严格符合以下定义：
{{
  "summary": "文本摘要内容",
  "keywords": ["关键词1", "关键词2", "关键词3"],
  "tags": ["标签1", "标签2"]
}}

### 待分析文本：
---
{content}
---
"""

    def __init__(self, llm_client: LLMClient, max_concurrency: int = 5):
        self.llm_client = llm_client
        self.logger = logging.getLogger(__name__)
        self.semaphore = asyncio.Semaphore(max_concurrency)

    async def process_payload(self, payload: RAGTaskPayload, all_tags: List[str]):
        """
        供 Manager 调用的核心入口：将所有 Node 分发为独立的异步任务
        """
        methods = payload.content.pipeline_instructions.enrichment_methods
        nodes = [n for n in payload.content.nodes if n.page_content.strip()]
        
        if not nodes:
            return

        labels_info_str = json.dumps(all_tags, ensure_ascii=False)

        self.logger.info(f"执行丰富化，节点数: {len(nodes)}")

        tasks = [
            self._enrich_single_node(node, labels_info_str, methods) 
            for node in nodes
        ]
        await asyncio.gather(*tasks)

    async def _enrich_single_node(self, node: Node, labels_info: str, requested_methods: List[str]):
        async with self.semaphore:
            prompt = self.UNIFIED_PROMPT_TEMPLATE.format(
                labels_info=labels_info,
                content=node.page_content
            )
            
            try:
                response = await self.llm_client.get_llm().acomplete(prompt)
                response_text = str(response.text) if hasattr(response, 'text') else str(response)
                full_result = self._parse_json_response(response_text)
                
                if full_result:
                    # 根据参数按需提取
                    final_meta = {}
                    if EnrichmentMethod.SUMMARY in requested_methods:
                        final_meta["summary"] = full_result.get("summary", "")
                    if EnrichmentMethod.KEYWORDS in requested_methods:
                        final_meta["keywords"] = full_result.get("keywords", [])
                    if EnrichmentMethod.TAGGING in requested_methods:
                        final_meta["tags"] = full_result.get("tags", ["其他"])
                    
                    node.metadata.update(final_meta)
            except Exception as e:
                self.logger.error(f"节点处理异常: {e}")

    def _parse_json_response(self, text: str) -> Optional[Dict[str, Any]]:
        try:
            match = re.search(r'(\{.*\})', text.replace('\n', ' '), re.DOTALL)
            return json.loads(match.group(1)) if match else json.loads(text)
        except:
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