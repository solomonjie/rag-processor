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
你是一个专业的新闻内容分析与结构化信息抽取系统，具备高可靠性与低幻觉要求。请严格基于【待分析文本】进行信息提取与总结，禁止编造、推测或引入外部知识。

---

# 【任务说明】

请一次性完成以下所有任务，并确保信息覆盖完整、结构稳定。

---

## 1. 摘要生成 (summary)

生成一个用于语义检索（embedding）的高质量摘要，必须满足：

### 结构要求（三段式）：
- 【核心事实】：一句话概括“谁在何时何地做了什么，结果如何”，≤50字
- 【关键信息】：补充背景、时间、地点、关键数据、涉及主体等，100-150字
- 【主要观点】：总结各方观点、影响、意义或未来展望，100-150字

### 强约束（必须遵守）：
- 必须尽量覆盖以下信息（若原文存在）：
  - 人物 / 公司 / 机构
  - 时间 / 地点
  - 数值（金额、比例、规模等）
  - 关键事件动作（发布 / 融资 / 收购等）
- 不得遗漏关键实体或关键数据
- 不得引入原文未提及的信息
- 语言必须连贯、信息密度高
- ❗摘要中必须包含高辨识度信息（如专有名词、术语、数值），避免过度抽象表达（面向检索优化）

---

## 2. 关键事实提取 (facts)

提取 5-10 条“原子事实”，每条为一个简洁独立的陈述：

### 要求：
- 每条必须为简单句，避免复合句
- 每条必须基于原文明确内容，不可编造
- ❗每条事实应尽量包含以下结构中的至少两项：
  - 主体（人物 / 公司 / 机构）
  - 动作（发布 / 收购 / 投资 / 合作等）
  - 对象或结果
  - 时间 / 数值 / 地点（如有）
- 优先覆盖：
  - 数值信息（金额、时间、比例、规模等）
  - 人物 / 公司 / 机构
  - 关键动作（发布、合作、投资、收购等）
- 各条之间尽量不重复
- 若原文事实不足 5 条，则按实际数量输出；若不足 3 条，允许输出少于 3 条，但需确保未遗漏可拆分的事实

---

## 3. 关键词提取 (keywords)

提取 5-8 个最具代表性的关键词：

### 要求：
- 必须来自原文或原文可直接映射（避免泛化词）
- 优先选择：
  - 人名 / 公司名 / 产品名
  - 专业术语
  - 事件类型词
- 避免无信息量词（如“发展”“影响”等）

---

## 4. 标签分类 (tags)

从【候选标签列表】中选择最匹配的标签：

### 要求：
- 必须且只能从候选列表中选择
- 最多选择 3 个
- 按相关度从高到低排序
- 若无匹配，返回 ["其他"]

---

## 5. 元数据提取 (metadata)

提取以下字段：

- publish_date：
  - 优先使用新闻发布日期（YYYY-MM-DD）
  - 若无，则使用事件发生时间
  - 若仍无法确定，返回 null

- source：
  - 提取新闻来源媒体名称
  - 若未提及，返回 null

- location：
  - 提取主要事件发生地（国家或城市）
  - 若多个，选择最核心的一个
  - 若无法判断，返回 null

- event_type：
  - 从以下类型中选择最匹配的一个（若都不匹配则使用“其他”）：
    - 融资、发布、收购、政策、事故、合作、其他
  - 必须选择最主要的一个，不可输出列表以外的值

---

## 6. 一致性与质量检查（内部执行，不输出）

在输出前，请自行检查：

- summary 是否覆盖关键实体与数值
- facts 中的关键点是否在 summary 中有所体现（允许部分重叠）
- facts 是否为原子事实且无明显遗漏
- 是否存在任何编造或推测
- JSON结构是否完整、字段是否齐全

---

# 【候选标签列表】
{labels_info}

---

# 【输出格式要求】

- 仅输出一个 JSON 对象
- 不要包含任何解释、Markdown 或多余文本
- JSON 必须严格符合以下结构：

{{
  "summary": "【核心事实】...\n\n【关键信息】...\n\n【主要观点】...",
  "facts": ["事实1", "事实2", "事实3"],
  "keywords": ["关键词1", "关键词2", "关键词3", "关键词4", "关键词5"],
  "tags": ["标签1", "标签2"],
  "metadata": {{
    "publish_date": "YYYY-MM-DD 或 null",
    "source": "来源名称或 null",
    "location": "地点或 null",
    "event_type": "融资/发布/收购/政策/事故/合作/其他"
  }}
}}

---

# 【待分析文本】
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
        nodes = [n for n in payload.content.nodes if n.page_content.strip()]
        
        if not nodes:
            return

        labels_info_str = json.dumps(all_tags, ensure_ascii=False)

        self.logger.info(f"执行丰富化，节点数: {len(nodes)}")

        tasks = [
            self._enrich_single_node(node, labels_info_str) 
            for node in nodes
        ]
        await asyncio.gather(*tasks)

    async def _enrich_single_node(self, node: Node, labels_info: str):
        async with self.semaphore:
            try:
                prompt = self.UNIFIED_PROMPT_TEMPLATE.format(
                    labels_info=labels_info,
                    content=node.page_content
                )
            
                response = await self.llm_client.get_llm().acomplete(prompt)
                response_text = str(response.text) if hasattr(response, 'text') else str(response)
                full_result = self._parse_json_response(response_text)
                
                if full_result:
                    # 根据参数按需提取
                    final_meta = {}
                    final_meta["summary"] = full_result.get("summary", "")
                    final_meta["keywords"] = full_result.get("keywords", [])
                    final_meta["tags"] = full_result.get("tags", ["其他"])
                    final_meta["facts"] = full_result.get("facts", ["其他"])
                    
                    node.metadata.update(final_meta)
            except Exception as e:
                self.logger.error(f"节点处理异常: {e}")

    def _parse_json_response(self, text: str) -> Optional[Dict[str, Any]]:
        try:
            match = re.search(r'(\{.*\})', text.replace('\n', ' '), re.DOTALL)
            return json.loads(match.group(1)) if match else json.loads(text)
        except:
            return None