"""阶段2（enrich）入口：running/<batch>/1_cleaned/*.jsonl → 2_enriched/*.jsonl。

本阶段不只是判定，还负责全部结构化信息抽取——每条新闻一次私有 LLM 调用（D5）：
- is_ad / is_news / relevant 判定（广告、非新闻、不相关 → 丢弃，由阶段3过滤）；
- tags 从 tag_collection is_active 候选中选 ≤3 个（vllm guided_json 约束解码 + 白名单兜底）；
- region 从省级封闭词表中选（D9 规则①；爬取侧自带合法 region 时优先）。

LLM 之前先做 URL 去重（Milvus 已入库的 node_id 直接跳过，省调用）。
产出含全部判定记录（含被丢弃的，带 verdict 字段）供审计；LLM 失败的条目进死信。
独立运行：python -m enrich.run --once
"""
import argparse
import asyncio
import json
import logging
import os
import time
from collections import Counter
from dataclasses import asdict, dataclass, field
from typing import List, Sequence, Tuple

from openai import AsyncOpenAI
from pymilvus import MilvusClient

from common.batch import CLEANED, ENRICHED, iter_batches, stage_dir
from common.config import SETTINGS
from common.tags import get_tagstore
from common.utils import (dead_letter, ensure_dirs, iter_input_files, move_done,
                          now_stamp, read_jsonl, write_jsonl)

log = logging.getLogger("enrich.run")


class EnrichError(Exception):
    """LLM 重试耗尽或输出非法 → 该条进死信。"""


# 省级行政区标准名（封闭词表；将来需要市级粒度时在消费端词表与本表同步扩充）
REGIONS = [
    "未知",
    "北京市", "天津市", "河北省", "山西省", "内蒙古自治区", "辽宁省", "吉林省", "黑龙江省",
    "上海市", "江苏省", "浙江省", "安徽省", "福建省", "江西省", "山东省",
    "河南省", "湖北省", "湖南省", "广东省", "广西壮族自治区", "海南省",
    "重庆市", "四川省", "贵州省", "云南省", "西藏自治区",
    "陕西省", "甘肃省", "青海省", "宁夏回族自治区", "新疆维吾尔自治区",
    "香港特别行政区", "澳门特别行政区", "台湾省",
]
UNKNOWN_REGION = "未知"

SYSTEM_PROMPT = (
    "你是新闻审核与标注助手。只输出 JSON，不输出任何解释文字。"
    "判断基于正文证据，广告/软文判定宁可保守：拿不准时 is_ad 输出 true。"
)


@dataclass
class Verdict:
    keep: bool
    drop_reason: str = ""          # ad / not_news / irrelevant
    tags: List[str] = field(default_factory=list)
    region: str | None = None      # 省级标准名；判断不了为 None
    why: str = ""                  # 一句话依据，供抽检


# ---------------------------------------------------------------- 判定核心

def build_user_prompt(item: dict, tags: Sequence[Tuple[str, str]]) -> str:
    tag_lines = "\n".join(f"- {name}：{desc or '（无描述）'}" for name, desc in tags)
    if SETTINGS.monitored_entities:
        entity_part = "、".join(SETTINGS.monitored_entities)
    else:
        entity_part = "（未配置监测主体，relevant 一律输出 true）"
    full_text = f"标题: {item['title']}\n{item['content']}"[:SETTINGS.judge_input_max_chars]
    return f"""【任务】对下面的新闻完成判定与标注：

1. is_ad：是否为广告/软文/推广/导购/垃圾页面
2. is_news：是否为正常新闻内容（资讯、报道、评论、讨论帖等有信息量的内容）
3. relevant：是否与监测主体相关（监测主体：{entity_part}）
4. tags：从【候选标签】中选 1~3 个最匹配的，只能从候选里选，不得自造
5. region：新闻发生地，从【地区列表】中选一个；正文没有明确发生地时选 "未知"

【候选标签】
{tag_lines}

【地区列表】
{"、".join(REGIONS)}

【新闻】
{full_text}"""


def _guided_schema(tag_names: List[str]) -> dict:
    return {
        "type": "object",
        "properties": {
            "is_ad": {"type": "boolean"},
            "is_news": {"type": "boolean"},
            "relevant": {"type": "boolean"},
            "tags": {
                "type": "array",
                "items": {"type": "string", "enum": tag_names},
                "minItems": 1,
                "maxItems": 3,
            },
            "region": {"type": "string", "enum": REGIONS},
            "why": {"type": "string"},
        },
        "required": ["is_ad", "is_news", "relevant", "tags", "region", "why"],
        "additionalProperties": False,
    }


def _to_verdict(data: dict, allowed_tags: set) -> Verdict:
    tags = [t for t in data.get("tags") or [] if t in allowed_tags]
    region = data.get("region")
    region = region if region in REGIONS and region != UNKNOWN_REGION else None
    why = (data.get("why") or "")[:500]

    if data.get("is_ad") is True:
        return Verdict(False, "ad", tags, region, why)
    if data.get("is_news") is False:
        return Verdict(False, "not_news", tags, region, why)
    if data.get("relevant") is False:
        return Verdict(False, "irrelevant", tags, region, why)
    if not tags:
        # 保留的新闻必须至少有一个有效标签，否则消费端 tags 过滤永远漏掉它
        raise EnrichError("no_valid_tags")
    return Verdict(True, "", tags[:3], region, why)


# vllm 服务端不支持 guided_json 时自动降级为普通生成+白名单校验
_use_guided = True


async def _enrich_one(client: AsyncOpenAI, sem: asyncio.Semaphore,
                     item: dict, tags) -> Verdict:
    global _use_guided
    tag_names = [t[0] for t in tags]
    allowed = set(tag_names)
    prompt = build_user_prompt(item, tags)

    async with sem:
        for attempt in range(1, 4):
            kwargs = dict(
                model=SETTINGS.llm_model,
                messages=[
                    {"role": "system", "content": SYSTEM_PROMPT},
                    {"role": "user", "content": prompt},
                ],
                temperature=0,
                max_tokens=300,
                timeout=SETTINGS.llm_timeout,
            )
            if _use_guided:
                kwargs["extra_body"] = {"guided_json": _guided_schema(tag_names)}
            try:
                resp = await client.chat.completions.create(**kwargs)
                data = json.loads(resp.choices[0].message.content)
                return _to_verdict(data, allowed)
            except EnrichError:
                raise  # 输出结构问题重试也没用，直接死信
            except Exception as e:
                msg = str(e)
                if _use_guided and ("guided" in msg.lower() or "400" in msg):
                    log.warning("服务端不支持 guided_json，降级为普通生成 + 白名单校验")
                    _use_guided = False
                    continue
                log.warning("LLM 调用失败(第 %d 次) url=%s: %s", attempt, item["url"], msg)
                await asyncio.sleep(2 * attempt)
    raise EnrichError(f"llm_failed url={item['url']}")


async def _enrich_batch(items: List[dict], tags) -> list:
    client = AsyncOpenAI(
        base_url=SETTINGS.llm_base_url,
        api_key=SETTINGS.llm_api_key,
        timeout=SETTINGS.llm_timeout,
    )
    sem = asyncio.Semaphore(SETTINGS.llm_concurrency)
    tasks = [_enrich_one(client, sem, it, tags) for it in items]
    return await asyncio.gather(*tasks, return_exceptions=True)


def enrich_batch(items: List[dict], tags) -> list:
    """并发判定一批条目。返回与 items 等长的列表：Verdict 或 EnrichError。"""
    return asyncio.run(_enrich_batch(items, tags))


# ---------------------------------------------------------------- URL 去重

def _existing_ids(node_ids: List[str]) -> set:
    """查内容库已入库的 node_id；collection 还不存在时视为无重复（首跑）。"""
    if not node_ids:
        return set()
    try:
        client = MilvusClient(uri=SETTINGS.milvus_url, token=SETTINGS.milvus_token)
        if not client.has_collection(SETTINGS.collection):
            return set()
        found = set()
        for i in range(0, len(node_ids), 100):
            chunk = node_ids[i:i + 100]
            expr = "id in [" + ",".join(json.dumps(n) for n in chunk) + "]"
            rows = client.query(SETTINGS.collection, filter=expr,
                                output_fields=["id"], limit=len(chunk))
            found.update(r["id"] for r in rows)
        return found
    except Exception as e:
        log.warning("去重查询失败（本次跳过去重，靠 upsert 幂等兜底）: %s", e)
        return set()


# ---------------------------------------------------------------- sweep

def sweep() -> int:
    """处理所有进行中批次的 1_cleaned，返回处理文件数。"""
    batches = iter_batches()
    if not batches:
        return 0

    tag_list = get_tagstore().get()
    if not tag_list:
        # 无可用标签时不判定（否则全部 no_valid_tags 死信），文件留待重试
        log.error("tag_collection 为空或不可达，enrich 暂不处理")
        return 0

    done = 0
    for bdir in batches:
        batch = os.path.basename(bdir)
        in_dir = stage_dir(bdir, CLEANED)
        out_dir = stage_dir(bdir, ENRICHED)
        ensure_dirs(in_dir, out_dir, SETTINGS.dead_letter_dir)
        for path in iter_input_files(in_dir, ["*.jsonl"]):
            name = os.path.basename(path)
            rows, bad = read_jsonl(path)
            stats = Counter(records=len(rows), bad_lines=bad)

            # URL 去重：已入库跳过 + 文件内重复只留第一条
            existing = _existing_ids([r["node_id"] for r in rows if "node_id" in r])
            seen, fresh = set(), []
            for r in rows:
                nid = r.get("node_id")
                if not nid:
                    stats["bad_lines"] += 1
                    continue
                if nid in existing or nid in seen:
                    stats["dedup_skipped"] += 1
                    continue
                seen.add(nid)
                fresh.append(r)

            out_rows, deads = [], []
            if fresh:
                verdicts = enrich_batch(fresh, tag_list)
                for it, v in zip(fresh, verdicts):
                    if isinstance(v, EnrichError):
                        deads.append({"stage": "enrich", "reason": str(v), "item": it})
                        stats["dead_letter"] += 1
                    else:
                        # 爬取侧自带合法 region 时优先于 LLM 抽取（结构化字段更可靠）
                        hint = it.get("region")
                        if hint in REGIONS and hint != UNKNOWN_REGION:
                            v.region = hint
                        if not v.keep:
                            stats[v.drop_reason] += 1
                        out_rows.append({**it, "verdict": asdict(v)})

            if out_rows:
                stats["out"] = len(out_rows)
                write_jsonl(os.path.join(out_dir, f"{now_stamp()}_{name}"), out_rows)
            if deads:
                p = dead_letter(SETTINGS.dead_letter_dir, "enrich", f"{batch}_{name}", deads)
                log.warning("enrich 死信 %d 条 → %s", len(deads), p)
            move_done(path)
            done += 1
            log.info("[enrich] %s/%s | %s", batch, name, _fmt(stats))
    return done


def _fmt(c: Counter) -> str:
    order = ["records", "bad_lines", "dedup_skipped", "out",
             "ad", "not_news", "irrelevant", "dead_letter"]
    parts = [f"{k}={c[k]}" for k in order if c[k]]
    parts += [f"{k}={v}" for k, v in sorted(c.items()) if k not in order]
    return " ".join(parts) or "nothing"


def main() -> None:
    parser = argparse.ArgumentParser(description="阶段2 enrich：LLM 判定+打标+信息抽取")
    parser.add_argument("--once", action="store_true", help="处理完当前积压后退出")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO,
                        format="%(asctime)s | %(levelname)s | %(name)s | %(message)s")
    while True:
        n = sweep()
        if args.once:
            if n == 0:
                break
        elif n == 0:
            time.sleep(SETTINGS.poll_interval)
    log.info("enrich 阶段结束")


if __name__ == "__main__":
    main()
