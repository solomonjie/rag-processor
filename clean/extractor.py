"""record → clean 阶段产物（NewsItem dict）。

- 有 html，或 content 里检出 HTML 块级标签（上游 content 实测多为网页 HTML，
  xlsx 62 行样例里 37/38 行是 div 套 div 的正文）→ trafilatura 抽正文
  （D7：LLM 永远不碰原始 HTML）；trafilatura 认不出结构的弱片段剥标签兜底；
- content 是纯文本 → 直通。

published_date 全部代码归一化（P0-1），LLM 不参与时间解析。
"""
import hashlib
import json
import logging
import re
from datetime import datetime, timedelta, timezone
from html import unescape
from urllib.parse import urlsplit

import trafilatura

from common.config import SETTINGS

log = logging.getLogger("clean.extractor")


class ExtractError(Exception):
    """reason ∈ DROP_REASONS 的按垃圾丢弃，其余（no_publish_date 等）进死信。"""

    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


# 这些失败说明该记录本身没有新闻价值（缺内容/抽不出正文/正文过短），直接丢弃计数
DROP_REASONS = {
    "missing_content", "extract_failed",
    "extract_output_invalid", "content_too_short",
}


_DT_FORMATS = (
    "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S",
    "%Y-%m-%dT%H:%M", "%Y-%m-%d %H:%M", "%Y/%m/%d %H:%M",
    "%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%d %H:%M:%S.%f",
    "%Y年%m月%d日 %H:%M:%S", "%Y年%m月%d日 %H:%M",
)
_DAY_FORMATS = ("%Y-%m-%d", "%Y/%m/%d", "%Y.%m.%d", "%Y年%m月%d日")

_TITLE_NOISE = re.compile(r"\s*[@＠]\S+\s*$")  # 标题尾部 "@作者" 噪声

# content 字段检出块级标签即视为 HTML（纯文本新闻正文不会出现这些）
_HTML_TAG_RE = re.compile(r"</?(?:div|p|br|span|img|section|article|h[1-6]|ul|ol|li)\b", re.I)


def _strip_tags(s: str) -> str:
    """trafilatura 认不出结构的弱 HTML 片段（如裸 <p>）兜底：剥标签保段落换行。"""
    s = re.sub(r"(?is)<(script|style).*?</\1>", "", s)
    s = re.sub(r"(?i)</?(?:div|p|br|li|h[1-6]|ul|ol|tr)[^>]*>", "\n", s)
    s = re.sub(r"<[^>]+>", "", s)
    return "\n".join(ln.strip() for ln in unescape(s).splitlines() if ln.strip())


def _epoch_to_iso(v: float) -> str | None:
    """Unix 时间戳（秒/毫秒）→ 东八区 naive ISO。无效值返回 None。"""
    tz8 = timezone(timedelta(hours=8))
    try:
        if 1e9 <= v < 1e11:      # 秒级（2001~5138 年）
            dt = datetime.fromtimestamp(v, tz=timezone.utc).astimezone(tz8)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        if 1e12 <= v < 1e13:     # 毫秒级
            dt = datetime.fromtimestamp(v / 1000, tz=timezone.utc).astimezone(tz8)
            return dt.strftime("%Y-%m-%dT%H:%M:%S")
        if 30000 < v < 80000:    # Excel 日期序列号
            return (datetime(1899, 12, 30) + timedelta(days=v)).strftime("%Y-%m-%dT%H:%M:%S")
    except (OverflowError, OSError, ValueError):
        return None
    return None


def normalize_datetime(raw) -> str | None:
    """常见时间表述 → 定长 ISO 字符串（YYYY-MM-DDTHH:MM:SS）；天级补 T00:00:00。

    支持 datetime 实例、Unix 时间戳（秒/毫秒）、Excel 日期序列号、
    字符串（含中文日期/分钟级/时区后缀/纯数字时间戳）。
    失败返回 None。时区一律按东八区口径存 naive（契约 §6.1）。
    """
    if isinstance(raw, datetime):
        return raw.strftime("%Y-%m-%dT%H:%M:%S")
    if isinstance(raw, (int, float)) and not isinstance(raw, bool):
        return _epoch_to_iso(float(raw))
    if not isinstance(raw, str):
        return None
    s = raw.strip()
    if not s:
        return None
    if s.isdigit() and len(s) >= 10:  # 字符串形态的时间戳（"1768795361000"）
        return _epoch_to_iso(float(s))
    s = s.replace("Z", "").split("+")[0].strip()
    for fmt in _DT_FORMATS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%dT%H:%M:%S")
        except ValueError:
            continue
    try:  # 兜底：ISO 变体（含时区偏移、缺秒等）fromisoformat 大多能吃下
        return datetime.fromisoformat(s).strftime("%Y-%m-%dT%H:%M:%S")
    except ValueError:
        pass
    for fmt in _DAY_FORMATS:
        try:
            return datetime.strptime(s, fmt).strftime("%Y-%m-%dT00:00:00")
        except ValueError:
            continue
    return None


def node_id_of(url: str) -> str:
    return hashlib.md5(url.encode("utf-8")).hexdigest()


def _domain_of(url: str) -> str:
    host = urlsplit(url).netloc or ""
    return host[4:] if host.startswith("www.") else host


def _synth_url(title: str, content: str) -> str:
    """无 url 记录的内容指纹伪 url：同内容重复导入天然去重（consumer 侧渠道过滤不命中）。"""
    fp = hashlib.md5((title + "\n" + content[:500]).encode("utf-8")).hexdigest()
    return f"nohost://{fp}"


def extract_record(rec: dict) -> dict:
    """规范化 record → 阶段1产物 dict（node_id/url/title/content/published_date/source）。"""
    html = str(rec.get("html") or "").strip()
    raw_content = str(rec.get("content") or "").strip()

    # ---- 正文 ----
    content_html = raw_content if _HTML_TAG_RE.search(raw_content) else ""
    if html or content_html:
        raw = trafilatura.extract(
            html or content_html,
            output_format="json",
            with_metadata=True,      # 否则 JSON 里只有 text，没有 title/date
            include_comments=False,
        )
        if raw:
            try:
                meta = json.loads(raw)
            except json.JSONDecodeError:
                raise ExtractError("extract_output_invalid")
            content = (meta.get("text") or "").strip()
            page_title = (meta.get("title") or "").strip()
            page_date = meta.get("date")
        elif content_html and not html:
            # 结构太弱 trafilatura 认不出（如裸 <p> 片段）：剥标签兜底，不丢内容
            content = _strip_tags(content_html)
            page_title, page_date = "", None
        else:
            raise ExtractError("extract_failed")
    elif raw_content:
        content = raw_content
        page_title = ""
        page_date = None
    else:
        raise ExtractError("missing_content")

    # 契约 §4.1 建议正文 500~2000 字；<300 基本必是抽取失败（如 OCR 只抓到
    # 微信导语）或纯垃圾页，按垃圾丢弃不进死信、不花 LLM
    if len(content) < 300:
        raise ExtractError("content_too_short")

    # ---- 标题 ----
    title = (str(rec.get("title") or "").strip() or page_title
             or content.splitlines()[0][:60])
    title = _TITLE_NOISE.sub("", title).strip() or content.splitlines()[0][:60]

    # ---- url（无 url 时用内容指纹合成，保证 node_id 稳定可去重）----
    url = str(rec.get("url") or "").strip() or _synth_url(title, content)

    # ---- published_date：结构化字段优先（爬取元数据 > 页面内时间 > 采集时间）----
    published = (
        normalize_datetime(rec.get("publish_time"))
        or normalize_datetime(page_date)
        or normalize_datetime(rec.get("collect_time"))
    )
    if not published:
        # 无发布时间的记录无法被消费端时间过滤命中（契约 §6.3），宁可进死信人工处理
        raise ExtractError("no_publish_date")

    # ---- source ----
    source = str(rec.get("source") or "").strip() or _domain_of(url)

    # ---- region 透传（结构化字段优先于 LLM 抽取，与 published_date 同一原则；
    #      值是否合法省级标准名由阶段2校验，非法则忽略走 LLM）----
    region = str(rec.get("region") or "").strip() or None

    return {
        "node_id": node_id_of(url),
        "url": url[:2000],
        "title": title[:500],
        "content": content[:SETTINGS.content_max_chars],
        "published_date": published,
        "source": source[:250],
        "region": region,
    }
