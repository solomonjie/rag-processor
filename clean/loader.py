"""阶段1（clean）输入读取：多格式文件 → 规范化 record 列表。

支持格式（扩展名决定解析方式）：
    .json    单对象或 JSON 数组
    .jsonl   每行一个对象
    .xlsx    每行一条记录，列名自动映射（中英文别名见 _ALIASES）
    .txt     单文件单篇：首个非空行为标题，其余为正文（url 用 txt://文件名）
    .html    单文件单页：整文件作为 html 走 trafilatura（url 用 file://文件名）

record 的规范字段：url / html / title / content / publish_time / source / collect_time。
其中 html 与 content 二选一（html 走抽取，content 视为已抽好的纯文本直通）。
"""
import json
import logging
import math
import os
from datetime import datetime

import pandas as pd

log = logging.getLogger("clean.loader")


class LoaderError(Exception):
    """整个文件无法解析（格式不支持 / 编码损坏 / Excel 结构错误）。"""


# 列名别名表：规范字段 → 候选列名（按优先级排序，先匹配到的优先；
# 如同时有 fullContent 与 content 时取全量内容）。原始列名不区分大小写。
_ALIASES = {
    "url": ("url", "link", "链接", "链接地址", "网址", "文章链接", "url地址",
            "infourl", "originurl"),
    "html": ("html", "raw_html", "网页内容", "页面内容", "网页源码", "源码"),
    "title": ("title", "标题", "文章标题", "题名"),
    "content": ("fullcontent", "全量内容", "content", "正文", "内容", "text",
                "文本", "正文内容", "summary", "汇总简介"),
    "publish_time": ("publish_time", "publishtime", "publish_date", "published_at",
                     "originpublishtime", "date", "发布时间", "发布日期", "时间", "日期"),
    "source": ("source", "来源", "来源网站", "站点", "网站", "webname", "sitename"),
    "collect_time": ("collect_time", "collecttime", "crawl_time", "inserttime",
                     "采集时间", "抓取时间", "爬取时间"),
    "region": ("region", "地区", "发生地"),
}

SUPPORTED_EXT = ("json", "jsonl", "xlsx", "txt", "html", "htm")
INPUT_PATTERNS = ["*.json", "*.jsonl", "*.xlsx", "*.txt", "*.html", "*.htm"]


def _read_text(path: str) -> str:
    """txt/html 读取：先 UTF-8，失败退 GBK（中文爬取产物常见），再不行替换坏字符。"""
    for enc in ("utf-8", "gbk"):
        try:
            with open(path, "r", encoding=enc) as f:
                return f.read()
        except UnicodeDecodeError:
            continue
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        return f.read()


def _mtime(path: str) -> datetime:
    return datetime.fromtimestamp(os.path.getmtime(path))


def _canon_row(row: dict) -> dict:
    """一行原始数据 → 规范字段 dict；每个规范字段按别名优先级取第一个非空值。"""
    lowered = {str(k).strip().lower(): v for k, v in row.items()}
    out = {}
    for canon, aliases in _ALIASES.items():
        for a in aliases:
            v = lowered.get(a)
            if v is None:
                continue
            if isinstance(v, float) and math.isnan(v):
                continue
            if isinstance(v, str) and not v.strip():
                continue
            out[canon] = v
            break
    return out


def _load_json(path: str) -> tuple:
    text = _read_text(path).strip()
    if not text:
        return [], 0
    try:
        data = json.loads(text)
    except json.JSONDecodeError:
        raise LoaderError("invalid_json")
    # 注意：记录级格式不做 mtime 兜底——爬取 JSON 没有 collect_time 就是真缺，
    # 时间链路全断时走死信人工审计（mtime 与真实采集时间可能差很远）
    if isinstance(data, list):
        records, bad = [], 0
        for r in data:
            if isinstance(r, dict):
                records.append(_canon_row(r))
            else:
                bad += 1
        return records, bad
    if isinstance(data, dict):
        return [_canon_row(data)], 0
    raise LoaderError("invalid_json_root")


def _load_jsonl(path: str) -> tuple:
    records, bad = [], 0
    for line in _read_text(path).splitlines():
        line = line.strip()
        if not line:
            continue
        try:
            rec = _canon_row(json.loads(line))
        except json.JSONDecodeError:
            bad += 1
            continue
        records.append(rec)
    return records, bad


def _load_excel(path: str) -> tuple:
    try:
        df = pd.read_excel(path, dtype=object)
    except Exception as e:
        raise LoaderError(f"excel_read_failed: {e}")
    records, bad = [], 0
    for row in df.to_dict("records"):
        rec = _canon_row(row)
        if not (rec.get("html") or rec.get("content")):
            bad += 1  # 既无网页源码也无正文的行没有价值
            continue
        records.append(rec)
    return records, bad


def _load_txt(path: str) -> tuple:
    lines = [l.strip() for l in _read_text(path).splitlines() if l.strip()]
    if not lines:
        return [], 0
    title = lines[0]
    content = "\n".join(lines[1:]) or title
    return [{
        "url": f"txt://{os.path.basename(path)}",   # 稳定伪 url：同名文件重导入可去重
        "title": title,
        "content": content,
        "collect_time": _mtime(path),
    }], 0


def _load_html(path: str) -> tuple:
    return [{
        "url": f"file://{os.path.basename(path)}",
        "html": _read_text(path),
        "collect_time": _mtime(path),
    }], 0


def load_file(path: str) -> tuple:
    """任意支持格式 → (records, bad_rows)。文件级失败抛 LoaderError。"""
    ext = os.path.splitext(path)[1].lower().lstrip(".")
    if ext == "json":
        return _load_json(path)
    if ext == "jsonl":
        return _load_jsonl(path)
    if ext == "xlsx":
        return _load_excel(path)
    if ext == "txt":
        return _load_txt(path)
    if ext in ("html", "htm"):
        return _load_html(path)
    raise LoaderError(f"unsupported:{ext}")
