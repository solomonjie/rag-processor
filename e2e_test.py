"""流模式端到端测试：用 data/pipeline.xlsx 的真实样例行当用例 → 推 RocketMQ → 验收线上 Milvus。

数据源是上游协议的完整导出（93 列 camelCase、publishTime 毫秒时间戳、部分行无 content），
逐行原样发送（仅替换 url 为每轮唯一的 e2e url，保证可重跑、可清理）。因此覆盖真实场景：
  - 38 行有 content → 走 LLM 判定，入库或按广告/非新闻/不相关丢弃
  - 24 行无 content/fullContent/html → clean 阶段丢弃（无正文）
  - 附加场景 D：settle 后重发第 1 行的 url → 该 url 仍只有 1 行（跨波去重幂等）
  - 附加场景 E：发一条非法 JSON → 死信目录出现 invalid_json

验收规则：入库行必须满足字段契约（id=md5(url)、published_date 定长 ISO、tags 非空、
region 非空）；未入库行列为"已丢弃"（正常结局），但**一行都没入库 = FAIL**（链路断了）。

前置（rocketmq binding 仅 Linux——本脚本与 worker 都在容器/WSL 里跑）：
  1. worker 已在流模式运行（同 topic/group）
  2. 可达 RocketMQ（--namesrv）与 Milvus（.env 的 Milvus_Server_URL/TOKEN，线上库）

用法：
  python e2e_test.py                     # 全量 62 行
  python e2e_test.py --limit 8           # 快速冒烟（前 8 行）
  python e2e_test.py --skip-send         # 不发送，复验上一轮（url 记录在 data/_e2e_last.json）
  python e2e_test.py --cleanup           # 清理线上测试行（独立执行）
"""
import argparse
import glob
import hashlib
import json
import os
import re
import sys
import time
from datetime import datetime, timezone

from openpyxl import load_workbook

from common.config import SETTINGS
from enrich.run import REGIONS  # 省级标准名词表（region 非空时校验）

COLL = "product_knowledge_base"  # 事故规约：测试脚本 collection 一律硬编码
PREFIX = "http://example.com/e2e-"
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}$")
HTML_RE = re.compile(r"</?(?:div|p|br|span|img|section|article)\b", re.I)
BASE = os.path.dirname(os.path.abspath(__file__))  # 相对脚本定位，不依赖 cwd
LAST_RUN = os.path.join(BASE, "data", "_e2e_last.json")
STABLE_POLLS = 9   # 连续 N 次轮询行数不变 = settle（窗口要长于一波 LLM 处理时间）
POLL_SECS = 5
SETTLE_FLOOR = 90  # 最短观察期，防止把"处理慢"误判为"处理完"


def load_cases(path: str) -> list:
    """xlsx 逐行 → dict（原样保留协议字段；datetime→毫秒时间戳，整型 float→int）。"""
    ws = load_workbook(path, read_only=True).active
    rows = ws.iter_rows(values_only=True)
    header = [str(h) for h in next(rows)]
    cases = []
    for r in rows:
        d = {}
        for h, v in zip(header, r):
            if isinstance(v, datetime):
                v = int(v.replace(tzinfo=timezone.utc).timestamp() * 1000)
            elif isinstance(v, float) and v.is_integer():
                v = int(v)
            d[h] = v
        if any(v is not None for v in d.values()):
            cases.append(d)
    return cases


def _enc(o):
    return int(o.replace(tzinfo=timezone.utc).timestamp() * 1000) if isinstance(o, datetime) else str(o)


def send(namesrv: str, msgs: list) -> None:
    from rocketmq.client import Message, Producer
    p = Producer("rag-e2e-producer")
    p.set_name_server_address(namesrv)
    p.start()
    try:
        for tag, body in msgs:
            m = Message(SETTINGS.rocketmq_topic)
            m.set_body(body if isinstance(body, bytes)
                       else json.dumps(body, ensure_ascii=False, default=_enc).encode("utf-8"))
            r = p.send_sync(m)
            print(f"  sent {tag}: status={r.status}")
    finally:
        p.shutdown()


def query_urls(client, urls: list) -> list:
    expr = "url in [" + ",".join(json.dumps(u) for u in urls) + "]"
    return client.query(COLL, filter=expr,
                        output_fields=["id", "url", "region", "published_date", "tags", "text"],
                        limit=len(urls))


def wait_settled(client, urls: list, timeout: int) -> list:
    """轮询到入库行数稳定（连续 STABLE_POLLS 次不变且已等满 1 分钟）或超时。"""
    rows, stable, floor = [], 0, time.time() + SETTLE_FLOOR
    deadline = time.time() + timeout
    while time.time() < deadline:
        time.sleep(POLL_SECS)
        cur = query_urls(client, urls)
        stable = stable + 1 if len(cur) == len(rows) and rows else 0
        rows = cur
        print(f"  已入库 {len(rows)}/{len(urls)}，稳定计数 {stable}/{STABLE_POLLS}")
        if stable >= STABLE_POLLS and time.time() >= floor:
            break
    return rows


def scan_dead_letters(start: float) -> list:
    """start 之后新增的死信条目 [(stage, reason)]。"""
    out = []
    for f in glob.glob(os.path.join(SETTINGS.dead_letter_dir, "*.jsonl")):
        if os.path.getmtime(f) < start - 5:
            continue
        for line in open(f, encoding="utf-8"):
            try:
                e = json.loads(line)
                out.append((str(e.get("stage", "?")), str(e.get("reason", "?"))[:40]))
            except ValueError:
                pass
    return out


def main() -> None:
    ap = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    ap.add_argument("--file", default=os.path.join(BASE, "data", "pipeline.xlsx"))
    ap.add_argument("--namesrv", default="localhost:19876")
    ap.add_argument("--timeout", type=int, default=420, help="settle 轮询超时秒数")
    ap.add_argument("--limit", type=int, default=0, help="只发前 N 行（0=全部）")
    ap.add_argument("--skip-send", action="store_true", help="不发送，复验上一轮 url")
    ap.add_argument("--cleanup", action="store_true", help="删除线上测试行（独立执行）")
    args = ap.parse_args()

    from pymilvus import MilvusClient
    client = MilvusClient(uri=SETTINGS.milvus_url, token=SETTINGS.milvus_token)

    if args.cleanup:
        expr = 'url like "http://example.com/e2e-%" or url like "http://example.com/stream-e2e-%"'
        client.delete(COLL, filter=expr)
        time.sleep(3)  # Zilliz 删除异步，稍候再验证
        left = client.query(COLL, filter=expr, output_fields=["url"], limit=100)
        print(f"清理完成，残留 {len(left)} 行" + ("：" + ",".join(r["url"] for r in left) if left else ""))
        return

    start = time.time()
    if args.skip_send:
        last = json.load(open(LAST_RUN, encoding="utf-8"))
        ts, urls, titles = last["ts"], last["urls"], last["titles"]
    else:
        cases = load_cases(args.file)
        if args.limit:
            cases = cases[:args.limit]
        ts = time.strftime("%Y%m%d%H%M%S")
        urls = [f"{PREFIX}{ts}-{i}" for i in range(len(cases))]
        titles = [str(c.get("title") or "")[:24] for c in cases]
        json.dump({"ts": ts, "urls": urls, "titles": titles},
                  open(LAST_RUN, "w", encoding="utf-8"))
        msgs = [(f"row{i}", {**c, "url": u}) for i, (c, u) in enumerate(zip(cases, urls))]
        msgs.append(("bad-json", b"{not-json"))
        print(f"[1/4] 发送 {len(cases)} 行（来自 {args.file}）+ 1 条坏 JSON → {args.namesrv}")
        send(args.namesrv, msgs)

    print("[2/4] 等待处理 settle（入库行数稳定）...")
    rows = wait_settled(client, urls, args.timeout)
    found = {r["url"]: r for r in rows}

    # ---- 验收入库行契约 ----
    bad = []
    for u, r in found.items():
        errs = []
        if r["id"] != hashlib.md5(u.encode()).hexdigest():
            errs.append("id≠md5(url)")
        if not DATE_RE.match(r["published_date"] or ""):
            errs.append(f"date={r['published_date']!r}")
        if not r["tags"]:
            errs.append("tags空")
        if HTML_RE.search(r["text"] or ""):
            errs.append("text含HTML标签")
        if (r["region"] or "").strip() and r["region"] not in REGIONS:
            errs.append(f"region={r['region']!r} 非省级标准名")
        # region 允许为空（契约 §C：region 当前无人读取，仅未来预留；空=判不出）
        if errs:
            bad.append((u, "; ".join(errs)))
    print(f"[3/4] 入库 {len(found)}/{len(urls)} 行；契约违规 {len(bad)}")
    for u, e in bad:
        print(f"  FAIL {u}: {e}")

    absent = [(u, t) for u, t in zip(urls, titles) if u not in found]
    print(f"  未入库（判定丢弃/无正文）{len(absent)} 行：")
    for u, t in absent[:10]:
        print(f"    - {t}")
    if len(absent) > 10:
        print(f"    ... 共 {len(absent)} 行")

    # ---- 场景 D：重发第 1 行 url，跨波去重幂等 ----
    if not args.skip_send:
        print("[4/4] 重发第 1 行 url（场景 D）+ 坏 JSON 死信核验（场景 E）")
        cases0 = load_cases(args.file)[:1]
        send(args.namesrv, [("dup", {**cases0[0], "url": urls[0]})])
        time.sleep(15)
    dup = query_urls(client, [urls[0]])

    deads = scan_dead_letters(start)
    has_badjson = any(r.startswith("invalid_json") for _, r in deads)

    results = [
        (len(found) > 0, f"有入库行（{len(found)}/{len(urls)}；全 0 = 链路断了）"),
        (not bad, f"入库行字段契约（id/date/tags/region）违规 {len(bad)}"),
        (len(dup) == 1, f"重复 url 幂等：{len(dup)} 行（应 1）"),
        (args.skip_send or has_badjson,
         f"坏 JSON 死信（本轮死信共 {len(deads)} 条：{deads[:5]}）"),
    ]
    print("=" * 8, "结果", "=" * 8)
    failed = 0
    for okv, detail in results:
        print(f"  {'PASS' if okv else 'FAIL'}  {detail}")
        failed += 0 if okv else 1
    sys.exit(1 if failed else 0)


if __name__ == "__main__":
    main()
