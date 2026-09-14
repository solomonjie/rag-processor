"""跨阶段共用的小工具：目录扫描、_done 归档、jsonl 读写、死信落盘、批次名清洗。"""
import glob
import json
import logging
import os
import re
import shutil
import time

# 每个阶段目录下的归档子目录：文件被下一阶段消费后移入，目录本身保持"待处理队列"语义
DONE = "_done"

log = logging.getLogger("utils")


def now_stamp() -> str:
    return time.strftime("%Y%m%dT%H%M%S")


def safe_batch_id(name: str) -> str:
    """批次名清洗：非法字符替换为 _，截断到 120 字符（作目录名/对象前缀都安全）。"""
    return re.sub(r"[^\w.-]", "_", name)[:120] or "batch"


def ensure_dirs(*dirs: str) -> None:
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def iter_input_files(directory: str, patterns: list) -> list:
    """按扩展名模式列出目录下待处理文件（_done/ 子目录天然不会被匹配到）。"""
    files = []
    for p in patterns:
        files += glob.glob(os.path.join(directory, p))
    return sorted(set(files))


def move_unique(src: str, dest_dir: str) -> str:
    """移动文件；目标重名时追加序号，绝不覆盖（归档可追溯）。"""
    os.makedirs(dest_dir, exist_ok=True)
    base = os.path.basename(src)
    dest = os.path.join(dest_dir, base)
    if not os.path.exists(dest):
        shutil.move(src, dest)
        return dest
    stem, ext = os.path.splitext(base)
    for i in range(1, 10000):
        dest = os.path.join(dest_dir, f"{stem}~{i}{ext}")
        if not os.path.exists(dest):
            shutil.move(src, dest)
            return dest
    raise RuntimeError(f"无法归档 {src}（重名过多）")


def move_done(path: str) -> str:
    """文件被本阶段消费完毕 → 移入同目录 _done/。"""
    return move_unique(path, os.path.join(os.path.dirname(path), DONE))


def write_jsonl(path: str, rows: list) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        for r in rows:
            f.write(json.dumps(r, ensure_ascii=False) + "\n")


def read_jsonl(path: str) -> tuple:
    """读 jsonl，返回 (rows, 坏行数)。坏行跳过计数（阶段产物是我们自己写的，正常为 0）。"""
    rows, bad = [], 0
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                rows.append(json.loads(line))
            except json.JSONDecodeError:
                bad += 1
    return rows, bad


def dead_letter(dead_dir: str, stage: str, src_name: str, entries: list) -> str:
    """阶段死信落盘：data/dead_letter/{时间}_{阶段}_{来源}.jsonl，每行含 reason + 原始数据。

    死信是唯一需要落盘留痕的产物（不随批次/波清理）。生产部署给 data/dead_letter/
    挂卷即可持久——这是流模式容器对磁盘的唯一要求。
    """
    if not entries:
        return ""
    base = os.path.splitext(os.path.basename(src_name))[0]
    path = os.path.join(dead_dir, f"{now_stamp()}_{stage}_{base}.jsonl")
    write_jsonl(path, entries)
    return path
