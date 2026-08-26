"""批次管理：目录即队列的"批次文件夹"版。

一个批次 = data/running/<batch_id>/ 下的三个阶段目录：
    inbox/  → 1_cleaned/ → 2_enriched/ → (index 入 Milvus)

状态机（文件位置即状态，无额外成功标记）：
    文件在阶段目录   = 待处理
    文件在 _done/    = 该阶段已消费成功
    dead_letter/     = 终态，需人工介入（不随批次清理删除）
    批次目录被删除   = 三阶段全部消费完毕（enrich/index 失败的文件原地保留，批次不完成）

批次来源（adopt_arrivals 统一收编到 running/）：
    - 本地 data/inbox/ 下的子目录（目录名=batch_id）或散文件（文件名去扩展名=batch_id）
    - MinIO raw/<batch_id>/ 前缀（配置了 Minio_Endpoint 时，见 minio_source）
"""
import logging
import os
import re
import shutil

from clean.loader import INPUT_PATTERNS
from common import minio_source
from common.config import SETTINGS
from common.utils import ensure_dirs, iter_input_files, move_unique

log = logging.getLogger("batch")

INBOX = "inbox"
CLEANED = "1_cleaned"
ENRICHED = "2_enriched"

# 批次目录内，判定"批次完成"需要检查的（目录, 待处理文件模式）
_STAGE_SLOTS = ((INBOX, INPUT_PATTERNS), (CLEANED, ["*.jsonl"]), (ENRICHED, ["*.jsonl"]))


def safe_batch_id(name: str) -> str:
    return re.sub(r"[^\w.-]", "_", name)[:120] or "batch"


def stage_dir(batch_dir: str, stage: str) -> str:
    return os.path.join(batch_dir, stage)


def adopt_arrivals() -> int:
    """收编新批次：本地 inbox + MinIO 远端 → running/<batch>/inbox/。返回收编批次数。"""
    ensure_dirs(SETTINGS.inbox_dir, SETTINGS.running_dir)
    n = minio_source.fetch_batches()  # MinIO 拉到 running/<batch>/inbox/
    for entry in sorted(os.listdir(SETTINGS.inbox_dir)):
        src = os.path.join(SETTINGS.inbox_dir, entry)
        batch = safe_batch_id(entry if os.path.isdir(src) else os.path.splitext(entry)[0])
        dst = os.path.join(SETTINGS.running_dir, batch, INBOX)
        if os.path.exists(dst):
            continue  # 同名批次处理中/待清理，先不动（完成后下一轮重新收编）
        ensure_dirs(dst)
        if os.path.isdir(src):
            for f in os.listdir(src):
                move_unique(os.path.join(src, f), dst)
            os.rmdir(src)
        else:
            move_unique(src, dst)
        log.info("收编本地批次 %s", batch)
        n += 1
    return n


def iter_batches() -> list:
    """枚举 running/ 下所有进行中的批次目录。"""
    d = SETTINGS.running_dir
    if not os.path.isdir(d):
        return []
    return [os.path.join(d, b) for b in sorted(os.listdir(d))
            if os.path.isdir(os.path.join(d, b))]


def batch_complete(batch_dir: str) -> bool:
    """三阶段目录都没有待处理文件 = 批次完成（_done 归档不算待处理）。"""
    return all(
        not iter_input_files(os.path.join(batch_dir, sub), pats)
        for sub, pats in _STAGE_SLOTS
    )


def finish_batches() -> int:
    """清理完成批次：先删 MinIO 前缀、成功后再删本地目录（崩溃安全：
    远端删失败则本地保留 → 下轮重试删除，不会造成远端残留批次被重新拉取重跑）。
    死信不随批次删除。返回清理批次数。"""
    cleaned = 0
    for bdir in iter_batches():
        if not batch_complete(bdir):
            continue
        batch = os.path.basename(bdir)
        if not minio_source.delete_batch(batch):
            log.error("批次 %s 远端删除失败，本地目录保留待重试", batch)
            continue
        shutil.rmtree(bdir, ignore_errors=True)
        cleaned += 1
        log.info("批次 %s 完成并清理（本地目录已删，%s）", batch,
                 "远端前缀已删" if minio_source.delete_enabled() else "纯本地模式")
    return cleaned
