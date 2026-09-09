"""MinIO 批次源（可选，Minio_Endpoint 为空 = 纯本地模式）。

约定：bucket 的 Minio_Prefix（默认 raw/）下一级前缀 = 一个批次，如 raw/20260826_0900/**。
批次下载到 running/<batch>/inbox/（子路径扁平化：a/b.json → a__b.json）。
批次完成后由 batch.finish_batches 删除远端前缀（可通过 Minio_Delete_On_Done=false 关闭，
保留原始数据以便将来换 embedding 模型/重建 collection 时离线重跑）。
"""
import logging
import os

from common.config import SETTINGS
from common.utils import ensure_dirs

log = logging.getLogger("minio")

_client = None


def enabled() -> bool:
    return bool(SETTINGS.minio_endpoint)


def delete_enabled() -> bool:
    return enabled() and SETTINGS.minio_delete_on_done


def _get_client():
    global _client
    if _client is None:
        from minio import Minio
        _client = Minio(
            SETTINGS.minio_endpoint,
            access_key=SETTINGS.minio_access_key,
            secret_key=SETTINGS.minio_secret_key,
            secure=SETTINGS.minio_secure,
        )
    return _client


def ensure_bucket() -> bool:
    """确保 bucket 存在（落盘器启动时调用）。失败返回 False。"""
    try:
        client = _get_client()
        if not client.bucket_exists(SETTINGS.minio_bucket):
            client.make_bucket(SETTINGS.minio_bucket)
            log.info("创建 bucket %s", SETTINGS.minio_bucket)
        return True
    except Exception as e:
        log.error("MinIO bucket 检查失败: %s", e)
        return False


def put_bytes(object_name: str, data: bytes) -> None:
    """上传字节到 bucket（同名覆盖，幂等）。失败抛异常由调用方处理。"""
    import io
    client = _get_client()
    client.put_object(SETTINGS.minio_bucket, object_name, io.BytesIO(data),
                      length=len(data), content_type="application/json")


def fetch_batches() -> int:
    """列出远端批次前缀，本地 running/ 尚无同名批次的下载之。返回新拉取批次数。"""
    if not enabled():
        return 0
    from common.batch import safe_batch_id
    try:
        client = _get_client()
        root = SETTINGS.minio_prefix.strip("/") + "/"
        batches = set()
        for obj in client.list_objects(SETTINGS.minio_bucket, prefix=root, recursive=False):
            rel = obj.object_name[len(root):]
            head = rel.split("/", 1)[0]
            if head:
                batches.add(head)

        n = 0
        for raw_batch in sorted(batches):
            batch = safe_batch_id(raw_batch)
            dst = os.path.join(SETTINGS.running_dir, batch, "inbox")
            if os.path.exists(dst):
                continue  # 处理中；已完成并清理过远端的批次不会再出现在远端列表里
            ensure_dirs(dst)
            bprefix = root + raw_batch + "/"
            count = 0
            for obj in client.list_objects(SETTINGS.minio_bucket, prefix=bprefix, recursive=True):
                if obj.is_dir:
                    continue
                flat = obj.object_name[len(bprefix):].replace("/", "__")
                client.fget_object(SETTINGS.minio_bucket, obj.object_name,
                                   os.path.join(dst, flat))
                count += 1
            log.info("拉取 MinIO 批次 %s（%d 个对象）", batch, count)
            n += 1
        return n
    except Exception as e:
        log.error("MinIO 拉取失败: %s", e)
        return 0


def delete_batch(batch_id: str) -> bool:
    """删除远端批次前缀。成功（或无需删除）返回 True；失败返回 False（调用方保留本地目录）。"""
    if not delete_enabled():
        return True
    try:
        client = _get_client()
        root = SETTINGS.minio_prefix.strip("/") + "/"
        prefix = root + batch_id + "/"
        names = [o.object_name for o in
                 client.list_objects(SETTINGS.minio_bucket, prefix=prefix, recursive=True)]
        for name in names:
            client.remove_object(SETTINGS.minio_bucket, name)
        log.info("MinIO 批次 %s 远端已删（%d 对象）", batch_id, len(names))
        return True
    except Exception as e:
        log.error("MinIO 删除批次 %s 失败: %s", batch_id, e)
        return False
