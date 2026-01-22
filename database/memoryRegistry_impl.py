from typing import List, Set
from database.interfaces import BaseStatusRegistry

class MemoryStatusRegistry(BaseStatusRegistry):
    def __init__(self):
        self._completed_files = {}  # {file_name: file_hash}
        self._temp_chunks = {}      # {file_name: set(chunk_ids)}

    def is_file_processed(self, file_name: str) -> bool:
        return file_name in self._completed_files

    def get_processed_chunks(self, file_name: str) -> Set[str]:
        return self._temp_chunks.get(file_name, set())

    def mark_chunks_processed(self, file_name: str, chunk_ids: List[str]):
        if file_name not in self._temp_chunks:
            self._temp_chunks[file_name] = set()
        self._temp_chunks[file_name].update(chunk_ids)

    def mark_file_complete(self, file_name: str, file_hash: str):
        self._completed_files[file_name] = file_hash
        self._temp_chunks.pop(file_name, None) # 清理内存，仅保留文件索引