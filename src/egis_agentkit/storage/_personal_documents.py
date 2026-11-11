"""保存个人助理文档，按用户锁定文件读写。"""

import os
from contextlib import asynccontextmanager
from uuid import uuid4

from sqlalchemy import text

DOCUMENT_FILES = {"agent_md": "agent.md", "soul_md": "soul.md", "user_md": "user.md"}


class PersonalDocumentStorage:
    """维护三份 Markdown 文件和跨进程写入锁。"""

    def __init__(self, storage, files):
        self.engine, self.files = storage.engine, files

    @asynccontextmanager
    async def _locked_root(self, user_id):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": "preferences:" + user_id},
            )
            yield self.files.user_root(user_id) / "preferences"

    async def read(self, user_id):
        async with self._locked_root(user_id) as root:
            return {
                key: (root / name).read_text("utf-8")
                for key, name in DOCUMENT_FILES.items()
                if (root / name).is_file()
            }

    async def save(self, user_id, documents):
        async with self._locked_root(user_id) as root:
            root.mkdir(parents=True, exist_ok=True)
            staged = []
            try:
                # 先写完所有临时文件，再替换各份文档。
                for key, name in DOCUMENT_FILES.items():
                    target = root / name
                    temporary = target.with_suffix(f".{uuid4().hex}.tmp")
                    staged.append((temporary, target))
                    with temporary.open("x", encoding="utf-8") as stream:
                        stream.write(documents[key])
                        stream.flush()
                        os.fsync(stream.fileno())
                for temporary, target in staged:
                    os.replace(temporary, target)
            finally:
                for temporary, _ in staged:
                    temporary.unlink(missing_ok=True)
