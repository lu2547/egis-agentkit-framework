"""可删除的 PDF 派生文件缓存；命中前的项目权限和原件校验由文件服务负责。"""

import os
import tempfile
import time
from contextlib import asynccontextmanager
from pathlib import Path

from sqlalchemy import text


class PreviewCacheStorage:
    def __init__(self, engine=None):
        self.engine = engine

    @asynccontextmanager
    async def guard(self, key):
        if self.engine is None:
            yield
            return
        async with self.engine.begin() as conn:
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": "agentkit-preview:" + key},
            )
            yield

    @staticmethod
    def _root(settings):
        root = Path(settings.root)
        if root.is_symlink():
            raise OSError("Preview cache root cannot be a symbolic link")
        root.mkdir(parents=True, exist_ok=True, mode=0o700)
        return root

    def get_preview(self, key, settings, max_bytes):
        target = self._root(settings) / (key + ".pdf")
        try:
            if (
                target.is_symlink()
                or time.time() - target.stat().st_mtime > settings.ttl_seconds
            ):
                return None
            with target.open("rb") as source:
                content = source.read(max_bytes + 1)
            if len(content) > max_bytes or not content.startswith(b"%PDF-"):
                target.unlink(missing_ok=True)
                return None
            return content
        except FileNotFoundError:
            return None

    def save_preview(self, key, content, settings):
        if len(content) > settings.max_total_bytes:
            return
        root = self._root(settings)
        target = root / (key + ".pdf")
        name = None
        try:
            with tempfile.NamedTemporaryFile(
                dir=root, prefix=".preview-", delete=False
            ) as handle:
                name = handle.name
                handle.write(content)
            os.replace(name, target)
        finally:
            if name:
                Path(name).unlink(missing_ok=True)
        entries = []
        for path in root.glob("*.pdf"):
            try:
                stat = path.stat()
                if not path.is_symlink():
                    entries.append((stat.st_mtime, path, stat.st_size))
            except FileNotFoundError:
                continue
        entries.sort(reverse=True)
        total = 0
        for index, (modified, path, size) in enumerate(entries):
            total += size
            if (
                index >= settings.max_entries
                or total > settings.max_total_bytes
                or time.time() - modified > settings.ttl_seconds
            ):
                path.unlink(missing_ok=True)
