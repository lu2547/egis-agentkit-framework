"""在 NAS 挂载目录保存技能包，数据库只引用完整包的相对路径和摘要。

核心能力
- write_archive：按原始包 SHA256 定位，完整写入后原子发布，同包复用。
- read_archive：校验固定路径、大小及包摘要，不读取数据库二进制回退。
- from_settings：由 Nacos 根路径与归档限额构造文件存储。
"""

import hashlib
import os
import re
from pathlib import Path

from egis_agentkit.errors import Conflict, NotFound
from egis_agentkit.storage._files import FileStorage

SHA256_PATTERN = re.compile(r"[a-f0-9]{64}")
ARCHIVE_SUFFIX = ".zip"


class SkillArchiveStorage(FileStorage):
    """复用受管文件 IO，通过同目录临时文件保证共享目录中的读者只见完整包。"""

    @classmethod
    def from_settings(cls, settings, *, config_provider=None):
        """必须明确配置 NAS 挂载绝对目录，不隐式回退到本地工作区。"""
        if settings.storage.root is None:
            raise ValueError("必须配置 agentkit.skill.storage.root 的 NAS 挂载目录")
        root = Path(settings.storage.root)
        return cls(
            root, max_bytes=settings.archive.max_bytes, config_provider=config_provider
        )

    @property
    def max_bytes(self):
        """归档限额可热更新，根路径切换仍要求重启。"""
        return (
            self.config.snapshot.skill.archive.max_bytes
            if self.config
            else self._max_bytes
        )

    def write_archive(self, content: bytes) -> dict:
        """写完整包再提交元数据；同名包损坏时明确失败，不静默覆盖。"""
        if len(content) > self.max_bytes:
            raise Conflict("技能包超过存储大小限制")
        digest = hashlib.sha256(content).hexdigest()
        location = {
            "archive_path": digest + ARCHIVE_SUFFIX,
            "archive_sha256": digest,
            "archive_size_bytes": len(content),
        }
        # 1. 已存在的同包必须完整且校验通过，不能信任仅存在的文件名。
        try:
            self.read_archive(location)
        except NotFound:
            pass
        else:
            return location
        # 2. FileStorage 使用同目录临时文件、fsync 和原子替换，支持并发同包写入。
        self._write_root(
            self.root,
            location["archive_path"],
            content,
            overwrite=True,
            max_bytes=self.max_bytes,
        )
        descriptor = self._directory_fd(self.root, [])
        try:
            os.fsync(descriptor)
        finally:
            os.close(descriptor)
        # 3. 记录入库之前确认持久包可读，失败时调用方不发布资源版本。
        self.read_archive(location)
        return location

    def read_archive(self, location: dict) -> bytes:
        """只读取包摘要确定的固定文件，拒绝越界、符号链接及损坏内容。"""
        digest = location.get("archive_sha256")
        size = location.get("archive_size_bytes")
        if not isinstance(digest, str) or not SHA256_PATTERN.fullmatch(digest):
            raise Conflict("技能包缺少有效存储摘要，请先升级存储")
        if location.get("archive_path") != digest + ARCHIVE_SUFFIX:
            raise Conflict("技能包路径与存储摘要不一致")
        if type(size) is not int or size < 0 or size > self.max_bytes:
            raise Conflict("技能包大小元数据无效或超过限制")
        content, _ = self._read_root(
            self.root, location["archive_path"], max_bytes=self.max_bytes
        )
        if len(content) != size or hashlib.sha256(content).hexdigest() != digest:
            raise Conflict("技能包完整性校验失败")
        return content
