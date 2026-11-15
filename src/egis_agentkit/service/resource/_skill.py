"""管理本地 Skill 归档和 manifest，验证内容摘要及解包路径。"""

from __future__ import annotations

import hashlib
import io
import re
import stat
import zipfile
from pathlib import PurePosixPath

import yaml

from egis_agentkit.client.nacos._settings import (
    DEFAULT_SKILL_ARCHIVE_MAX_BYTES,
    SkillArchiveConfig,
)
from egis_agentkit.errors import NotFound

from ._registry import _LocalRegistry


class LocalSkillRegistry(_LocalRegistry):
    def __init__(self, assets):
        super().__init__(assets, "skill")

    async def register(self, asset, *, actor, archive=None):
        if archive is None:
            raise ValueError("Skill archive is required")
        await self.assets.put_archive(asset.checksum_sha256, archive)
        await super().register(asset, actor=actor)

    async def archive(self, asset):
        archive = await self.assets.get_archive(asset["checksum_sha256"])
        if archive is None:
            raise NotFound("技能版本归档不存在")
        return archive


def _archive_files(
    archive: bytes, maximum: int | SkillArchiveConfig = DEFAULT_SKILL_ARCHIVE_MAX_BYTES
) -> dict[str, bytes]:
    limits = (
        maximum
        if isinstance(maximum, SkillArchiveConfig)
        else SkillArchiveConfig(max_bytes=maximum)
    )
    if len(archive) > limits.max_bytes:
        raise ValueError("Skill archive exceeds upload limit")
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            entries = z.infolist()
            if sum(e.file_size for e in entries) > limits.expanded_bytes_limit:
                raise ValueError("Expanded Skill archive exceeds limit")
            files = {}
            for entry in entries:
                path = PurePosixPath(entry.filename)
                if (
                    path.is_absolute()
                    or ".." in path.parts
                    or "\\" in entry.filename
                    or stat.S_ISLNK(entry.external_attr >> 16)
                ):
                    raise ValueError("Unsafe Skill archive path")
                if entry.is_dir():
                    continue
                if not path.parts or ":" in path.parts[0] or path.as_posix() in files:
                    raise ValueError("Invalid or duplicate Skill archive path")
                files[path.as_posix()] = z.read(entry)
            manifests = [
                name for name in files if PurePosixPath(name).name == "SKILL.md"
            ]
            if len(manifests) != 1:
                raise ValueError("Exactly one SKILL.md is required")
            prefix = PurePosixPath(manifests[0]).parent
            if prefix != PurePosixPath("."):
                if any(
                    not PurePosixPath(name).is_relative_to(prefix) for name in files
                ):
                    raise ValueError(
                        "All Skill files must belong to the manifest directory"
                    )
                files = {
                    str(PurePosixPath(name).relative_to(prefix)): value
                    for name, value in files.items()
                }
            return files
    except (zipfile.BadZipFile, RuntimeError, UnicodeError):
        raise ValueError("Invalid Skill ZIP archive") from None


def _skill_front(markdown: str) -> dict:
    try:
        match = re.match(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|$)", markdown, re.S)
        front = yaml.safe_load(match.group(1)) if match else {}
        if not isinstance(front, dict):
            raise ValueError("Invalid Skill front matter")
        return front
    except yaml.YAMLError:
        raise ValueError("Invalid Skill front matter") from None


def _files_digest(files: dict[str, bytes]) -> str:
    digest = hashlib.sha256()
    for name, value in sorted(files.items()):
        for part in (name.encode(), value):
            digest.update(len(part).to_bytes(8, "big"))
            digest.update(part)
    return digest.hexdigest()


def skill_archive_digest(archive: bytes) -> str:
    """Stable content identity independent of generated ZIP timestamps/root folder."""
    return _files_digest(_archive_files(archive))


def _zip(files: dict[str, bytes], name: str) -> bytes:
    result = io.BytesIO()
    with zipfile.ZipFile(result, "w", zipfile.ZIP_DEFLATED) as z:
        for path, content in sorted(files.items()):
            z.writestr(name + "/" + path, content)
    return result.getvalue()
