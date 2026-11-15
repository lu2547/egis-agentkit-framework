"""技能 ZIP 的边界校验、内容摘要与打包规则，复用于发布、下载及数据迁移。"""

import hashlib
import io
import stat
import zipfile
from pathlib import PurePosixPath

from egis_agentkit.client.nacos._settings import (
    DEFAULT_SKILL_ARCHIVE_MAX_BYTES,
    SkillArchiveConfig,
)


def read_archive_files(
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


def calculate_files_digest(files: dict[str, bytes]) -> str:
    digest = hashlib.sha256()
    for name, value in sorted(files.items()):
        for part in (name.encode(), value):
            digest.update(len(part).to_bytes(8, "big"))
            digest.update(part)
    return digest.hexdigest()


def calculate_archive_digest(
    archive: bytes, maximum=DEFAULT_SKILL_ARCHIVE_MAX_BYTES
) -> str:
    """Stable content identity independent of generated ZIP timestamps/root folder."""
    return calculate_files_digest(read_archive_files(archive, maximum))


def build_skill_archive(files: dict[str, bytes], name: str) -> bytes:
    result = io.BytesIO()
    with zipfile.ZipFile(result, "w", zipfile.ZIP_DEFLATED) as z:
        for path, content in sorted(files.items()):
            z.writestr(name + "/" + path, content)
    return result.getvalue()
