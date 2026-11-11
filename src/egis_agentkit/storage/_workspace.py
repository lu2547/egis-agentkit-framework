"""Authorized callers pass user/project IDs; clients never pass host paths.

Reads/writes walk directory file descriptors with O_NOFOLLOW, avoiding
the check-then-FileResponse symlink race. Only regular files are served.
"""

import hashlib
import mimetypes
import os
import re
import stat
from contextlib import suppress
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

from egis_agentkit.backend.nacos._settings import (
    DEFAULT_WORKSPACE_UPLOAD_BYTES,
    WorkspaceDeliveryConfig,
)
from egis_agentkit.errors import Conflict, Forbidden, NotFound, UploadNameConflict


class WorkspaceFiles:
    RESERVED = frozenset((".work", "artifacts"))

    def __init__(
        self,
        root: Path,
        max_bytes=DEFAULT_WORKSPACE_UPLOAD_BYTES,
        *,
        config_provider=None,
    ):
        self.root = Path(root).resolve()
        self.config = config_provider
        self._max_bytes = max_bytes

    @property
    def max_bytes(self):
        """读取当前上传及下载大小限制。"""
        return (
            self.config.snapshot.workspace.upload.max_bytes
            if self.config
            else self._max_bytes
        )

    @staticmethod
    def _identifier(value):
        if not re.fullmatch(r"[\w.@-]{1,128}", value) or value in (".", ".."):
            raise Forbidden("非法用户或项目标识")
        return value

    def user_root(self, user_id):
        return self.root / "user" / self._identifier(user_id)

    def project_root(self, user_id, project_id):
        return self.user_root(user_id) / "projects" / self._identifier(project_id)

    def initialize_project(self, user_id, project_id):
        for area in ("uploads", "work", "artifacts"):
            fd = self._directory_fd(
                self.area_root(user_id, project_id, area), [], create=True
            )
            os.close(fd)

    def area_root(self, user_id, project_id, area):
        if area not in ("work", "artifacts", "uploads"):
            raise Forbidden("非法工作区范围")
        root = self.project_root(user_id, project_id)
        return (
            root if area == "uploads" else root / (".work" if area == "work" else area)
        )

    def _area_parts(self, area, path):
        parts = self._parts(path)
        if area == "uploads" and parts and parts[0].casefold() in self.RESERVED:
            raise Forbidden("上传资料不能访问工作或交付目录")
        return parts

    @staticmethod
    def _parts(path):
        if "\\" in path or "\x00" in path or len(path) > 2048:
            raise Forbidden("非法文件路径")
        value = PurePosixPath(path or ".")
        if value.is_absolute() or ".." in value.parts or len(value.parts) > 32:
            raise Forbidden("文件必须位于当前任务目录")
        return [p for p in value.parts if p != "."]

    def resolve(self, user_id, project_id, area, path):
        root = self.area_root(user_id, project_id, area)
        parts = self._area_parts(area, path)
        value = root.joinpath(*parts)
        # Disallow even internal symlinks so an installation cannot retarget later.
        candidate = root
        for part in parts:
            candidate = candidate / part
            if candidate.is_symlink():
                raise Forbidden("不允许通过符号链接访问文件")
        if not value.resolve().is_relative_to(root.resolve()):
            raise Forbidden()
        return value

    def _directory_fd(self, root: Path, parts: list[str], *, create=False):
        # The configured workspace root itself is trusted; everything below
        # it is opened as a directory without following a link.
        self.root.mkdir(parents=True, exist_ok=True)
        fd = os.open(self.root, os.O_RDONLY | os.O_DIRECTORY)
        try:
            all_parts = list(root.relative_to(self.root).parts) + parts
            for part in all_parts:
                if create:
                    try:
                        os.mkdir(part, mode=0o700, dir_fd=fd)
                    except FileExistsError:
                        pass
                nxt = os.open(
                    part, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd
                )
                os.close(fd)
                fd = nxt
            return fd
        except OSError as exc:
            os.close(fd)
            if isinstance(exc, FileNotFoundError):
                raise NotFound("文件目录不存在") from exc
            raise Forbidden("不允许访问此文件路径") from exc

    def read(self, user_id, project_id, area, path):
        self._area_parts(area, path)
        return self._read_root(self.area_root(user_id, project_id, area), path)

    def _read_root(self, root, path, *, max_bytes=None):
        max_bytes = self.max_bytes if max_bytes is None else max_bytes
        parts = self._parts(path)
        if not parts:
            raise Forbidden("请选择文件")
        fd = self._directory_fd(root, parts[:-1])
        try:
            try:
                item = os.open(
                    parts[-1], os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd
                )
            except OSError as exc:
                raise NotFound("文件不存在或不允许读取") from exc
            with os.fdopen(item, "rb") as stream:
                info = os.fstat(stream.fileno())
                if not stat.S_ISREG(info.st_mode):
                    raise Forbidden("只能预览普通文件")
                if info.st_size > max_bytes:
                    raise Conflict("文件超过预览和下载大小限制")
                content = stream.read(max_bytes + 1)
                if len(content) > max_bytes:
                    raise Conflict("文件超过大小限制")
            return content, mimetypes.guess_type(parts[-1])[
                0
            ] or "application/octet-stream"
        finally:
            os.close(fd)

    def write(
        self, user_id, project_id, path, content: bytes, *, area="work", overwrite=None
    ):
        self._area_parts(area, path)
        return self._write_root(
            self.area_root(user_id, project_id, area),
            path,
            content,
            overwrite=overwrite,
        )

    @staticmethod
    def _require_regular_target(fd, name):
        try:
            existing = os.stat(name, dir_fd=fd, follow_symlinks=False)
        except FileNotFoundError:
            return
        if not stat.S_ISREG(existing.st_mode):
            raise Conflict("同名路径不是普通文件，无法覆盖。")

    def _write_root(self, root, path, content, *, overwrite=None, max_bytes=None):
        max_bytes = self.max_bytes if max_bytes is None else max_bytes
        if len(content) > max_bytes:
            raise Conflict("上传文件超过大小限制")
        parts = self._parts(path)
        if not parts:
            raise Forbidden("文件名不能为空")
        fd = self._directory_fd(root, parts[:-1], create=True)
        try:
            name = parts[-1]
            if overwrite:
                self._require_regular_target(fd, name)
            output_name = f".upload-{uuid4().hex}" if overwrite else name
            try:
                out = os.open(
                    output_name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=fd,
                )
            except FileExistsError:
                if overwrite is False:
                    self._require_regular_target(fd, name)
                    raise UploadNameConflict() from None
                if overwrite:
                    raise
                stem, suffix = os.path.splitext(name)
                name = output_name = f"{stem}-{uuid4().hex[:8]}{suffix}"
                out = os.open(
                    name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=fd,
                )
            try:
                with os.fdopen(out, "wb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                if overwrite:
                    os.replace(output_name, name, src_dir_fd=fd, dst_dir_fd=fd)
            except BaseException:
                with suppress(FileNotFoundError):
                    os.unlink(output_name, dir_fd=fd)
                raise
            return root.joinpath(*parts[:-1], name)
        finally:
            os.close(fd)

    def list(self, user_id, project_id, area, path="", *, recursive=False):
        root = self.area_root(user_id, project_id, area)
        self._area_parts(area, path)
        ignored = self.RESERVED if area == "uploads" else ()
        if recursive:
            return list(self._walk_files(root, path, ignored_root=ignored))
        return self._list_root(root, path, ignored_root=ignored)

    def _list_root(self, root, path, *, ignored_root=()):
        parts = self._parts(path)
        try:
            fd = self._directory_fd(root, parts)
        except NotFound:
            return []
        try:
            result = []
            for name in os.listdir(fd):
                if not parts and name.casefold() in ignored_root:
                    continue
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode) or not (
                    stat.S_ISREG(info.st_mode) or stat.S_ISDIR(info.st_mode)
                ):
                    continue
                result.append(
                    {
                        "name": name,
                        "path": "/".join([*parts, name]),
                        "is_directory": stat.S_ISDIR(info.st_mode),
                        "size": info.st_size,
                        "modified_at": datetime.fromtimestamp(
                            info.st_mtime, timezone.utc
                        ).isoformat(),
                    }
                )
            return sorted(result, key=lambda r: (not r["is_directory"], r["name"]))
        finally:
            os.close(fd)

    def deliver(self, user_id, project_id, path):
        """在一次交付操作中使用同一个文件大小限额。"""
        max_bytes = self.max_bytes
        work = self.area_root(user_id, project_id, "work")
        artifacts = self.area_root(user_id, project_id, "artifacts")
        content, _ = self._read_root(work, path, max_bytes=max_bytes)
        return self._write_root(artifacts, path, content, max_bytes=max_bytes)

    def _walk_files(
        self,
        root,
        path="",
        *,
        ignored=(),
        ignored_root=(),
        skip_hidden=False,
        limits=None,
    ):
        """Bounded enumeration, using the same no-follow access as downloads."""
        if limits is None:
            limits = (
                self.config.snapshot.workspace.delivery
                if self.config
                else WorkspaceDeliveryConfig()
            )
        count = total = 0

        def walk(relative):
            nonlocal count, total
            for item in self._list_root(root, relative, ignored_root=ignored_root):
                count += 1
                if count > limits.max_entries:
                    raise Conflict("文件数量超过扫描限制")
                if (
                    item["name"] in ignored
                    or skip_hidden
                    and item["name"].startswith(".")
                ):
                    continue
                if item["is_directory"]:
                    yield from walk(item["path"])
                else:
                    total += item["size"]
                    if total > limits.max_total_bytes:
                        raise Conflict("文件大小超过扫描限制")
                    yield item

        yield from walk(path)

    def _modified_ns(self, root, path):
        parts = self._parts(path)
        fd = self._directory_fd(root, parts[:-1])
        try:
            return os.stat(parts[-1], dir_fd=fd, follow_symlinks=False).st_mtime_ns
        finally:
            os.close(fd)

    def discover_deliveries(
        self,
        user_id,
        project_id,
        *,
        eligible_paths=None,
        deleted_after=None,
        settings=None,
    ):
        """Recover document/media outputs without promoting uploads or scripts.

        Explicit artifacts remain authoritative. Automatic discovery is limited
        to common deliverable formats in this project's work directory.
        Unchanged copies of project uploads are excluded.
        Existing artifacts are immutable and repeat scans are content-idempotent.
        """
        if settings is None and self.config:
            settings = self.config.snapshot.workspace
        limits = settings.delivery if settings else WorkspaceDeliveryConfig()
        max_bytes = settings.upload.max_bytes if settings else self._max_bytes
        extensions = {
            ".md",
            ".txt",
            ".pdf",
            ".doc",
            ".docx",
            ".ppt",
            ".pptx",
            ".xls",
            ".xlsx",
            ".csv",
            ".tsv",
            ".html",
            ".htm",
            ".rtf",
            ".png",
            ".jpg",
            ".jpeg",
            ".gif",
            ".webp",
            ".svg",
            ".mp3",
            ".wav",
            ".mp4",
            ".webm",
            ".zip",
        }
        ignored = {"uploads", "node_modules", "__pycache__", "venv", "env"}
        work = self.area_root(user_id, project_id, "work")
        artifacts = self.area_root(user_id, project_id, "artifacts")
        candidates = []
        for item in self._walk_files(
            work, ignored=ignored, skip_hidden=True, limits=limits
        ):
            if (
                (eligible_paths is None or item["path"] in eligible_paths)
                and Path(item["path"]).suffix.lower() in extensions
                and item["size"] <= max_bytes
            ):
                content, _ = self._read_root(work, item["path"], max_bytes=max_bytes)
                candidates.append(
                    (
                        item["path"],
                        content,
                        hashlib.sha256(content).hexdigest(),
                        self._modified_ns(work, item["path"]),
                    )
                )
        if not candidates:
            return
        uploads = set()
        project_uploads = self.area_root(user_id, project_id, "uploads")
        for item in self._walk_files(
            project_uploads, ignored_root=self.RESERVED, limits=limits
        ):
            if item["size"] <= max_bytes:
                content, _ = self._read_root(
                    project_uploads, item["path"], max_bytes=max_bytes
                )
                uploads.add(hashlib.sha256(content).hexdigest())
        for path, content, digest, modified_ns in candidates:
            if digest in uploads:
                continue
            target = path
            try:
                previous, _ = self._read_root(artifacts, target, max_bytes=max_bytes)
            except NotFound:
                previous = None
            if previous == content:
                continue
            if previous is not None:
                if self._modified_ns(artifacts, target) >= modified_ns:
                    continue
                source = PurePosixPath(path)
                target = str(
                    source.with_name(f"{source.stem}-{digest[:12]}{source.suffix}")
                )
                try:
                    previous, _ = self._read_root(
                        artifacts, target, max_bytes=max_bytes
                    )
                except NotFound:
                    previous = None
                if previous == content:
                    continue
                if (
                    previous is not None
                    and self._modified_ns(artifacts, target) >= modified_ns
                ):
                    continue
            if modified_ns <= (deleted_after or {}).get(target, -1):
                continue
            self._write_root(artifacts, target, content, max_bytes=max_bytes)

    def _remove_entry(self, parent, name):
        info = os.stat(name, dir_fd=parent, follow_symlinks=False)
        if stat.S_ISDIR(info.st_mode):
            child = os.open(
                name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
            )
            try:
                for entry in os.listdir(child):
                    self._remove_entry(child, entry)
            finally:
                os.close(child)
            os.rmdir(name, dir_fd=parent)
        else:
            os.unlink(name, dir_fd=parent)

    def remove(self, user_id, project_id, area, path):
        """Remove a project entry; area roots are never user-deletable."""
        parts = self._area_parts(area, path)
        if not parts:
            raise Forbidden("项目根目录、.work 和产物目录不能删除")
        parent = self._directory_fd(
            self.area_root(user_id, project_id, area), parts[:-1]
        )
        try:
            try:
                self._remove_entry(parent, parts[-1])
            except FileNotFoundError:
                pass  # A retry after a completed delete is idempotent.
        finally:
            os.close(parent)

    def remove_project(self, user_id, project_id):
        """Delete one project's tree, unlinking links without following them."""
        try:
            parent = self._directory_fd(self.user_root(user_id) / "projects", [])
        except NotFound:
            return
        try:
            try:
                self._remove_entry(parent, self._identifier(project_id))
            except FileNotFoundError:
                pass
        finally:
            os.close(parent)
