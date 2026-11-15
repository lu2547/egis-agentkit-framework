"""提供受管目录的安全文件读写，复用于用户文档和项目文件。

核心能力
- 校验标识、配置中的路径限额及目录边界，禁止跟随符号链接。
- 在文件锁内读取、原子替换用户文档，统一上传覆盖冲突处理。
- 通过目录描述符完成枚举和删除，避免跨目录访问。
"""

import fcntl
import mimetypes
import os
import re
import stat
from contextlib import contextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

from egis_agentkit.client.nacos._settings import (
    DEFAULT_WORKSPACE_UPLOAD_BYTES,
    ProjectScanConfig,
)
from egis_agentkit.errors import Conflict, Forbidden, NotFound, UploadNameConflict


class FileStorage:
    """受管文件 IO 的公共实现，不处理项目生命周期或业务授权。"""

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
        return self._max_bytes

    @staticmethod
    def _identifier(value):
        if not re.fullmatch(r"[\w.@-]{1,128}", value) or value in (".", ".."):
            raise Forbidden("非法用户或项目标识")
        return value

    def user_root(self, user_id):
        return self.root / "user" / self._identifier(user_id)

    @contextmanager
    def _user_files_lock(self, user_id, directory, *, exclusive):
        """在用户目录内锁定一组文件；路径遍历和符号链接规则沿用统一文件存储。"""
        root = self.user_root(user_id) / self._identifier(directory)
        parent = self._directory_fd(self.user_root(user_id), [], create=True)
        try:
            descriptor = os.open(
                f".files-{directory}.lock",
                os.O_RDWR | os.O_CREAT | os.O_NOFOLLOW,
                0o600,
                dir_fd=parent,
            )
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX if exclusive else fcntl.LOCK_SH)
                yield root
            finally:
                os.close(descriptor)
        finally:
            os.close(parent)

    def read_user_texts(self, user_id, directory, filenames, *, max_bytes):
        """在同一文件锁内读取一组用户文本，缺失项交由调用方补默认值。"""
        result = {}
        with self._user_files_lock(user_id, directory, exclusive=False) as root:
            for name in filenames:
                try:
                    content, _ = self._read_root(root, name, max_bytes=max_bytes)
                except NotFound:
                    continue
                result[name] = content.decode("utf-8")
        return result

    def write_user_texts(self, user_id, directory, documents, *, max_bytes):
        """在同一文件锁内以临时文件替换用户文本，读者不会读到写入中的文件。"""
        with self._user_files_lock(user_id, directory, exclusive=True) as root:
            for name, content in documents.items():
                self._write_root(
                    root, name, content.encode(), max_bytes=max_bytes, overwrite=True
                )

    def _parts(self, path):
        """用当前路径限额校验相对路径，按 UTF-8 字节计算路径长度。"""
        limits = (
            self.config.snapshot.project.scan if self.config else ProjectScanConfig()
        )
        if (
            "\\" in path
            or "\x00" in path
            or len(path.encode("utf-8")) > limits.max_path_bytes
        ):
            raise Forbidden("非法文件路径")
        value = PurePosixPath(path or ".")
        if (
            value.is_absolute()
            or ".." in value.parts
            or len(value.parts) > limits.max_depth
        ):
            raise Forbidden("文件必须位于当前任务目录")
        return [p for p in value.parts if p != "."]

    def _directory_fd(self, root: Path, parts: list[str], *, create=False):
        # 配置的根目录是信任边界；其下每一级目录都禁止跟随符号链接。
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
