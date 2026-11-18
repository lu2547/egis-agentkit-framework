"""提供受管目录的安全文件读写，复用于用户文档和项目文件。

核心能力
- initialize_user_workspace：按既定 NAS 布局补齐用户标准目录，不创建具体项目。
- 校验标识、配置中的路径限额及目录边界，禁止跟随符号链接。
- 在文件锁内读取、原子替换用户文档，统一上传覆盖冲突处理。
- 在宿主平台固定受管目录后枚举和删除，避免跨目录访问。
"""

import mimetypes
import os
import re
import stat
from contextlib import ExitStack, contextmanager, suppress
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

from egis_agentkit.client.nacos._settings import (
    DEFAULT_WORKSPACE_UPLOAD_BYTES,
    ProjectScanConfig,
    WorkspaceFilesConfig,
)
from egis_agentkit.client.nas_files import NasFilesClient
from egis_agentkit.errors import Conflict, Forbidden, NotFound, UploadNameConflict

from ._directory import PosixDirectory, is_link
from ._file_lock import hold_file_lock


class FileStorage:
    """受管文件 IO 的公共实现，不处理项目生命周期或业务授权。"""

    USER_WORKSPACE_DIRECTORIES = ("skills", "session", "projects")

    def __init__(
        self,
        root: str | PurePosixPath,
        max_bytes=DEFAULT_WORKSPACE_UPLOAD_BYTES,
        *,
        config_provider=None,
    ):
        self.config = config_provider
        self._max_bytes = max_bytes
        workspace = (
            getattr(config_provider.snapshot, "workspace", None)
            if config_provider
            else None
        )
        settings = getattr(workspace, "files", None) or WorkspaceFilesConfig()
        if os.name == "nt" and not settings.win:
            raise ValueError("Windows Agent 需要启用 agentkit.workspace.files.win")
        self._nas = NasFilesClient(settings) if settings.win else None
        self.root = PurePosixPath(root) if settings.win else Path(root).resolve()

    @property
    def uses_nas_api(self):
        """文件是否由挂载 NAS 的 Linux 服务操作。"""
        return self._nas is not None

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

    def user_skill_runtime_root(self, user_id, runtime_binding_id):
        return (
            self.user_root(user_id)
            / "skills"
            / ".runtime"
            / self._identifier(runtime_binding_id)
        )

    def initialize_user_workspace(self, user_id, runtime_binding_id=None):
        """补齐用户目录及执行分区；已有内容保留，每一级都拒绝符号链接。"""
        if self._nas is not None:
            return self._nas.call(
                "workspace", "initialize_user_workspace",
                user_id=user_id, runtime_binding_id=runtime_binding_id,
            )
        root = self.user_root(user_id)
        for directory in self.USER_WORKSPACE_DIRECTORIES:
            with self.open_directory(root, [directory], create=True):
                pass
        if runtime_binding_id is not None:
            binding = self._identifier(runtime_binding_id)
            for directory in (
                self.user_skill_runtime_root(user_id, binding),
                root / "session" / binding,
            ):
                with self.open_directory(directory, [], create=True):
                    pass

    @contextmanager
    def _user_files_lock(self, user_id, directory, *, exclusive):
        """在用户目录内锁定一组文件；路径遍历和符号链接规则沿用统一文件存储。"""
        root = self.user_root(user_id) / self._identifier(directory)
        with self.open_directory(self.user_root(user_id), [], create=True) as parent:
            descriptor = parent.open_file(
                f".files-{directory}.lock", os.O_RDWR | os.O_CREAT
            )
            try:
                if not stat.S_ISREG(os.fstat(descriptor).st_mode):
                    raise Forbidden("文件锁必须使用普通文件")
                with hold_file_lock(descriptor, exclusive=exclusive):
                    yield root
            finally:
                os.close(descriptor)

    def read_user_texts(self, user_id, directory, filenames, *, max_bytes):
        """在同一文件锁内读取一组用户文本，缺失项交由调用方补默认值。"""
        if self._nas is not None:
            return self._nas.call(
                "workspace", "read_user_texts", user_id=user_id, directory=directory,
                filenames=tuple(filenames), max_bytes=max_bytes,
            )
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
        if self._nas is not None:
            return self._nas.call(
                "workspace", "write_user_texts", user_id=user_id, directory=directory,
                documents=documents, max_bytes=max_bytes,
            )
        with self._user_files_lock(user_id, directory, exclusive=True) as root:
            for name, content in documents.items():
                self._write_root(
                    root, name, content.encode(), max_bytes=max_bytes, overwrite=True
                )

    def _parts(self, path):
        """用当前路径限额校验相对路径，按 UTF-8 字节计算路径长度。"""
        project = getattr(self.config.snapshot, "project", None) if self.config else None
        limits = project.scan if project is not None else ProjectScanConfig()
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

    @contextmanager
    def open_directory(self, root: Path, parts: list[str], *, create=False):
        """在操作期间固定根下的各级目录并拒绝跟随链接。"""
        if self._nas is not None:
            raise RuntimeError("HTTP 文件模式必须通过存储操作访问 NAS")
        with ExitStack() as stack:
            try:
                if create:
                    self.root.mkdir(parents=True, exist_ok=True)
                current = stack.enter_context(PosixDirectory.open(self.root))
                for part in (*root.relative_to(self.root).parts, *parts):
                    if create:
                        with suppress(FileExistsError):
                            current.mkdir(part)
                    current = stack.enter_context(current.child(part))
            except FileNotFoundError as exc:
                raise NotFound("文件目录不存在") from exc
            except OSError as exc:
                raise Forbidden("不允许访问此文件路径") from exc
            yield current

    def _read_root(self, root, path, *, max_bytes=None):
        max_bytes = self.max_bytes if max_bytes is None else max_bytes
        parts = self._parts(path)
        if not parts:
            raise Forbidden("请选择文件")
        with self.open_directory(root, parts[:-1]) as directory:
            try:
                item = directory.open_file(parts[-1], os.O_RDONLY)
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

    @staticmethod
    def _require_regular_target(directory, name):
        try:
            existing = directory.stat(name)
        except FileNotFoundError:
            return
        if is_link(existing) or not stat.S_ISREG(existing.st_mode):
            raise Conflict("同名路径不是普通文件，无法覆盖。")

    def _write_root(self, root, path, content, *, overwrite=None, max_bytes=None):
        max_bytes = self.max_bytes if max_bytes is None else max_bytes
        if len(content) > max_bytes:
            raise Conflict("上传文件超过大小限制")
        parts = self._parts(path)
        if not parts:
            raise Forbidden("文件名不能为空")
        with self.open_directory(root, parts[:-1], create=True) as directory:
            name = parts[-1]
            if overwrite:
                self._require_regular_target(directory, name)
            output_name = f".upload-{uuid4().hex}" if overwrite else name
            try:
                out = directory.open_file(
                    output_name, os.O_WRONLY | os.O_CREAT | os.O_EXCL
                )
            except FileExistsError:
                if overwrite is False:
                    self._require_regular_target(directory, name)
                    raise UploadNameConflict() from None
                if overwrite:
                    raise
                stem, suffix = os.path.splitext(name)
                name = output_name = f"{stem}-{uuid4().hex[:8]}{suffix}"
                out = directory.open_file(name, os.O_WRONLY | os.O_CREAT | os.O_EXCL)
            try:
                with os.fdopen(out, "wb") as stream:
                    stream.write(content)
                    stream.flush()
                    os.fsync(stream.fileno())
                if overwrite:
                    directory.rename(output_name, name, replace=True)
            except BaseException:
                with suppress(FileNotFoundError):
                    directory.unlink(output_name)
                raise
            return root.joinpath(*parts[:-1], name)

    def _list_root(self, root, path, *, ignored_root=()):
        parts = self._parts(path)
        try:
            with self.open_directory(root, parts) as directory:
                result = []
                for name in directory.list():
                    if not parts and name.casefold() in ignored_root:
                        continue
                    info = directory.stat(name)
                    if is_link(info) or not (
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
        except NotFound:
            return []
        return sorted(result, key=lambda r: (not r["is_directory"], r["name"]))

    def _remove_entry(self, parent, name):
        info = parent.stat(name)
        if not stat.S_ISDIR(info.st_mode):
            parent.unlink(name)
            return
        if not is_link(info):
            with parent.child(name) as child:
                for entry in child.list():
                    self._remove_entry(child, entry)
        parent.rmdir(name)
