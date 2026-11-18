"""持久化项目文件，管理项目目录和交付区域，不承担页面管理流程。

核心能力
- 定位和初始化项目输入、工作、交付目录，校验长期记忆目录。
- 读写、交付和删除项目文件，保留显式覆盖及内容并发校验。
- 按配置限额枚举文件、发现可交付内容，不查询数据库或权限。
"""

import hashlib
import os
import stat
from pathlib import Path, PurePosixPath

from egis_agentkit.client.nacos._settings import (
    ProjectContextConfig,
    ProjectDeliveryConfig,
    ProjectScanConfig,
)
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.storage._directory import is_link
from egis_agentkit.storage._files import FileStorage


class ProjectFileStorage(FileStorage):
    """在通用文件存储上实现项目目录、输入保护和交付规则。"""

    MEMORY_DIRECTORY = ".memory"
    RESERVED = frozenset((".work", "artifacts", MEMORY_DIRECTORY))
    PROBE_DIRECTORY = ".agentkit-probes"  # 内部命名空间，禁止用于项目业务文件。

    @property
    def max_bytes(self):
        """读取当前上传及下载大小限制。"""
        return (
            self.config.snapshot.project.upload.max_bytes
            if self.config
            else self._max_bytes
        )

    def project_root(self, user_id, project_id):
        return self.user_root(user_id) / "projects" / self._identifier(project_id)

    def memory_root(self, user_id, project_id) -> Path:
        """确认已有记忆目录不经过符号链接，首次创建交给 ReMe。"""
        directory = self.project_root(user_id, project_id) / self.MEMORY_DIRECTORY
        try:
            with self.open_directory(directory, []):
                pass
        except NotFound:
            return directory
        return directory

    def initialize_project(self, user_id, project_id):
        if self._nas is not None:
            return self._nas.call(
                "project", "initialize_project", user_id=user_id, project_id=project_id
            )
        for area in ("uploads", "work", "artifacts"):
            with self.open_directory(
                self.area_root(user_id, project_id, area), [], create=True
            ):
                pass

    def area_root(self, user_id, project_id, area):
        if area not in ("work", "artifacts", "uploads"):
            raise Forbidden("非法工作区范围")
        root = self.project_root(user_id, project_id)
        return (
            root if area == "uploads" else root / (".work" if area == "work" else area)
        )

    def _area_parts(self, area, path):
        if any(
            part.casefold() == self.PROBE_DIRECTORY
            for part in PurePosixPath(path).parts
        ):
            raise Forbidden("该目录由工作区就绪检查保留")
        parts = self._parts(path)
        if area == "uploads" and parts and parts[0].casefold() in self.RESERVED:
            raise Forbidden("上传资料不能访问项目内部目录")
        return parts

    def resolve(self, user_id, project_id, area, path):
        root = self.area_root(user_id, project_id, area)
        parts = self._area_parts(area, path)
        value = root.joinpath(*parts)
        # 即使是目录内部的符号链接也拒绝，避免检查后被重新指向其他位置。
        candidate = root
        for part in parts:
            candidate = candidate / part
            if candidate.is_symlink():
                raise Forbidden("不允许通过符号链接访问文件")
        if not value.resolve().is_relative_to(root.resolve()):
            raise Forbidden()
        return value

    def read(self, user_id, project_id, area, path):
        if self._nas is not None:
            return self._nas.call(
                "project",
                "read",
                user_id=user_id,
                project_id=project_id,
                area=area,
                path=path,
            )
        self._area_parts(area, path)
        return self._read_root(self.area_root(user_id, project_id, area), path)

    def write(
        self,
        user_id,
        project_id,
        path,
        content: bytes,
        *,
        area="work",
        overwrite=None,
        return_metadata=False,
    ):
        if self._nas is not None:
            return self._nas.call(
                "project",
                "write",
                user_id=user_id,
                project_id=project_id,
                path=path,
                content=content,
                area=area,
                overwrite=overwrite,
                return_metadata=return_metadata,
            )
        self._area_parts(area, path)
        root = self.area_root(user_id, project_id, area)
        target = self._write_root(root, path, content, overwrite=overwrite)
        if not return_metadata:
            return target
        relative = target.relative_to(root)
        with self.open_directory(root, relative.parts[:-1]) as directory:
            return {
                "name": relative.name,
                "path": relative.as_posix(),
                "is_directory": False,
                "size": len(content),
                "modified_at": directory.stat(relative.name).st_mtime,
            }

    def update_file(
        self,
        user_id: str,
        project_id: str,
        area: str,
        path: str,
        content: bytes,
        checksum: str,
        *,
        return_metadata: bool = False,
    ) -> Path | PurePosixPath | dict:
        """校验已读文件摘要并原子替换；调用方持有项目文件锁。"""
        if self._nas is not None:
            return self._nas.call(
                "project",
                "update_file",
                user_id=user_id,
                project_id=project_id,
                area=area,
                path=path,
                content=content,
                checksum=checksum,
                return_metadata=return_metadata,
            )
        previous, _ = self.read(user_id, project_id, area, path)
        if hashlib.sha256(previous).hexdigest() != checksum:
            raise Conflict("文件已被更新，请先下载当前编辑内容，再重新打开最新文件")
        return self.write(
            user_id,
            project_id,
            path,
            content,
            area=area,
            overwrite=True,
            return_metadata=return_metadata,
        )

    def scan(self, user_id, project_id, limits=None):
        """按同一份扫描限额读取字节并计算文件指纹。"""
        if self._nas is not None:
            return self._nas.call(
                "project", "scan", user_id=user_id, project_id=project_id
            )
        if limits is None:
            limits = (
                self.config.snapshot.project.scan
                if self.config
                else ProjectScanConfig()
            )
        result = {}
        count = total = 0

        def walk(directory, area, prefix, depth):
            nonlocal count, total
            if depth > limits.max_depth:
                raise Conflict("文件目录过深，无法完整记录变更")
            for name in directory.list():
                normalized = name.casefold()
                reserved = (
                    area == "uploads" and not prefix and normalized in self.RESERVED
                )
                if normalized == self.PROBE_DIRECTORY or (
                    reserved and normalized == self.MEMORY_DIRECTORY
                ):
                    continue
                count += 1
                if count > limits.max_entries:
                    raise Conflict("文件数量超过变更记录扫描限制")
                info = directory.stat(name)
                if is_link(info):
                    continue
                path = f"{prefix}/{name}" if prefix else name
                if len(path.encode("utf-8")) > limits.max_path_bytes:
                    raise Conflict("文件路径过长，无法完整记录变更")
                if stat.S_ISDIR(info.st_mode):
                    with directory.child(name) as child:
                        if reserved:
                            walk(
                                child,
                                "work" if normalized == ".work" else "artifacts",
                                "",
                                depth + 1,
                            )
                        else:
                            walk(child, area, path, depth + 1)
                elif stat.S_ISREG(info.st_mode) and not reserved:
                    total += info.st_size
                    if total > limits.max_total_bytes:
                        raise Conflict("文件数量或大小超过变更记录扫描限制")
                    item = directory.open_file(name, os.O_RDONLY)
                    try:
                        before = os.fstat(item)
                        if not stat.S_ISREG(before.st_mode):
                            raise Conflict("文件在扫描期间发生变化")
                        digest = hashlib.sha256()
                        size = 0
                        while chunk := os.read(item, limits.read_chunk_bytes):
                            size += len(chunk)
                            if (
                                size > before.st_size
                                or total - info.st_size + size > limits.max_total_bytes
                            ):
                                raise Conflict("文件在扫描期间发生变化")
                            digest.update(chunk)
                        after = os.fstat(item)
                        if (before.st_size, before.st_mtime_ns, before.st_ino) != (
                            after.st_size,
                            after.st_mtime_ns,
                            after.st_ino,
                        ) or size != before.st_size:
                            raise Conflict("文件在扫描期间发生变化")
                        result[(area, path)] = (digest.hexdigest(), size)
                    finally:
                        os.close(item)

        try:
            with self.open_directory(
                self.project_root(user_id, project_id), []
            ) as directory:
                walk(directory, "uploads", "", 0)
        except NotFound:
            return result
        return result

    def context_inputs(self, user_id, project_id):
        """有界枚举项目可见输入，忽略内部目录和符号链接。"""
        if self._nas is not None:
            return self._nas.call(
                "project", "context_inputs", user_id=user_id, project_id=project_id
            )
        limits = (
            self.config.snapshot.project.context
            if self.config
            else ProjectContextConfig()
        )
        root = self.project_root(user_id, project_id)
        result = []
        remaining_directories = limits.max_directories

        def walk(directory, path):
            nonlocal remaining_directories
            remaining_directories -= 1
            children = []
            for name in sorted(directory.list()):
                normalized = name.casefold()
                if normalized == self.PROBE_DIRECTORY or (
                    not path and normalized in self.RESERVED
                ):
                    continue
                try:
                    info = directory.stat(name)
                except FileNotFoundError:
                    continue
                if is_link(info):
                    continue
                if stat.S_ISDIR(info.st_mode) and not name.startswith("."):
                    children.append(name)
                elif stat.S_ISREG(info.st_mode):
                    result.append(str(root.joinpath(*path, name)))
                    if len(result) == limits.max_files:
                        return

            for name in children:
                if not remaining_directories or len(result) == limits.max_files:
                    return
                try:
                    with directory.child(name) as child:
                        walk(child, (*path, name))
                except (FileNotFoundError, NotADirectoryError):
                    continue

        try:
            with self.open_directory(root, []) as directory:
                walk(directory, ())
        except (NotFound, Forbidden):
            pass
        return result

    def list(self, user_id, project_id, area, path="", *, recursive=False):
        if self._nas is not None:
            return self._nas.call(
                "project",
                "list",
                user_id=user_id,
                project_id=project_id,
                area=area,
                path=path,
                recursive=recursive,
            )
        root = self.area_root(user_id, project_id, area)
        self._area_parts(area, path)
        ignored = {self.PROBE_DIRECTORY}
        if area == "uploads":
            ignored.update(self.RESERVED)
        if recursive:
            return list(self._walk_files(root, path, ignored_root=ignored))
        return [
            item
            for item in self._list_root(root, path, ignored_root=ignored)
            if item["name"].casefold() != self.PROBE_DIRECTORY
        ]

    def deliver(self, user_id, project_id, path):
        """在一次交付操作中使用同一个文件大小限额。"""
        if self._nas is not None:
            return self._nas.call(
                "project", "deliver", user_id=user_id, project_id=project_id, path=path
            )
        self._area_parts("work", path)
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
        """按限额枚举项目文件，和下载一样禁止跟随符号链接。"""
        if limits is None:
            limits = (
                self.config.snapshot.project.delivery
                if self.config
                else ProjectDeliveryConfig()
            )
        count = total = 0

        def walk(relative):
            nonlocal count, total
            for item in self._list_root(root, relative, ignored_root=ignored_root):
                if item["name"].casefold() == self.PROBE_DIRECTORY:
                    continue
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
        with self.open_directory(root, parts[:-1]) as directory:
            return directory.stat(parts[-1]).st_mtime_ns

    def discover_deliveries(
        self,
        user_id,
        project_id,
        *,
        eligible_paths=None,
        deleted_after=None,
        settings=None,
    ):
        """从工作目录发现可交付文件，不把上传资料和脚本自动当作产物。

        显式写入产物目录的文件优先，自动发现只接受常见文档和媒体格式。
        排除与上传资料内容一致的副本，不覆盖已有产物；重复扫描保持幂等。
        """
        if self._nas is not None:
            return self._nas.call(
                "project",
                "discover_deliveries",
                user_id=user_id,
                project_id=project_id,
                eligible_paths=sorted(eligible_paths)
                if eligible_paths is not None
                else None,
                deleted_after=deleted_after,
            )
        if eligible_paths is not None:
            eligible_paths = set(eligible_paths)
        if settings is None and self.config:
            settings = self.config.snapshot.project
        limits = settings.delivery if settings else ProjectDeliveryConfig()
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

    def remove(self, user_id, project_id, area, path):
        """删除项目中的文件或子目录，禁止单独删除各区域根目录。"""
        if self._nas is not None:
            return self._nas.call(
                "project",
                "remove",
                user_id=user_id,
                project_id=project_id,
                area=area,
                path=path,
            )
        parts = self._area_parts(area, path)
        if not parts:
            raise Forbidden("项目根目录、.work 和产物目录不能删除")
        with self.open_directory(
            self.area_root(user_id, project_id, area), parts[:-1]
        ) as parent:
            try:
                self._remove_entry(parent, parts[-1])
            except FileNotFoundError:
                pass  # A retry after a completed delete is idempotent.

    def write_probe_file(self, user_id, project_id, area, token, content):
        """原子写入独立探针文件；覆盖已有内容仅限当前探针的随机身份。"""
        if self._nas is not None:
            return self._nas.call(
                "project",
                "write_probe_file",
                user_id=user_id,
                project_id=project_id,
                area=area,
                token=token,
                content=content,
            )
        root = self.area_root(user_id, project_id, area)
        path = self.PROBE_DIRECTORY + "/" + self._identifier(token)
        self._write_root(root, path, content, overwrite=False, max_bytes=len(content))
        return str(root / path)

    def read_probe_file(self, user_id, project_id, area, token, size):
        """验证沙箱写回的数据，继续复用受管存储的防符号链接检查。"""
        if self._nas is not None:
            return self._nas.call(
                "project",
                "read_probe_file",
                user_id=user_id,
                project_id=project_id,
                area=area,
                token=token,
                size=size,
            )
        root = self.area_root(user_id, project_id, area)
        return self._read_root(
            root, self.PROBE_DIRECTORY + "/" + self._identifier(token), max_bytes=size
        )[0]

    def remove_probe_file(self, user_id, project_id, area, token):
        """只清理当前探针文件，不删除保留目录或其他运行实例的探针。"""
        if self._nas is not None:
            return self._nas.call(
                "project",
                "remove_probe_file",
                user_id=user_id,
                project_id=project_id,
                area=area,
                token=token,
            )
        root = self.area_root(user_id, project_id, area)
        try:
            with self.open_directory(root, [self.PROBE_DIRECTORY]) as parent:
                self._remove_entry(parent, self._identifier(token))
        except (NotFound, FileNotFoundError):
            pass

    def remove_project(self, user_id, project_id):
        """删除一个项目的目录树，遇到符号链接只移除链接本身。"""
        if self._nas is not None:
            return self._nas.call(
                "project", "remove_project", user_id=user_id, project_id=project_id
            )
        try:
            with self.open_directory(
                self.user_root(user_id) / "projects", []
            ) as parent:
                self._remove_entry(parent, self._identifier(project_id))
        except (NotFound, FileNotFoundError):
            pass
