"""Authorized callers pass user/project IDs; clients never pass host paths.

Reads/writes walk directory file descriptors with O_NOFOLLOW, avoiding
the check-then-FileResponse symlink race. Only regular files are served.
"""

import hashlib
import json
import mimetypes
import os
import re
import stat
from datetime import datetime, timezone
from pathlib import Path, PurePosixPath
from uuid import uuid4

from ..errors import Conflict, Forbidden, NotFound


class WorkspaceFiles:
    RESERVED = frozenset((".work", "artifacts"))

    def __init__(self, root: Path, max_bytes=20 * 1024 * 1024):
        self.root = Path(root).resolve()
        self.max_bytes = max_bytes

    @staticmethod
    def _identifier(value):
        if not re.fullmatch(r"[\w.@-]{1,128}", value) or value in (".", ".."):
            raise Forbidden("非法用户或项目标识")
        return value

    def user_root(self, user_id):
        return self.root / "user" / self._identifier(user_id)

    def project_root(self, user_id, project_id):
        return self.user_root(user_id) / "projects" / self._identifier(project_id)

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

    def _read_root(self, root, path):
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
                if info.st_size > self.max_bytes:
                    raise Conflict("文件超过预览和下载大小限制")
                content = stream.read(self.max_bytes + 1)
                if len(content) > self.max_bytes:
                    raise Conflict("文件超过大小限制")
            return content, mimetypes.guess_type(parts[-1])[
                0
            ] or "application/octet-stream"
        finally:
            os.close(fd)

    def write(self, user_id, project_id, path, content: bytes, *, area="work"):
        self._area_parts(area, path)
        return self._write_root(
            self.area_root(user_id, project_id, area), path, content
        )

    def _write_root(self, root, path, content):
        if len(content) > self.max_bytes:
            raise Conflict("上传文件超过大小限制")
        parts = self._parts(path)
        if not parts:
            raise Forbidden("文件名不能为空")
        fd = self._directory_fd(root, parts[:-1], create=True)
        try:
            name = parts[-1]
            try:
                out = os.open(
                    name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=fd,
                )
            except FileExistsError:
                stem, suffix = os.path.splitext(name)
                name = f"{stem}-{uuid4().hex[:8]}{suffix}"
                out = os.open(
                    name,
                    os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW,
                    0o600,
                    dir_fd=fd,
                )
            with os.fdopen(out, "wb") as stream:
                stream.write(content)
                stream.flush()
                os.fsync(stream.fileno())
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

    def _list_root(self, root, path, *, prefix="", ignored_root=()):
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
                        "path": "/".join(filter(None, [prefix, *parts, name])),
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

    def _user_path(self, user_id, path, *, upload_only=False):
        parts = self._parts(path)
        allowed = ("uploads",) if upload_only else ("work", "artifacts", "uploads")
        if not parts or parts[0] not in allowed:
            raise Forbidden("非法工作区范围")
        return self.user_root(user_id) / parts[0], "/".join(parts[1:]), parts[0]

    def list_user(self, user_id, path=""):
        if not self._parts(path):
            for area in ("work", "artifacts", "uploads"):
                fd = self._directory_fd(self.user_root(user_id) / area, [], create=True)
                os.close(fd)
            return [
                item
                for item in self._list_root(self.user_root(user_id), "")
                if item["name"] in ("work", "artifacts", "uploads")
            ]
        root, relative, area = self._user_path(user_id, path)
        return self._list_root(root, relative, prefix=area)

    def read_user(self, user_id, path):
        root, relative, _ = self._user_path(user_id, path)
        return self._read_root(root, relative)

    def write_user(self, user_id, path, content):
        root, relative, _ = self._user_path(user_id, path, upload_only=True)
        return self._write_root(root, relative, content)

    def deliver(self, user_id, project_id, path):
        content, _ = self.read(user_id, project_id, "work", path)
        return self.write(user_id, project_id, path, content, area="artifacts")

    def _walk_files(
        self, root, path="", *, ignored=(), ignored_root=(), skip_hidden=False
    ):
        """Bounded enumeration, using the same no-follow access as downloads."""
        count = total = 0

        def walk(relative):
            nonlocal count, total
            for item in self._list_root(root, relative, ignored_root=ignored_root):
                count += 1
                if count > 10000:
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
                    if total > 200 * 1024 * 1024:
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

    def discover_deliveries(self, user_id, project_id):
        """Recover document/media outputs without promoting uploads or scripts.

        Explicit artifacts remain authoritative. Automatic discovery is limited
        to common deliverable formats in this project's work directory. Legacy
        task uploads were copied into work and the user's uploads archive; an
        unchanged copy of those inputs must never be presented as a delivery.
        Existing artifacts are immutable and repeat scans are content-idempotent.
        """
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
        for item in self._walk_files(work, ignored=ignored, skip_hidden=True):
            if (
                Path(item["path"]).suffix.lower() in extensions
                and item["size"] <= self.max_bytes
            ):
                content, _ = self._read_root(work, item["path"])
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
        upload_root = self.user_root(user_id) / "uploads"
        # Legacy task uploads were archived directly under uploads/. Never
        # inspect other sessions' upload directories when deciding a delivery.
        inputs = [
            (upload_root, item)
            for item in self._list_root(upload_root, "")
            if not item["is_directory"]
        ]
        project_uploads = self.area_root(user_id, project_id, "uploads")
        inputs.extend(
            (project_uploads, item)
            for item in self._walk_files(project_uploads, ignored_root=self.RESERVED)
        )
        for root, item in inputs:
            if item["size"] <= self.max_bytes:
                content, _ = self._read_root(root, item["path"])
                uploads.add(hashlib.sha256(content).hexdigest())
        for path, content, digest, modified_ns in candidates:
            if digest in uploads:
                continue
            target = path
            try:
                previous, _ = self._read_root(artifacts, target)
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
                    previous, _ = self._read_root(artifacts, target)
                except NotFound:
                    previous = None
                if previous == content:
                    continue
                if (
                    previous is not None
                    and self._modified_ns(artifacts, target) >= modified_ns
                ):
                    continue
            self._write_root(artifacts, target, content)

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

    def remove_session(self, user_id, session_id):
        """Clean up legacy session trees only; project files outlive sessions."""
        sid = self._identifier(session_id)

        for area in ("work", "artifacts", "uploads"):
            try:
                parent = self._directory_fd(self.user_root(user_id) / area, [])
            except NotFound:
                continue
            try:
                try:
                    self._remove_entry(parent, sid)
                except FileNotFoundError:
                    pass
            finally:
                os.close(parent)

    def migrate_legacy_session(self, user_id, session_id, project_id):
        """Merge legacy files without links or overwrites; retain a retry journal.

        Callers hold the project file lock. Each journal entry is durable before
        its source is removed, so database adoption may safely retry after a crash.
        """
        sid = self._identifier(session_id)
        work = self.area_root(user_id, project_id, "work")
        mapping = {}
        journal_size = 0

        def journal_parts(path, area=None):
            # Legacy native files may exceed the interactive API's depth limit.
            value = PurePosixPath(path)
            if value.is_absolute() or ".." in value.parts or "\x00" in path:
                raise Forbidden("旧会话迁移记录无效")
            parts = list(value.parts)
            if not parts or area == "uploads" and parts[0].casefold() in self.RESERVED:
                raise Forbidden("旧会话迁移记录无效")
            return parts

        try:
            journal_fd = self._directory_fd(work, [".legacy-migrations"])
        except NotFound:
            pass
        else:
            try:
                try:
                    item = os.open(
                        f"{sid}.jsonl",
                        os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK,
                        dir_fd=journal_fd,
                    )
                except FileNotFoundError:
                    pass
                else:
                    with os.fdopen(item, "rb") as stream:
                        if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                            raise Forbidden("旧会话迁移记录无效")
                        for line in stream:
                            if not line.endswith(b"\n"):
                                break
                            area, path, target_area, target = json.loads(line)
                            if (
                                area not in ("uploads", "work", "artifacts")
                                or target_area != area
                            ):
                                raise Forbidden("旧会话迁移记录无效")
                            journal_parts(path)
                            journal_parts(target, target_area)
                            mapping[(area, path)] = (target_area, target)
                            journal_size += len(line)
            finally:
                os.close(journal_fd)

        def save_mapping(key):
            nonlocal journal_size
            fd = self._directory_fd(work, [".legacy-migrations"], create=True)
            try:
                item = os.open(
                    f"{sid}.jsonl",
                    os.O_WRONLY
                    | os.O_CREAT
                    | os.O_APPEND
                    | os.O_NOFOLLOW
                    | os.O_NONBLOCK,
                    0o600,
                    dir_fd=fd,
                )
                with os.fdopen(item, "wb") as stream:
                    if not stat.S_ISREG(os.fstat(stream.fileno()).st_mode):
                        raise Forbidden("旧会话迁移记录无效")
                    os.ftruncate(stream.fileno(), journal_size)
                    line = (json.dumps((*key, *mapping[key])) + "\n").encode()
                    stream.write(line)
                    stream.flush()
                    os.fsync(stream.fileno())
                    journal_size += len(line)
                os.fsync(fd)
            finally:
                os.close(fd)

        def suffix(name, index):
            stem, extension = os.path.splitext(name)
            tag = sid if len(sid.encode()) <= 64 else hashlib.sha256(sid.encode()).hexdigest()[:16]
            ending = f"-legacy-{tag}-{index}"
            extension = extension.encode()[:64].decode("utf-8", errors="ignore")
            remaining = 255 - len((ending + extension).encode())
            stem = stem.encode()[:remaining].decode("utf-8", errors="ignore")
            return f"{stem}{ending}{extension}"

        def equal(source, parent, name):
            try:
                target = os.open(
                    name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=parent
                )
            except OSError:
                return False
            try:
                info = os.fstat(target)
                if (
                    not stat.S_ISREG(info.st_mode)
                    or info.st_size != os.fstat(source).st_size
                ):
                    return False
                os.lseek(source, 0, os.SEEK_SET)
                while chunk := os.read(source, 1024 * 1024):
                    if os.read(target, len(chunk)) != chunk:
                        return False
                return os.read(target, 1) == b""
            finally:
                os.close(target)
                os.lseek(source, 0, os.SEEK_SET)

        def merge(source, target, area, old_prefix="", new_prefix=""):
            for name in sorted(os.listdir(source)):
                info = os.stat(name, dir_fd=source, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode) or not (
                    stat.S_ISDIR(info.st_mode) or stat.S_ISREG(info.st_mode)
                ):
                    os.unlink(name, dir_fd=source)
                    continue
                old_path = "/".join(filter(None, (old_prefix, name)))
                new_name = name
                index = 0
                reserved = (
                area == "uploads" and not new_prefix and name.casefold() in self.RESERVED
                )
                if stat.S_ISDIR(info.st_mode):
                    while True:
                        try:
                            existing = os.stat(
                                new_name, dir_fd=target, follow_symlinks=False
                            )
                        except FileNotFoundError:
                            if not reserved:
                                os.mkdir(new_name, mode=0o700, dir_fd=target)
                                break
                        else:
                            if stat.S_ISDIR(existing.st_mode) and not reserved:
                                break
                        index += 1
                        new_name, reserved = suffix(name, index), False
                    child_source = os.open(
                        name,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd=source,
                    )
                    child_target = os.open(
                        new_name,
                        os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW,
                        dir_fd=target,
                    )
                    try:
                        merge(
                            child_source,
                            child_target,
                            area,
                            old_path,
                            "/".join(filter(None, (new_prefix, new_name))),
                        )
                    finally:
                        os.close(child_source)
                        os.close(child_target)
                    os.rmdir(name, dir_fd=source)
                    continue
                item = os.open(
                    name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=source
                )
                try:
                    before = os.fstat(item)
                    if not stat.S_ISREG(before.st_mode):
                        raise Conflict("旧会话文件在迁移期间发生变化")
                    previous = mapping.get((area, old_path))
                    if previous:
                        parts = journal_parts(previous[1], area)
                        parent = self._directory_fd(
                            self.area_root(user_id, project_id, area), parts[:-1]
                        )
                        try:
                            if not equal(item, parent, parts[-1]):
                                raise Conflict("旧会话迁移文件与记录不一致")
                        finally:
                            os.close(parent)
                    else:
                        while True:
                            if not reserved and equal(item, target, new_name):
                                break
                            try:
                                if reserved:
                                    raise FileExistsError()
                                out = os.open(
                                    new_name,
                                    os.O_WRONLY
                                    | os.O_CREAT
                                    | os.O_EXCL
                                    | os.O_NOFOLLOW,
                                    0o600,
                                    dir_fd=target,
                                )
                            except FileExistsError:
                                index += 1
                                new_name, reserved = suffix(name, index), False
                                continue
                            try:
                                with os.fdopen(out, "wb") as stream:
                                    while chunk := os.read(item, 1024 * 1024):
                                        stream.write(chunk)
                                    stream.flush()
                                    os.fsync(stream.fileno())
                                after = os.fstat(item)
                                if (before.st_size, before.st_mtime_ns) != (
                                    after.st_size,
                                    after.st_mtime_ns,
                                ):
                                    raise Conflict("旧会话文件在迁移期间发生变化")
                                os.utime(
                                    new_name,
                                    ns=(before.st_atime_ns, before.st_mtime_ns),
                                    dir_fd=target,
                                    follow_symlinks=False,
                                )
                                os.fsync(target)
                            except BaseException:
                                os.unlink(new_name, dir_fd=target)
                                raise
                            break
                        mapping[(area, old_path)] = (
                            area,
                            "/".join(filter(None, (new_prefix, new_name))),
                        )
                        save_mapping((area, old_path))
                    os.unlink(name, dir_fd=source)
                finally:
                    os.close(item)

        for area in ("uploads", "work", "artifacts"):
            legacy = self.user_root(user_id) / area
            try:
                parent = self._directory_fd(legacy, [])
            except NotFound:
                continue
            try:
                try:
                    info = os.stat(sid, dir_fd=parent, follow_symlinks=False)
                except FileNotFoundError:
                    continue
                if not stat.S_ISDIR(info.st_mode):
                    # A link is not a legacy tree and must never be followed.
                    continue
                source = os.open(
                    sid, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=parent
                )
                target = self._directory_fd(
                    self.area_root(user_id, project_id, area), [], create=True
                )
                try:
                    merge(source, target, area)
                finally:
                    os.close(source)
                    os.close(target)
                os.rmdir(sid, dir_fd=parent)
                os.fsync(parent)
            finally:
                os.close(parent)
        return mapping
