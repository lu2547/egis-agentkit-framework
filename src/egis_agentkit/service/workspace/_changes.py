"""扫描文件指纹并记录项目变更。

核心能力
- 比较文件内容和大小，保存扫描基线和可分页的历史。
- 追踪单次回复实际生成的产物并保留会话关联。
"""

import asyncio
import hashlib
import logging
import os
import stat
from contextlib import asynccontextmanager

from egis_agentkit.backend.nacos._settings import (
    WorkspaceChangesConfig,
    WorkspaceScanConfig,
)
from egis_agentkit.errors import Conflict, InvalidRequest, NotFound
from egis_agentkit.storage._workspace import WorkspaceFiles


class FileChangeService:
    def __init__(self, storage, root, *, files=None, config_provider=None):
        self.storage = storage
        self.config = config_provider
        self.files = (
            files
            if files is not None
            else WorkspaceFiles(root, config_provider=config_provider)
        )

    def locked(self, project_ids):
        """串行修改项目文件，并共享变更记录使用的锁。"""
        return self.storage.workspace.locked(project_ids)

    def scan(self, user_id, project_id, limits=None):
        """按同一份扫描限额读取字节并计算文件指纹。"""
        if limits is None:
            limits = (
                self.config.snapshot.workspace.scan
                if self.config
                else WorkspaceScanConfig()
            )
        result = {}
        count = total = 0

        def walk(fd, area, prefix, depth):
            nonlocal count, total
            if depth > limits.max_depth:
                raise Conflict("文件目录过深，无法完整记录变更")
            for name in os.listdir(fd):
                count += 1
                if count > limits.max_entries:
                    raise Conflict("文件数量超过变更记录扫描限制")
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    continue
                path = f"{prefix}/{name}" if prefix else name
                if len(path.encode("utf-8")) > limits.max_path_bytes:
                    raise Conflict("文件路径过长，无法完整记录变更")
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(
                        name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd
                    )
                    try:
                        if (
                            area == "uploads"
                            and not prefix
                            and name.casefold() in self.files.RESERVED
                        ):
                            walk(
                                child,
                                "work" if name.casefold() == ".work" else "artifacts",
                                "",
                                depth + 1,
                            )
                        else:
                            walk(child, area, path, depth + 1)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(info.st_mode):
                    if (
                        area == "uploads"
                        and not prefix
                        and name.casefold() in self.files.RESERVED
                    ):
                        continue
                    total += info.st_size
                    if total > limits.max_total_bytes:
                        raise Conflict("文件数量或大小超过变更记录扫描限制")
                    item = os.open(
                        name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd
                    )
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
            fd = self.files._directory_fd(
                self.files.project_root(user_id, project_id), []
            )
        except NotFound:
            return result
        try:
            walk(fd, "uploads", "", 0)
        finally:
            os.close(fd)
        return result

    @asynccontextmanager
    async def track_deliveries(self, user_id, project_id, session_id, reply_id):
        """在项目文件锁内比较工具执行前后的快照并记录本次交付。"""
        settings = self.config.snapshot.workspace if self.config else None
        limits = settings.scan if settings else WorkspaceScanConfig()
        failure = None
        async with self.locked([project_id]) as workspace:
            try:
                before = await asyncio.to_thread(self.scan, user_id, project_id, limits)
            except Exception:
                logging.getLogger(__name__).exception(
                    "Could not snapshot reply deliveries"
                )
                before = None
            try:
                yield
            except BaseException as exc:
                # 记录已写入的文件后，将工具执行的失败或取消传回调用方。
                failure = exc
            finally:
                if before is not None:
                    try:
                        after = await asyncio.to_thread(
                            self.scan, user_id, project_id, limits
                        )
                        changed_work = {
                            path
                            for (area, path), fingerprint in after.items()
                            if area == "work"
                            and before.get((area, path)) != fingerprint
                        }
                        await asyncio.to_thread(
                            self.files.discover_deliveries,
                            user_id,
                            project_id,
                            eligible_paths=changed_work,
                            deleted_after=await workspace.deleted_artifacts(project_id),
                            settings=settings,
                        )
                        current = await asyncio.to_thread(
                            self.scan, user_id, project_id, limits
                        )
                        rows = [
                            dict(
                                project_id=project_id,
                                session_id=session_id,
                                reply_id=reply_id,
                                path=path,
                                digest=fingerprint[0],
                            )
                            for (area, path), fingerprint in current.items()
                            if area == "artifacts"
                            and before.get((area, path)) != fingerprint
                        ]
                        if rows and reply_id:
                            await workspace.save_deliveries(rows)
                    except Exception:
                        logging.getLogger(__name__).exception(
                            "Could not record reply deliveries"
                        )
        if failure is not None:
            raise failure

    async def reply_artifacts(self, user_id, project_id, session_id):
        """读取仍在项目目录中的该会话回复产物。"""
        rows = await self.storage.workspace.reply_artifacts(project_id, session_id)
        files = {
            item["path"]: item
            for item in await asyncio.to_thread(
                self.files.list,
                user_id,
                project_id,
                "artifacts",
                recursive=True,
            )
            if not item["is_directory"]
        }
        return [
            dict(files[row["path"]], area="artifacts", reply_id=row["reply_id"])
            for row in rows
            if row["path"] in files
        ]

    async def observe(self, user_id, project_id, *, discover=False):
        """在同一把文件锁内扫描、比较并保存文件变更。"""
        settings = self.config.snapshot.workspace if self.config else None
        limits = settings.scan if settings else WorkspaceScanConfig()
        async with self.locked([project_id]) as workspace:
            # 已删除项目的迟到扫描不能重建指纹或变更记录。
            baseline = await workspace.baseline_exists(user_id, project_id)
            if baseline is None:
                return
            if discover:
                await asyncio.to_thread(
                    self.files.discover_deliveries,
                    user_id,
                    project_id,
                    deleted_after=await workspace.deleted_artifacts(project_id),
                    settings=settings,
                )
            current = await asyncio.to_thread(self.scan, user_id, project_id, limits)
            changes = []
            if baseline:
                previous = await workspace.fingerprints(project_id)
                for key in sorted(current.keys() | previous.keys()):
                    old, new = previous.get(key), current.get(key)
                    if old == new:
                        continue
                    changes.append(
                        dict(
                            project_id=project_id,
                            area=key[0],
                            path=key[1],
                            change_type="created"
                            if old is None
                            else "deleted"
                            if new is None
                            else "modified",
                            size=new[1] if new else None,
                            previous_size=old[1] if old else None,
                        )
                    )
            await workspace.save_observation(user_id, project_id, current, changes)

    async def observe_safe(self, user_id, project_id, *, discover=False):
        try:
            await self.observe(user_id, project_id, discover=discover)
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "File change scan failed for project %s", project_id, exc_info=True
            )
            message = (
                str(exc) if isinstance(exc, Conflict) else "文件扫描失败，请稍后重试"
            )
            try:
                async with self.locked([project_id]) as workspace:
                    await workspace.record_scan_error(user_id, project_id, message)
            except Exception:
                logging.getLogger(__name__).exception(
                    "Could not persist file scan status"
                )

    async def list(self, project_id, cursor=None, limit=None):
        """返回项目变更页和最近一次扫描错误。"""
        settings = (
            self.config.snapshot.workspace.changes
            if self.config
            else WorkspaceChangesConfig()
        )
        limit = settings.default_page_size if limit is None else limit
        if not 1 <= limit <= settings.max_page_size:
            raise InvalidRequest(f"文件变更每页条数应为 1 到 {settings.max_page_size}")
        rows, error = await self.storage.workspace.list_changes(
            project_id, cursor, limit
        )
        items = [dict(row) for row in rows[:limit]]
        for item in items:
            item.pop("project_id")
        return {
            "items": items,
            "next_cursor": items[-1]["change_id"] if len(rows) > limit else None,
            "scan_error": error,
        }
