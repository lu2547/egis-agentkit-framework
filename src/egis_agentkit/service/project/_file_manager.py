"""管理项目文件操作、扫描基线、变化流水、交付和回复产物。

核心能力
- 页面文件操作校验项目范围；物理文件与记录交给 Storage。
- 工具期间记录会话/回复归属，在 REPLY_END 前汇总产物。
- 预览由 DocumentPreviewService 执行。
"""

import asyncio
import hashlib
import logging
import os
import stat
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any

from agentscope.event import ReplyEndEvent
from agentscope.tool import ToolResponse

from egis_agentkit.client.nacos._settings import (
    ProjectChangesConfig,
    ProjectContextConfig,
    ProjectScanConfig,
    ProjectWorkbookConfig,
)
from egis_agentkit.errors import Conflict, Forbidden, InvalidRequest, NotFound
from egis_agentkit.service.project._document_preview import DocumentPreviewService
from egis_agentkit.service.project._project_manager import ProjectManager
from egis_agentkit.storage.model import User
from egis_agentkit.storage.project._files import ProjectFileStorage


class ProjectFileManager:
    """统一管理项目文件、变化记录和回复产物，所有 SQL 与文件 IO 通过 Storage。"""

    def __init__(
        self,
        storage,
        root,
        *,
        files=None,
        projects=None,
        sessions=None,
        preview=None,
        config_provider=None,
    ):
        self.storage = storage
        self.config = config_provider
        self.files = (
            files
            if files is not None
            else ProjectFileStorage(root, config_provider=config_provider)
        )
        self.projects = (
            projects
            if projects is not None
            else ProjectManager(
                storage, root, files=self.files, config_provider=config_provider
            )
        )
        self.sessions = sessions
        self.preview = (
            preview
            if preview is not None
            else DocumentPreviewService(config_provider=config_provider)
        )

    async def get_preview_options(self, user, project_id) -> dict:
        """校验项目访问权后返回文件引用限制；不暴露连接配置和密钥。"""
        async with self.projects.guard(user, project_id):
            return self.preview.get_config().reference.model_dump(by_alias=True)

    @property
    def max_upload_bytes(self):
        return self.files.max_bytes

    @staticmethod
    def _uploaded_file(target, root, content):
        return {
            "name": target.name,
            "path": target.relative_to(root).as_posix(),
            "is_directory": False,
            "size": len(content),
            "modified_at": target.stat().st_mtime,
        }

    async def project_file_changes(self, user, project_id, cursor=None, limit=None):
        """观察所属项目后返回文件变更页。"""
        async with self.projects.guard(user, project_id):
            await self.observe_safe(user.user_id, project_id)
            return await self.list_changes(project_id, cursor, limit)

    async def list_project_files(
        self, user, project_id, area="uploads", path="", recursive=False
    ):
        async with self.projects.guard(user, project_id):
            if area == "artifacts":
                await self.observe_safe(user.user_id, project_id, discover=True)
            items = await asyncio.to_thread(
                self.files.list,
                user.user_id,
                project_id,
                area,
                path,
                recursive=recursive,
            )
            return {
                "items": [{**item, "area": area} for item in items],
                "area": area,
                "path": path,
            }

    async def session_artifacts(self, user, sid):
        """按会话所有者返回仍存在的回复产物。"""
        session = await self.sessions.get_session_record(user, sid, require_use=False)
        project_id = session.state.middle_context["egis_agentkit"]["project_id"]
        async with self.projects.guard(user, project_id):
            return {"items": await self.reply_artifacts(user.user_id, project_id, sid)}

    async def read_project_file(self, user, project_id, area, path):
        async with self.projects.guard(user, project_id):
            return await asyncio.to_thread(
                self.files.read, user.user_id, project_id, area, path
            )

    async def update_project_file(
        self,
        user: User,
        project_id: str,
        area: str,
        path: str,
        content: bytes,
        checksum: str,
    ) -> dict[str, Any]:
        """保存页面编辑结果：校验归属、检测文件冲突、替换文件并记录变更。"""
        if len(checksum) != hashlib.sha256().digest_size * 2 or any(
            char not in "0123456789abcdef" for char in checksum
        ):
            raise InvalidRequest("保存必须携带打开文件时的 SHA-256 摘要")
        # 1. 项目管理校验用户归属，建立保存前的变化基线。
        async with self.projects.guard(user, project_id):
            await self.observe_safe(user.user_id, project_id)
            # 2. Storage 在锁内检测旧摘要，拒绝覆盖其他编辑者的新内容。
            async with self.locked([project_id]):
                target = await asyncio.to_thread(
                    self.files.update_file,
                    user.user_id,
                    project_id,
                    area,
                    path,
                    content,
                    checksum,
                )
            # 3. 页面编辑不归属于任何 Agent 回复，只记录普通文件变更。
            await self.observe_safe(user.user_id, project_id)
            return {
                **self._uploaded_file(
                    target,
                    self.files.area_root(user.user_id, project_id, area),
                    content,
                ),
                "area": area,
                "checksum": hashlib.sha256(content).hexdigest(),
            }

    async def get_workbook_options(self, user: User, project_id: str) -> dict[str, Any]:
        """返回工作簿编辑限制，不包含服务端密钥。"""
        async with self.projects.guard(user, project_id):
            config = (
                self.config.snapshot.project.workbook
                if self.config
                else ProjectWorkbookConfig()
            )
            return config.model_dump(by_alias=True)

    async def delete_project_file(self, user, project_id, area, path):
        """先建立基线，删除文件后保存变更。"""
        async with self.projects.guard(user, project_id):
            # 自动交付通过删除记录识别已移除的产物。
            await self.observe(user.user_id, project_id)
            async with self.locked([project_id]):
                await asyncio.to_thread(
                    self.files.remove, user.user_id, project_id, area, path
                )
            await self.observe(user.user_id, project_id)

    async def upload_project_file(
        self,
        user,
        project_id,
        filename,
        content: bytes,
        area="uploads",
        path="",
        *,
        overwrite=False,
    ):
        """校验项目范围，在文件锁内写入并观察变更。"""
        if area != "uploads":
            raise Forbidden("上传只能保存到项目根目录")
        await self.projects.get(user, project_id)
        filename = Path(filename or "upload").name
        async with self.projects.guard(user, project_id):
            await self.observe_safe(user.user_id, project_id)
            async with self.locked([project_id]):
                target = await asyncio.to_thread(
                    self.files.write,
                    user.user_id,
                    project_id,
                    "/".join(filter(None, [path, filename])),
                    content,
                    area="uploads",
                    overwrite=overwrite,
                )
            await self.observe_safe(user.user_id, project_id)
            return {
                **self._uploaded_file(
                    target, self.files.project_root(user.user_id, project_id), content
                ),
                "area": "uploads",
            }

    async def deliver_project_file(self, user, project_id, path):
        """交付所属项目的工作文件并记录变更。"""
        async with self.projects.guard(user, project_id):
            await self.observe_safe(user.user_id, project_id)
            async with self.locked([project_id]):
                target = await asyncio.to_thread(
                    self.files.deliver, user.user_id, project_id, path
                )
            await self.observe_safe(user.user_id, project_id)
            return {
                "path": target.relative_to(
                    self.files.area_root(user.user_id, project_id, "artifacts")
                ).as_posix(),
                "area": "artifacts",
            }

    async def preview_project_document(
        self, user, project_id, area, path, *, checksum=None, page=None
    ):
        async with self.projects.guard(user, project_id):
            content, _ = await asyncio.to_thread(
                self.files.read, user.user_id, project_id, area, path
            )
            if checksum is not None and hashlib.sha256(content).hexdigest() != checksum:
                raise Conflict("源文件已更新，请重新打开后预览")
        # 原件已在项目生命周期锁内读取并校验；转换不再占用项目数据库连接。
        return await self.preview.convert(
            content, path, **({"page": page} if page is not None else {})
        )

    def locked(self, project_ids):
        """串行修改项目文件，并共享变更记录使用的锁。"""
        return self.storage.project_files.locked(project_ids)

    def scan(self, user_id, project_id, limits=None):
        """按同一份扫描限额读取字节并计算文件指纹。"""
        if limits is None:
            limits = (
                self.config.snapshot.project.scan
                if self.config
                else ProjectScanConfig()
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
    async def track_changes(self, user_id, project_id, session_id, reply_id):
        """锁内记录工具实际新增、修改、删除的文件及所属会话；此时不生成产物清单。"""
        settings = self.config.snapshot.project if self.config else None
        limits = settings.scan if settings else ProjectScanConfig()
        failure = None
        async with self.locked([project_id]) as workspace:
            try:
                before = await asyncio.to_thread(self.scan, user_id, project_id, limits)
                # 先收录工具开始前已有的变化，不把其他会话或人工操作归给当前工具。
                baseline = await workspace.baseline_exists(user_id, project_id)
                if baseline is None:
                    raise NotFound("项目不存在")
                previous = (
                    await workspace.fingerprints(project_id) if baseline else before
                )
                await workspace.save_observation(
                    user_id,
                    project_id,
                    before,
                    self._make_changes(project_id, previous, before),
                )
            except Exception:
                logging.getLogger(__name__).exception(
                    "Could not snapshot session file changes"
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
                        changes = self._make_changes(
                            project_id,
                            before,
                            current,
                            session_id=session_id,
                            reply_id=reply_id,
                        )
                        await workspace.save_observation(
                            user_id, project_id, current, changes
                        )
                    except Exception:
                        logging.getLogger(__name__).exception(
                            "Could not record session file changes"
                        )
        if failure is not None:
            raise failure

    @staticmethod
    def _make_changes(project_id, previous, current, *, session_id=None, reply_id=None):
        """统一构造变更流水；普通扫描保留空归属，只有实际执行的工具可指定会话。"""
        changes = []
        for area, path in sorted(current.keys() | previous.keys()):
            old, new = previous.get((area, path)), current.get((area, path))
            if old == new:
                continue
            changes.append(
                dict(
                    project_id=project_id,
                    session_id=session_id,
                    reply_id=reply_id,
                    area=area,
                    path=path,
                    change_type="created"
                    if old is None
                    else "deleted"
                    if new is None
                    else "modified",
                    size=new[1] if new else None,
                    previous_size=old[1] if old else None,
                )
            )
        return changes

    async def finalize_reply(
        self, user_id: str, project_id: str, session_id: str, reply_id: str
    ):
        """在结束事件发出前同步最终文件状态，并按会话与回复汇总一次产物。"""
        await self.observe_safe(user_id, project_id)
        async with self.locked([project_id]) as workspace:
            if await workspace.baseline_exists(user_id, project_id) is not None:
                await workspace.collect_reply_artifacts(
                    project_id, session_id, reply_id
                )

    async def reply_artifacts(self, user_id, project_id, session_id):
        """读取仍在项目目录中的该会话回复产物。"""
        rows = await self.storage.project_files.reply_artifacts(project_id, session_id)
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
        settings = self.config.snapshot.project if self.config else None
        limits = settings.scan if settings else ProjectScanConfig()
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
            previous = await workspace.fingerprints(project_id) if baseline else current
            changes = self._make_changes(project_id, previous, current)
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

    async def list_changes(self, project_id, cursor=None, limit=None):
        """返回项目变更页和最近一次扫描错误。"""
        settings = (
            self.config.snapshot.project.changes
            if self.config
            else ProjectChangesConfig()
        )
        limit = settings.default_page_size if limit is None else limit
        if not 1 <= limit <= settings.max_page_size:
            raise InvalidRequest(f"文件变更每页条数应为 1 到 {settings.max_page_size}")
        rows, error = await self.storage.project_files.list_changes(
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

    async def on_reply(self, scope, agent, input_kwargs, next_handler):
        """在回复结束事件可见前汇总本会话产物，异常退出仍观察文件变化。"""
        await self.observe_safe(scope.user_id, scope.project_id)
        try:
            async for event in next_handler(**input_kwargs):
                if isinstance(event, ReplyEndEvent):
                    await self.finalize_reply(
                        scope.user_id,
                        scope.project_id,
                        scope.session_id,
                        event.reply_id,
                    )
                yield event
        finally:
            await self.observe_safe(scope.user_id, scope.project_id)

    async def on_acting(self, scope, agent, input_kwargs, next_handler):
        """在实际写文件的工具执行期间记录归属，先提交事务再发送最终结果。"""
        # 1. 非写入工具直接放行，团队消息等等待不能持有项目文件锁。
        tool = input_kwargs.get("tool_call")
        if tool is None or tool.name not in self._change_settings().writing_tools:
            async for event in next_handler(**input_kwargs):
                yield event
            return
        await self.observe_safe(scope.user_id, scope.project_id)
        response = None
        try:
            async with self.track_changes(
                scope.user_id,
                scope.project_id,
                scope.session_id,
                agent.state.reply_id,
            ):
                async for event in next_handler(**input_kwargs):
                    # 原生卸载层收到 ToolResponse 会取消尾部任务；先完成变更事务。
                    if isinstance(event, ToolResponse):
                        response = event
                    else:
                        yield event
        finally:
            await self.observe_safe(scope.user_id, scope.project_id)
        if response is not None:
            yield response

    def context_inputs(self, workdir):
        """有界枚举项目可见输入，忽略内部目录和符号链接。"""
        limits = (
            self.config.snapshot.project.context
            if self.config
            else ProjectContextConfig()
        )
        result = []
        root = workdir.parent
        if root.is_symlink():
            return result
        for index, (directory, dirs, names) in enumerate(
            os.walk(root, followlinks=False)
        ):
            dirs[:] = sorted(
                name
                for name in dirs
                if not name.startswith(".")
                and not (Path(directory) == root and name == "artifacts")
                and not (Path(directory) / name).is_symlink()
            )
            if index >= limits.max_directories:
                break
            for name in sorted(names):
                path = Path(directory) / name
                if not path.is_symlink() and path.is_file():
                    result.append(str(path))
                if len(result) >= limits.max_files:
                    return result
        return result

    def _change_settings(self):
        """一次工具回调使用同一份已校验的变更配置。"""
        return (
            self.config.snapshot.project.changes
            if self.config
            else ProjectChangesConfig()
        )
