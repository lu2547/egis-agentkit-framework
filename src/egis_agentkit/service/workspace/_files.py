"""按项目和会话权限管理文件。

核心能力
- 列出、读取、上传和删除项目文件，按文件变化记录历史。
- 交付工作文件，查询会话产物，调用文档预览转换。
"""

import asyncio
from pathlib import Path

from egis_agentkit.errors import Forbidden


class WorkspaceService:
    def __init__(self, files, projects, sessions, file_changes, preview):
        self.files = files
        self.projects = projects
        self.sessions = sessions
        self.file_changes = file_changes
        self.preview = preview

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
            await self.file_changes.observe_safe(user.user_id, project_id)
            return await self.file_changes.list(project_id, cursor, limit)

    async def list_project_files(
        self, user, project_id, area="uploads", path="", recursive=False
    ):
        async with self.projects.guard(user, project_id):
            if area == "artifacts":
                await self.file_changes.observe_safe(
                    user.user_id, project_id, discover=True
                )
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
        session = await self.sessions.session(user, sid, require_use=False)
        project_id = session.state.middle_context["egis_agentkit"]["project_id"]
        async with self.projects.guard(user, project_id):
            return {
                "items": await self.file_changes.reply_artifacts(
                    user.user_id, project_id, sid
                )
            }

    async def read_project_file(self, user, project_id, area, path):
        async with self.projects.guard(user, project_id):
            return await asyncio.to_thread(
                self.files.read, user.user_id, project_id, area, path
            )

    async def delete_project_file(self, user, project_id, area, path):
        """先建立基线，删除文件后保存变更。"""
        async with self.projects.guard(user, project_id):
            # 自动交付通过删除记录识别已移除的产物。
            await self.file_changes.observe(user.user_id, project_id)
            async with self.file_changes.locked([project_id]):
                await asyncio.to_thread(
                    self.files.remove, user.user_id, project_id, area, path
                )
            await self.file_changes.observe(user.user_id, project_id)

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
            await self.file_changes.observe_safe(user.user_id, project_id)
            async with self.file_changes.locked([project_id]):
                target = await asyncio.to_thread(
                    self.files.write,
                    user.user_id,
                    project_id,
                    "/".join(filter(None, [path, filename])),
                    content,
                    area="uploads",
                    overwrite=overwrite,
                )
            await self.file_changes.observe_safe(user.user_id, project_id)
            return {
                **self._uploaded_file(
                    target, self.files.project_root(user.user_id, project_id), content
                ),
                "area": "uploads",
            }

    async def deliver_project_file(self, user, project_id, path):
        """交付所属项目的工作文件并记录变更。"""
        async with self.projects.guard(user, project_id):
            await self.file_changes.observe_safe(user.user_id, project_id)
            async with self.file_changes.locked([project_id]):
                target = await asyncio.to_thread(
                    self.files.deliver, user.user_id, project_id, path
                )
            await self.file_changes.observe_safe(user.user_id, project_id)
            return {
                "path": target.relative_to(
                    self.files.area_root(user.user_id, project_id, "artifacts")
                ).as_posix(),
                "area": "artifacts",
            }

    async def preview_project_document(self, user, project_id, area, path):
        async with self.projects.guard(user, project_id):
            content, _ = await asyncio.to_thread(
                self.files.read, user.user_id, project_id, area, path
            )
            return await self.preview.convert(content, path)
