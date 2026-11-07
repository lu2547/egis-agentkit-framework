"""Workspace file use cases independent of HTTP requests and responses."""

import asyncio
from pathlib import Path

from egis_agentkit.errors import Forbidden


class WorkspaceService:
    def __init__(self, files, projects, tasks, file_changes, preview):
        self.files = files
        self.projects = projects
        self.tasks = tasks
        self.file_changes = file_changes
        self.preview = preview

    @property
    def max_upload_bytes(self):
        return self.files.max_bytes

    async def _session_project_id(self, user, sid):
        session = await self.tasks.session(user, sid, require_use=False)
        return session.state.middle_context["egis_agentkit"]["project_id"]

    @staticmethod
    def _uploaded_file(target, root, content):
        return {
            "name": target.name,
            "path": target.relative_to(root).as_posix(),
            "is_directory": False,
            "size": len(content),
            "modified_at": target.stat().st_mtime,
        }

    async def list_user_files(self, user, path=""):
        return {
            "items": await asyncio.to_thread(self.files.list_user, user.user_id, path),
            "path": path,
        }

    async def read_user_file(self, user, path):
        return await asyncio.to_thread(self.files.read_user, user.user_id, path)

    async def upload_user_file(self, user, filename, content: bytes, path=""):
        parts = self.files._parts(path)
        if parts and parts[0] in ("work", "artifacts"):
            raise Forbidden("上传只能保存到 uploads")
        if parts and parts[0] == "uploads":
            parts = parts[1:]
        filename = Path(filename or "upload").name
        target = await asyncio.to_thread(
            self.files.write_user,
            user.user_id,
            "/".join(filter(None, ["uploads", *parts, filename])),
            content,
        )
        return self._uploaded_file(target, self.files.user_root(user.user_id), content)

    async def preview_user_document(self, user, path):
        content, _ = await self.read_user_file(user, path)
        return await self.preview.convert(content, path)

    async def project_file_changes(self, user, project_id, cursor=None, limit=50):
        async with self.projects.guard(user, project_id):
            await self.file_changes.observe_safe(user.user_id, project_id)
            return await self.file_changes.list(project_id, cursor, limit)

    async def session_file_changes(self, user, sid, cursor=None, limit=50):
        return await self.project_file_changes(
            user, await self._session_project_id(user, sid), cursor, limit
        )

    async def list_project_files(
        self, user, project_id, area="uploads", path="", recursive=False
    ):
        async with self.projects.guard(user, project_id):
            if area == "artifacts":
                await self.file_changes.reconcile_deliveries(user.user_id, project_id)
                await self.file_changes.observe_safe(
                    user.user_id, project_id, discover=True
                )
            items = await asyncio.to_thread(
                self.files.list, user.user_id, project_id, area, path, recursive=recursive
            )
            return {
                "items": [{**item, "area": area} for item in items],
                "area": area,
                "path": path,
            }

    async def list_session_files(
        self, user, sid, area="uploads", path="", recursive=False
    ):
        return await self.list_project_files(
            user, await self._session_project_id(user, sid), area, path, recursive
        )

    async def session_artifacts(self, user, sid):
        project_id = await self._session_project_id(user, sid)
        async with self.projects.guard(user, project_id):
            return {"items": await self.file_changes.reply_artifacts(
                user.user_id, project_id, sid
            )}

    async def read_project_file(self, user, project_id, area, path):
        async with self.projects.guard(user, project_id):
            return await asyncio.to_thread(
                self.files.read, user.user_id, project_id, area, path
            )

    async def read_session_file(self, user, sid, area, path):
        return await self.read_project_file(
            user, await self._session_project_id(user, sid), area, path
        )

    async def delete_project_file(self, user, project_id, area, path):
        async with self.projects.guard(user, project_id):
            # A committed deletion observation also prevents delivery discovery
            # from silently copying an unchanged .work source back to artifacts.
            await self.file_changes.observe(user.user_id, project_id)
            async with self.file_changes.locked([project_id]):
                await asyncio.to_thread(
                    self.files.remove, user.user_id, project_id, area, path
                )
            await self.file_changes.observe(user.user_id, project_id)

    async def delete_session_file(self, user, sid, area, path):
        await self.delete_project_file(
            user, await self._session_project_id(user, sid), area, path
        )

    async def upload_project_file(
        self, user, project_id, filename, content: bytes, area="uploads", path=""
    ):
        if area not in ("uploads", "work"):
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
                )
            await self.file_changes.observe_safe(user.user_id, project_id)
            return {
                **self._uploaded_file(
                    target, self.files.project_root(user.user_id, project_id), content
                ),
                "area": "uploads",
            }

    async def upload_session_file(
        self, user, sid, filename, content: bytes, area="uploads", path=""
    ):
        return await self.upload_project_file(
            user, await self._session_project_id(user, sid), filename, content, area, path
        )

    async def deliver_project_file(self, user, project_id, path):
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

    async def deliver_session_file(self, user, sid, path):
        return await self.deliver_project_file(
            user, await self._session_project_id(user, sid), path
        )

    async def preview_project_document(self, user, project_id, area, path):
        async with self.projects.guard(user, project_id):
            content, _ = await asyncio.to_thread(
                self.files.read, user.user_id, project_id, area, path
            )
            return await self.preview.convert(content, path)

    async def preview_session_document(self, user, sid, area, path):
        return await self.preview_project_document(
            user, await self._session_project_id(user, sid), area, path
        )
