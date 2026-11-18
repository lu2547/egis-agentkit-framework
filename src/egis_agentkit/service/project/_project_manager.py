"""管理项目生命周期与所有权。

核心能力
- 并发初始化默认项目，查询和重命名用户拥有的项目。
- 锁定项目和所属会话，协调原生会话、工作目录和文件记录清理。
"""

import asyncio
from contextlib import asynccontextmanager

from agentscope.app.message_bus import MessageBusKeys

from egis_agentkit.core.project._project import Project
from egis_agentkit.errors import Conflict, NotFound
from egis_agentkit.storage.project._files import ProjectFileStorage


async def purge_session_bus(bus, sid):
    """清理原生会话在消息总线上的投递和回放数据。"""
    await bus.log_trim(MessageBusKeys.session_events(sid))
    await bus.queue_delete(MessageBusKeys.inbox(sid))
    for namespace in (
        MessageBusKeys.inbox_consumer(sid),
        MessageBusKeys.bg_tasks(sid),
        MessageBusKeys.session_event_checkpoint(sid),
        MessageBusKeys.projection_namespace(sid),
    ):
        await bus.registry_drop(namespace)


class ProjectManager:
    """创建、查询和清理用户拥有的项目。"""

    def __init__(
        self,
        storage,
        workspace_root,
        *,
        files: ProjectFileStorage | None = None,
        config_provider=None,
        workspaces=None,
    ):
        self.storage = storage
        self.config = config_provider
        self.workspaces = workspaces
        self.project = Project(config=config_provider)
        self.files = (
            files
            if files is not None
            else ProjectFileStorage(workspace_root, config_provider=config_provider)
        )

    def _defaults(self):
        """读取项目的当前默认配置。"""
        return self.project.defaults()

    def _require(self, project, allow_deleting=False):
        """管理流程复用项目自身的生命周期规则。"""
        return self.project.require_available(project, allow_deleting=allow_deleting)

    async def get(self, user, project_id, *, allow_deleting=False):
        """校验项目归属和状态。"""
        return self._require(
            await self.storage.projects.get(user.user_id, project_id), allow_deleting
        )

    @asynccontextmanager
    async def guard(self, user, project_id):
        """在项目删除开始前保留会话或文件操作所需的项目。"""
        async with self.storage.projects.lifetime(project_id) as projects:
            yield self._require(await projects.get(user.user_id, project_id))

    def initialize_files(self, user_id, project_id):
        """初始化项目目录。"""
        self.files.initialize_project(user_id, project_id)

    async def create(self, user, project_name):
        """创建用户项目并初始化目录。"""
        name = project_name
        async with self.storage.projects.write() as projects:
            return await self._create(projects, user.user_id, name, is_default=False)

    async def _create(self, projects, user_id, name, *, is_default):
        project = await projects.create(
            self.project.build(user_id, name, is_default=is_default)
        )
        await asyncio.to_thread(self.initialize_files, user_id, project["id"])
        return project

    async def default(self, user):
        """并发调用复用该用户的同一个默认项目。"""
        defaults = self._defaults()
        async with self.storage.projects.lock_default(user.user_id) as projects:
            project = await projects.get_default(user.user_id)
            if project:
                if project["status_type"] != "active":
                    raise Conflict("默认项目正在删除，请选择其他项目")
                return project
            return await self._create(
                projects,
                user.user_id,
                defaults.name,
                is_default=True,
            )

    async def rename(self, user, project_id, project_name):
        """更新处于活动状态的所属项目。"""
        async with self.storage.projects.lifetime(project_id) as projects:
            project = self._require(await projects.get(user.user_id, project_id))
            name = self.project.validate_name(
                project_name, is_default=project["is_default"]
            )
            await projects.rename(user.user_id, project_id, name)
        return await self.get(user, project_id)

    async def session_project(self, user, session):
        """从原生会话绑定读取项目，并校验当前项目归属。"""
        binding = session.state.middle_context.get("egis_agentkit", {})
        project_id = binding.get("project_id")
        if not project_id:
            raise NotFound("会话未绑定项目")
        return await self.get(user, project_id)

    async def list(self, user):
        """按创建时间列出用户拥有的项目。"""
        return await self.storage.projects.list(user.user_id)

    async def delete(self, user, project_id, bus):
        """独占项目和会话锁，允许从清理失败的状态重试。"""
        user_id = user.user_id
        await self.get(user, project_id, allow_deleting=True)
        async with self.storage.projects.lifetime(project_id, exclusive=True) as guard:
            if guard is None:
                raise Conflict("项目中有正在执行的操作，请停止后再删除")
            # 默认项目的删除限制属于项目自身规则，校验通过后才清理会话或沙箱。
            self.project.require_deletable(await guard.get(user_id, project_id))
            rows = await self.storage.sessions.in_project(user_id, project_id)
            held = []
            try:
                for row in rows:
                    key = MessageBusKeys.session_lock(row.id)
                    if not await bus.try_lock(key):
                        raise Conflict("项目中有对话正在运行，请先停止生成")
                    held.append(key)
                await self.storage.projects.mark_deleting(user_id, project_id)
                await self._cleanup(user_id, project_id, rows, bus)
            finally:
                for key in reversed(held):
                    await bus.unlock(key)

    async def _cleanup(self, user_id, project_id, rows, bus):
        """保留可重试的会话归属，直到项目和文件记录一起提交。"""
        from egis_agentkit.service.team._collaboration import collaborators

        # 1. 先释放远程运行实例及其目录挂载，失败时保留项目记录以便重试。
        if self.workspaces is not None:
            await self.workspaces.destroy_workspaces(user_id, project_id)

        # 原生删除只清理借用专家的会话，借用的 Agent 由主会话持有。
        owned_projections = set()
        for row in rows:
            record = await self.storage.read_session_unscoped(
                user_id, row.agent_id, row.id
            )
            if record is not None:
                binding = record.state.middle_context.get("egis_agentkit", {})
                owned_projections.update(
                    member["native_agent_id"] for member in collaborators(binding)
                )

        async with self.storage.project_cleanup(project_id) as (
            projects,
            sessions,
            file_records,
        ):
            await sessions.delete_sessions(user_id, rows, owned_projections)
            await file_records.delete_project_records(project_id)
            await projects.delete(user_id, project_id)
            await asyncio.to_thread(self.files.remove_project, user_id, project_id)
            for row in rows:
                await purge_session_bus(bus, row.id)

    async def on_system_prompt(self, scope, files, agent, current_prompt):
        """取得项目输入清单，再由 Core Project 组织本轮项目上下文。"""
        paths = await asyncio.to_thread(
            files.context_inputs, scope.user_id, scope.project_id
        )
        return self.project.build_context(
            scope.workdir, scope.artifacts_dir, paths, current_prompt
        )
