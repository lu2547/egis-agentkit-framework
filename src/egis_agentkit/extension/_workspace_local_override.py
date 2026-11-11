"""覆盖本地 Workspace 的工具、资源和指令获取。"""

import asyncio

from agentscope.tool import Bash, Read
from agentscope.workspace import LocalWorkspace

from egis_agentkit.errors import AgentKitError, Forbidden
from egis_agentkit.extension._workspace_read_guard_middleware_impl import (
    ReadToolGuardMiddleware,
)


class ManagedLocalWorkspace(LocalWorkspace):
    def __init__(
        self, *, user_id, agent_id, session_id, project_id, files, resources, **kwargs
    ):
        self.egis_user_id, self.egis_agent_id, self.egis_session_id = (
            user_id,
            agent_id,
            session_id,
        )
        self.egis_project_id = project_id
        self.egis_files, self.egis_resources = files, resources
        self.egis_mcp_clients = {}
        self.egis_mcp_reconcile = asyncio.Lock()
        super().__init__(**kwargs)

    def current_session_id(self):
        from egis_agentkit.extension._session_lock_override import execution_scope

        scope = execution_scope.get()
        return (
            scope.session_id
            if scope and scope.user_id == self.egis_user_id
            else self.egis_session_id
        )

    def current_project_id(self):
        from egis_agentkit.extension._session_lock_override import execution_scope

        scope = execution_scope.get()
        return (
            scope.project_id
            if scope and scope.user_id == self.egis_user_id
            else self.egis_project_id
        )

    async def list_tools(self):
        tools = await super().list_tools()
        cwd = str(
            self.egis_files.area_root(
                self.egis_user_id, self.current_project_id(), "work"
            )
        )
        backend = self.get_backend()
        result = []
        for tool in tools:
            if tool.name == "Bash":
                tool = Bash(cwd=cwd, backend=backend)
            elif tool.name == "Read":
                tool = Read(
                    backend=backend, middlewares=[ReadToolGuardMiddleware(backend)]
                )
            result.append(tool)
        registry = getattr(self.egis_resources, "tool_registry", None)
        if registry is None:
            return result
        from egis_agentkit.extension._session_lock_override import execution_scope
        from egis_agentkit.service._user import UserService

        scope = execution_scope.get()
        if scope is None or scope.user_id != self.egis_user_id:
            return []
        user = await UserService(self.egis_resources.storage).get(scope.user_id)
        allowed = []
        for tool in result:
            try:
                await registry.authorize(user, scope, None, tool)
            except AgentKitError:
                continue
            allowed.append(tool)
        return allowed

    async def list_skills(self, *, agent_id=None):
        # The toolkit gets a live SkillLoader in policy middleware; avoid
        # eager native seeding/copies of every installed archive.
        return []

    async def list_mcps(self, *, agent_id=None, session_id=None):
        if self.egis_resources is None:
            return []
        from egis_agentkit.extension._session_lock_override import execution_scope
        from egis_agentkit.service._user import UserService

        scope = execution_scope.get()
        if not scope or (scope.user_id, scope.agent_id, scope.session_id) != (
            self.egis_user_id,
            agent_id or scope.agent_id,
            session_id or scope.session_id,
        ):
            raise Forbidden("MCP 装配缺少执行上下文")
        user = await UserService(self.egis_resources.storage).get(scope.user_id)
        refs = scope.definition["payload"].get("bindings", [])
        if scope.definition["agent_type"] == "personal":
            installed = await self.egis_resources.installations.assets(
                user, "mcp", enabled_only=True
            )
            refs = [
                {
                    key: row[key]
                    for key in ("resource_type", "resource_id", "resource_version")
                }
                for row in installed.values()
            ]
        declared = await self.egis_resources.mcps_for_refs(
            user,
            refs,
            authorization_scope=scope,
            skip_unavailable=True,
        )
        aid, sid = scope.agent_id, scope.session_id
        desired = {client.name: client for client in declared}
        async with self.egis_mcp_reconcile:
            previous = self.egis_mcp_clients.setdefault((aid, sid), {})
            for name in list(previous):
                if name not in desired:
                    obsolete = previous.pop(name)[1]
                    if obsolete.is_connected:
                        await obsolete.close()
            result = []
            for name, spec in desired.items():
                descriptor = spec.model_dump(mode="json")
                cached = previous.get(name)
                if (
                    cached is None
                    or cached[0] != descriptor
                    or cached[1].is_stateful
                    and not cached[1].is_connected
                ):
                    # Credential-bearing clients stay in memory. The native
                    # add_mcp method serializes its entire config to .mcp.
                    if cached is not None:
                        replaced = previous.pop(name)[1]
                        if replaced.is_connected:
                            await replaced.close()
                    try:
                        if spec.is_stateful:
                            await spec.connect()
                    except Exception:
                        continue
                    previous[name] = (descriptor, spec)
                result.append(previous[name][1])
            return result

    async def close(self):
        async with self.egis_mcp_reconcile:
            clients = [
                client
                for entries in self.egis_mcp_clients.values()
                for _, client in entries.values()
            ]
            self.egis_mcp_clients.clear()
            try:
                await asyncio.gather(
                    *(client.close() for client in clients), return_exceptions=True
                )
            finally:
                await super().close()

    async def get_instructions(self):
        base = await super().get_instructions()
        work = self.egis_files.area_root(
            self.egis_user_id, self.current_project_id(), "work"
        )
        artifacts = self.egis_files.area_root(
            self.egis_user_id, self.current_project_id(), "artifacts"
        )
        return f"{base}\n项目根目录（只读上传资料）：{work.parent}\n中间文件目录（.work/）：{work}\n交付目录（artifacts/）：{artifacts}\n文件工具使用绝对路径。上传资料直接从根目录读取；中间脚本放 .work/；完成的交付放 artifacts/，用户可以在右侧预览。"
