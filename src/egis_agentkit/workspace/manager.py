"""Native workspace lifecycle with user roots and project-specific shell cwd."""

import asyncio
import os
from pathlib import Path

from agentscope.app.workspace_manager import IsolationPolicy, WorkspaceManagerBase
from agentscope.tool import Bash
from agentscope.workspace import LocalWorkspace

from ..errors import Forbidden
from .files import WorkspaceFiles


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
        from ..services.assembly import execution_scope

        scope = execution_scope.get()
        return (
            scope.session_id
            if scope and scope.user_id == self.egis_user_id
            else self.egis_session_id
        )

    def current_project_id(self):
        from ..services.assembly import execution_scope

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
        return [
            Bash(cwd=cwd, backend=self.get_backend()) if tool.name == "Bash" else tool
            for tool in tools
        ]

    async def list_skills(self, *, agent_id=None):
        # The toolkit gets a live SkillLoader in policy middleware; avoid
        # eager native seeding/copies of every installed archive.
        return []

    async def list_mcps(self, *, agent_id=None, session_id=None):
        if self.egis_resources is None:
            return []
        from ..services.assembly import execution_scope
        from ..services.users import UserService

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
            installed = await self.egis_resources.list(user, "mcp")
            refs = [
                {
                    "resource_type": "mcp",
                    "resource_id": r["resource_id"],
                    "resource_version": r["resource_version"],
                    "resource_view_type": r["resource_view_type"],
                }
                for r in installed
                if r["is_installed"] and r["is_enabled"]
            ]
        declared = await self.egis_resources.mcps_for_refs(
            user, refs, authorization_scope=scope
        )
        aid, sid = scope.agent_id, scope.session_id
        desired = {client.name: client for client in declared}
        async with self.egis_mcp_reconcile:
            previous = self.egis_mcp_clients.setdefault((aid, sid), {})
            for name in list(previous):
                if name not in desired:
                    await super().remove_mcp(name, agent_id=aid, session_id=sid)
                    del previous[name]
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
                    # Only reconcile authorized declarations. Do not enumerate
                    # and connect arbitrary stale .mcp entries from disk.
                    await super().remove_mcp(name, agent_id=aid, session_id=sid)
                    live = await super().add_mcp(spec, agent_id=aid, session_id=sid)
                    previous[name] = (descriptor, live)
                result.append(previous[name][1])
            return result

    async def get_instructions(self):
        base = await super().get_instructions()
        work = self.egis_files.area_root(
            self.egis_user_id, self.current_project_id(), "work"
        )
        artifacts = self.egis_files.area_root(
            self.egis_user_id, self.current_project_id(), "artifacts"
        )
        return f"{base}\n项目根目录（只读上传资料）：{work.parent}\n中间文件目录（.work/）：{work}\n交付目录（artifacts/）：{artifacts}\n文件工具使用绝对路径。上传资料直接从根目录读取；中间脚本放 .work/；完成的交付放 artifacts/，用户可以在右侧预览。"


class EgisLocalWorkspaceManager(WorkspaceManagerBase):
    def __init__(self, workspace_root, resources=None):
        super().__init__(isolation=IsolationPolicy.PER_USER)
        self.files = WorkspaceFiles(Path(workspace_root))
        self.resources = resources
        self._workspaces = {}
        self._lock = asyncio.Lock()

    async def get_workspace(self, user_id, agent_id, session_id, workspace_id=None):
        from ..services.assembly import execution_scope, execution_setup_error

        error = execution_setup_error.get()
        if error is not None:
            raise error
        scope = execution_scope.get()
        project_id = (
            scope.project_id
            if scope and scope.user_id == user_id
            else workspace_id or session_id
        )
        # One native workspace object owns a user's .mcp file. Native agent /
        # session partitions still isolate live MCP clients and offload files.
        key = user_id
        async with self._lock:
            for area in ("work", "artifacts", "uploads"):
                fd = self.files._directory_fd(
                    self.files.area_root(user_id, project_id, area), [], create=True
                )
                os.close(fd)
            if key not in self._workspaces:
                root = self.files.user_root(user_id)
                for name in ("data", "skills", "sessions", "memory", "preferences"):
                    (root / name).mkdir(parents=True, exist_ok=True)
                ws = ManagedLocalWorkspace(
                    user_id=user_id,
                    agent_id=agent_id,
                    session_id=session_id,
                    project_id=project_id,
                    files=self.files,
                    resources=self.resources,
                    workdir=str(root),
                    workspace_id=project_id,
                )
                await ws.initialize()
                self._workspaces[key] = ws
            return self._workspaces[key]

    async def close(self, workspace_id):
        async with self._lock:
            entries = [
                (key, ws)
                for key, ws in self._workspaces.items()
                if ws.workspace_id == workspace_id
            ]
            for key, _ in entries:
                self._workspaces.pop(key)
        for _, ws in entries:
            await ws.close()

    async def close_all(self):
        async with self._lock:
            entries, self._workspaces = list(self._workspaces.values()), {}
        await asyncio.gather(*(ws.close() for ws in entries))
