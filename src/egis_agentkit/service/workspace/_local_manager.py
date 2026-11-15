"""实现 WorkspaceManagerBase，为会话取得对应项目的工作区。"""

import asyncio
from pathlib import Path

from agentscope.app.workspace_manager import IsolationPolicy, WorkspaceManagerBase

from egis_agentkit.core.workspace._identity import sandbox_binding_for, workspace_id_for
from egis_agentkit.core.workspace._local_workspace import ManagedLocalWorkspace
from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.extension._workspace_read_guard_middleware_impl import (
    ReadToolGuardMiddleware,
)
from egis_agentkit.storage.project._files import ProjectFileStorage

from ._resources import configure_workspace_resources


class LocalWorkspaceManager(WorkspaceManagerBase):
    """保留本地开发模式，为固定执行绑定创建独立视图并管理其关闭。"""

    def __init__(self, workspace_root, resources=None):
        super().__init__(isolation=IsolationPolicy.PER_USER)
        self.files = ProjectFileStorage(Path(workspace_root))
        self.resources = resources
        self._workspaces = {}
        self._lock = asyncio.Lock()

    async def get_workspace(
        self, user_id, agent_id, session_id, workspace_id=None, *, project_id=None
    ):
        from egis_agentkit.extension._session_lock_override import (
            execution_scope,
            execution_setup_error,
        )

        error = execution_setup_error.get()
        if error is not None:
            raise error
        scope = execution_scope.get()
        if scope is not None and scope.user_id != user_id:
            raise Forbidden("工作区用户与会话不一致")
        if (
            scope is not None
            and project_id is not None
            and project_id != scope.project_id
        ):
            raise Forbidden("项目参数与当前会话不一致")
        project_id = scope.project_id if scope is not None else project_id
        if workspace_id is not None and workspace_id != workspace_id_for(user_id):
            raise Forbidden("会话工作区与用户归属不一致")
        if not project_id:
            raise NotFound("会话未绑定项目")
        key = sandbox_binding_for(user_id, project_id, agent_id, session_id)
        async with self._lock:
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
                    read_guard_factory=ReadToolGuardMiddleware,
                    workdir=str(root),
                    workspace_id=workspace_id_for(user_id),
                )
                await ws.initialize()
                self._workspaces[key] = ws
            workspace = self._workspaces[key]
        await configure_workspace_resources(workspace, self.resources, scope)
        return workspace

    async def refresh_resources(self, workspace):
        """重新解析本地执行视图的当前授权。"""
        from egis_agentkit.extension._session_lock_override import execution_scope

        await configure_workspace_resources(
            workspace, self.resources, execution_scope.get()
        )

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

    async def release_project(self, user_id, project_id):
        """项目删除前释放其本地执行视图，不触碰其他项目或用户。"""
        async with self._lock:
            identifiers = [
                key
                for key, workspace in self._workspaces.items()
                if (workspace.egis_user_id, workspace.egis_project_id)
                == (user_id, project_id)
            ]
            entries = [self._workspaces.pop(key) for key in identifiers]
        await asyncio.gather(*(workspace.close() for workspace in entries))
