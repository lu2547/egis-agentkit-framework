"""实现 WorkspaceManagerBase，为会话取得对应项目的工作区。"""

import asyncio
import os
from pathlib import Path

from agentscope.app.workspace_manager import IsolationPolicy, WorkspaceManagerBase

from egis_agentkit.errors import NotFound
from egis_agentkit.storage._workspace import WorkspaceFiles

from ._workspace_local_override import ManagedLocalWorkspace


class EgisLocalWorkspaceManager(WorkspaceManagerBase):
    def __init__(self, workspace_root, resources=None):
        super().__init__(isolation=IsolationPolicy.PER_USER)
        self.files = WorkspaceFiles(Path(workspace_root))
        self.resources = resources
        self._workspaces = {}
        self._lock = asyncio.Lock()

    async def get_workspace(self, user_id, agent_id, session_id, workspace_id=None):
        from egis_agentkit.extension._session_lock_override import (
            execution_scope,
            execution_setup_error,
        )

        error = execution_setup_error.get()
        if error is not None:
            raise error
        scope = execution_scope.get()
        project_id = (
            scope.project_id
            if scope and scope.user_id == user_id
            else workspace_id
        )
        if not project_id:
            raise NotFound("会话未绑定项目")
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
