"""覆盖原生 LocalWorkspace，复用企业工具选择并在本地连接 MCP。"""

import asyncio

from agentscope.workspace import LocalWorkspace

from ._runtime_mixin import WorkspaceRuntimeMixin


class ManagedLocalWorkspace(WorkspaceRuntimeMixin, LocalWorkspace):
    """保留原生本地环境，补充企业运行身份和授权连接。"""

    def __init__(
        self,
        *,
        user_id,
        agent_id,
        session_id,
        project_id,
        files,
        read_guard_factory=None,
        **kwargs,
    ):
        self._bind_runtime(
            user_id=user_id,
            agent_id=agent_id,
            session_id=session_id,
            project_id=project_id,
            files=files,
            read_guard_factory=read_guard_factory,
        )
        super().__init__(**kwargs)

    async def list_mcps(self, *, agent_id=None, session_id=None):
        aid, sid = self._mcp_partition(agent_id, session_id)
        declared = self._selected_mcps
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
                    # 凭据仅保存在内存；原生 add_mcp 会把完整配置写入 .mcp，不能调用。
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
