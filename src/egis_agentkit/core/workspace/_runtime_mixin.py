"""复用执行环境的工具和已选资源装载，不查询业务权限或项目记录。"""

import asyncio

from agentscope.tool import Bash, Read

from egis_agentkit.errors import Forbidden


class WorkspaceRuntimeMixin:
    """只保存当前执行视图的固定绑定，业务工作区身份仍按用户共享。"""

    def _bind_runtime(
        self,
        *,
        user_id,
        agent_id,
        session_id,
        project_id,
        files,
        read_guard_factory=None,
    ):
        self.egis_user_id = user_id
        self.egis_agent_id = agent_id
        self.egis_session_id = session_id
        self.egis_project_id = project_id
        self.egis_files = files
        self._read_guard_factory = read_guard_factory
        self._selected_tools = None
        self._selected_mcps = []
        self.egis_mcp_clients = {}
        self.egis_mcp_reconcile = asyncio.Lock()

    def current_session_id(self):
        return self.egis_session_id

    def current_project_id(self):
        return self.egis_project_id

    async def get_builtin_tools(self):
        """原生工具使用本次执行的 Backend 和目录，Read 增强由管理层注入。"""
        backend = self.get_backend()
        tools = await super().list_tools()
        result = []
        for tool in tools:
            if tool.name == "Bash":
                tool = Bash(
                    cwd=str(
                        self.egis_files.area_root(
                            self.egis_user_id, self.egis_project_id, "work"
                        )
                    ),
                    backend=backend,
                )
            elif tool.name == "Read" and self._read_guard_factory is not None:
                tool = Read(
                    backend=backend, middlewares=[self._read_guard_factory(backend)]
                )
            result.append(tool)
        return result

    def configure_resources(self, *, tools, mcps):
        """装载管理服务已选择的资源，Core 不自行查询用户、广场或授权。"""
        self._selected_tools = tools
        self._selected_mcps = mcps

    async def list_tools(self):
        return (
            await self.get_builtin_tools()
            if self._selected_tools is None
            else self._selected_tools
        )

    async def list_skills(self, *, agent_id=None):
        """技能由会话 Loader 激活后发布，不预先复制全部技能。"""
        return []

    def _mcp_partition(self, agent_id, session_id):
        aid, sid = agent_id or self.egis_agent_id, session_id or self.egis_session_id
        if (aid, sid) != (self.egis_agent_id, self.egis_session_id):
            raise Forbidden("MCP 请求与当前执行环境绑定不一致")
        return aid, sid
