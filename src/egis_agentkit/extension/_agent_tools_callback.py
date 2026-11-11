"""原生 Agent 装配工具时调用，返回本轮已登记且获准的 Function Tool。"""

from agentscope.tool import ToolBase

from ._session_lock_override import bound_execution_scope


class BoundToolFactory:
    """Return deployed tools registered and authorized for this session."""

    def __init__(self, resources=None, users=None):
        self.resources, self.users = resources, users

    async def __call__(
        self, user_id: str, agent_id: str, session_id: str
    ) -> list[ToolBase]:
        scope = bound_execution_scope(user_id, agent_id, session_id)
        if self.resources is not None and self.resources.tool_registry is not None:
            user = await self.users.get(user_id)
            return await self.resources.tool_registry.resolve(user, scope)
        return []
