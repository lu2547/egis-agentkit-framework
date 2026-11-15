"""原生 Agent 装配工具时调用，返回本轮已登记且获准的 Function Tool。"""

from __future__ import annotations

from agentscope.tool import ToolBase

from egis_agentkit.extension.knowledge._knowledge_search_tool_impl import (
    SearchKnowledge,
)
from egis_agentkit.extension.runtime._session_lock_override import bound_execution_scope
from egis_agentkit.service._user import UserService
from egis_agentkit.service.resource._catalog import ResourceService


class BoundToolFactory:
    """Return deployed tools registered and authorized for this session."""

    def __init__(
        self,
        resources: ResourceService | None = None,
        users: UserService | None = None,
        knowledge=None,
    ) -> None:
        self.resources, self.users = resources, users
        self.knowledge = knowledge

    async def __call__(
        self, user_id: str, agent_id: str, session_id: str
    ) -> list[ToolBase]:
        scope = bound_execution_scope(user_id, agent_id, session_id)
        tools = []
        registry = self.resources.tool_registry if self.resources else None
        if self.resources is not None and self.resources.tool_registry is not None:
            user = await self.users.get(user_id)
            tools = list(await self.resources.tool_registry.resolve(user, scope))
        if (
            self.knowledge is not None
            and registry is not None
            and registry.is_default_tool_available(SearchKnowledge, scope)
            and scope.definition["agent_type"] != "remote"
            and await self.knowledge.allowed_knowledge_bases(user_id)
        ):
            tools.append(SearchKnowledge(self.knowledge, user_id))
        return tools
