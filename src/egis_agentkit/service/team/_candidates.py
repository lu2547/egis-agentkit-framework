"""解析自动邀请候选及校验当前用户的使用权限。"""

from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service.agent._agent_manager import get_definition_for_execution
from egis_agentkit.storage.model._definition import EXPERT_TYPES, ViewType


class ExpertCandidates:
    def __init__(self, permissions):
        self.permissions = permissions
        self.storage = permissions.storage.expert_candidates

    async def selections(self, user):
        return await self.storage.list(user.user_id)

    async def _expert(self, user, identifier, view, *, version=None):
        expert = await get_definition_for_execution(
            self.permissions, user, "agent", identifier, view, version=version
        )
        if expert["agent_type"] not in EXPERT_TYPES:
            raise Forbidden("只能选择有权使用的专家作为自动邀请候选")
        return expert

    async def add(self, user, identifier: str, view: ViewType) -> None:
        await self._expert(user, identifier, view)
        await self.storage.add(user.user_id, identifier, view)

    async def remove(self, user, identifier: str, view: ViewType) -> None:
        if not user.is_enabled:
            raise Forbidden("用户已停用")
        await self.storage.remove(user.user_id, identifier, view)

    async def resolve(self, user) -> list[dict]:
        selections = sorted(await self.selections(user))
        references = [
            {
                "resource_type": "agent",
                "resource_id": identifier,
                "resource_view_type": view,
            }
            for identifier, view in selections
        ]
        snapshot = await self.permissions.read_snapshot(user, references)
        experts = []
        for reference in references:
            try:
                expert = await snapshot.definition_view(reference)
                if expert["agent_type"] not in EXPERT_TYPES:
                    continue
                await snapshot.require_execution(expert)
                experts.append(expert)
            except (Forbidden, NotFound):
                continue
        return experts

    async def require(self, user, identifier, view, *, version):
        if (identifier, view) not in await self.selections(user):
            raise Forbidden("该专家已移出自动邀请候选")
        return await self._expert(user, identifier, view, version=version)
