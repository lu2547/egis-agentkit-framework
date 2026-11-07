from egis_agentkit.models import Definition, User
from egis_agentkit.service.agent.definition import AgentDefinitionManager
from egis_agentkit.service.agent.package import PackageService
from egis_agentkit.storage import AgentKitStorage


class AgentService:
    """Agent object service, composing definition and optional package capabilities."""

    def __init__(
        self,
        storage: AgentKitStorage,
        policies=None,
        *,
        packages: PackageService | None = None,
    ):
        self.storage, self.policies = storage, policies
        self.definitions = AgentDefinitionManager(storage, policies)
        self.packages = packages

    async def personal(self, user: User) -> dict:
        return await self.definitions.personal(user)

    async def save(
        self, user: User, data: Definition, *, conn=None, allow_team_role=False
    ) -> dict:
        return await self.definitions.save(
            user, data, conn=conn, allow_team_role=allow_team_role
        )

    async def raw_view(self, definition_id: str, view: str, *, conn=None) -> dict:
        return await self.definitions.raw_view(definition_id, view, conn=conn)

    async def resolve(self, user: User, definition_id: str, view: str) -> dict:
        return await self.definitions.resolve(user, definition_id, view)

    async def list(self, user: User, *, definition_type="agent") -> list[dict]:
        return await self.definitions.list(user, definition_type=definition_type)

    async def delete(self, user: User, definition_id: str) -> None:
        await self.definitions.delete(user, definition_id)
