from egis_agentkit.service.team.definition import TeamDefinitionManager


class TeamService:
    """Team object service with internally managed definitions."""

    def __init__(self, storage, policies=None):
        self.storage, self.policies = storage, policies
        self.definitions = TeamDefinitionManager(storage, policies)

    async def save(self, user, data: dict) -> dict:
        return await self.definitions.save(user, data)
