"""Definition detail visibility, use permission and editable team configuration."""

from egis_agentkit.errors import Forbidden, NotFound


class DefinitionDetailService:
    def __init__(self, storage, agents, permissions):
        self.storage = storage
        self.agents = agents
        self.permissions = permissions

    async def get(self, user, identifier, view, expected_type):
        source = await self.storage.get_definition(identifier)
        if source["definition_type"] != expected_type:
            raise NotFound("资源类型不匹配")
        editable = (
            user.role_type == "admin"
            or source["owner_type"] == "user"
            and source["owner_id"] == user.user_id
        )
        if view == "private" and not editable:
            raise Forbidden()
        if not editable and source["owner_type"] != "system":
            await self.permissions.require(user, expected_type, identifier, view)
        row = await self.agents.raw_view(identifier, view)
        result = {
            **row,
            "resource_view_type": view,
            "is_editable": editable,
            "is_deletable": editable,
            "is_usable": (
                await self.permissions.explain(user, expected_type, identifier, view)
            ).is_allowed,
        }
        if expected_type == "team" and editable:
            ref = row["payload"]["orchestrator"]
            leader = await self.storage.get_definition(ref["definition_id"])
            result["orchestrator_config"] = {
                key: leader[key]
                for key in ("agent_name", "agent_display_name", "payload", "config_version")
            }
        return result
