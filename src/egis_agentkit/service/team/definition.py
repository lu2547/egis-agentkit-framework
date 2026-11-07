from egis_agentkit.errors import Forbidden
from egis_agentkit.models import Definition, new_id
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.agent.definition import AgentDefinitionManager


class TeamDefinitionManager:
    def __init__(self, storage, policies=None):
        self.storage, self.policies = storage, policies
        self.agents = AgentDefinitionManager(storage, policies)
        self.repository = self.agents.repository

    async def save(self, user, data: dict) -> dict:
        if data.get("orchestrator_id"):
            raise Forbidden("主理人由专家团管理，不能选择一个 expert 充当主理人")
        if not data.get("orchestrator_config") or not data.get("members"):
            raise Forbidden("专家团必须有编排智能体和固定专家成员")
        owner_type = data.get("owner_type", "user")
        owner_id = data.get("owner_id", user.user_id)
        if user.role_type != "admin" and (
            owner_type != "user" or owner_id != user.user_id
        ):
            raise Forbidden()
        identifier = data.get("id") or new_id()
        async with self.storage.transaction() as conn:
            previous = await self.repository.locked_definition(identifier, conn=conn)
            if previous and (previous["owner_type"], previous["owner_id"]) != (
                owner_type,
                owner_id,
            ):
                raise Forbidden("不能改变专家团归属")
            orch_id = (
                previous["payload"]["orchestrator"]["definition_id"]
                if previous
                else new_id()
            )
            orch_version = (
                (await self.repository.get_definition(orch_id, conn=conn))[
                    "config_version"
                ]
                if previous
                else 1
            )
            orch = data["orchestrator_config"]
            leader = Definition(
                id=orch_id,
                owner_type=owner_type,
                owner_id=owner_id,
                agent_type="orchestrator",
                agent_name=orch["agent_name"],
                agent_display_name=orch["agent_display_name"],
                config_version=orch_version,
                payload=orch.get("payload", {}),
            )
            members = data["members"]
            names, seen = {leader.agent_name}, set()
            for ref in members:
                if ref["definition_id"] in seen:
                    raise Forbidden("不能重复添加同一专家")
                seen.add(ref["definition_id"])
                child = await self.agents.raw_view(
                    ref["definition_id"], ref["resource_view_type"], conn=conn
                )
                if child["agent_type"] != "expert" or child["agent_name"] in names:
                    raise Forbidden("成员必须是 expert，且团内英文名唯一")
                if owner_type == "system" and child["owner_type"] != "system":
                    raise Forbidden("系统专家团不能引用用户私有专家")
                if user.role_type != "admin":
                    await PermissionService(self.storage, self.policies).require(
                        user, "agent", child["id"], ref["resource_view_type"]
                    )
                names.add(child["agent_name"])
            await self.agents.save(user, leader, conn=conn, allow_team_role=True)
            team = Definition(
                id=identifier,
                definition_type="team",
                owner_type=owner_type,
                owner_id=owner_id,
                agent_type=None,
                team_name=data["team_name"],
                team_display_name=data["team_display_name"],
                config_version=data.get("config_version", 1),
                payload={
                    **{
                        k: data.get("payload", {}).get(k, default)
                        for k, default in {
                            "description": "",
                            "category": "办公协同",
                            "tags": [],
                            "avatar": "team",
                            "starter_prompts": [],
                        }.items()
                    },
                    "orchestrator": {
                        "definition_id": orch_id,
                        "resource_view_type": "system"
                        if owner_type == "system"
                        else "private",
                    },
                    "members": members,
                },
            )
            return await self.agents.save(user, team, conn=conn, allow_team_role=True)


# Compatibility for callers that still import the former definition service.
TeamService = TeamDefinitionManager
