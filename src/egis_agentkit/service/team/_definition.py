"""专家团定义、固定成员引用及主理人的原子写入。"""

from agentscope.app.storage import TeamData, TeamMember, TeamRecord

from egis_agentkit.client.nacos._settings import TeamRosterConfig
from egis_agentkit.core.agent._orchestrator_definition import OrchestratorDefinition
from egis_agentkit.errors import Conflict, Forbidden, InvalidRequest, NotFound
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.storage.model import TeamDefinitionRecord, new_id
from egis_agentkit.storage.model._definition import EXPERT_TYPES


class TeamDefinition:
    """维护专家团、主理人和固定成员版本。"""

    def __init__(self, storage, policies=None, *, config_provider=None):
        """注入定义存储、权限策略和主理人服务。"""
        self.config = config_provider
        self.storage = storage
        self.repository = storage.definitions
        self.permissions = PermissionService(storage, policies)

    @staticmethod
    def make_record(
        user_id: str,
        *,
        leader_agent_id: str,
        leader_session_id: str,
        name: str,
        description: str,
        members: list[TeamMember],
    ) -> TeamRecord:
        return TeamRecord(
            user_id=user_id,
            session_id=leader_session_id,
            leader_agent_id=leader_agent_id,
            data=TeamData(name=name, description=description, members=members),
        )

    async def _team_definition(self, identifier: str) -> dict:
        """读取专家团当前记录并校验定义类型。"""
        source = await self.repository.get_definition(identifier)
        if source["definition_type"] != "team":
            raise NotFound("资源类型不匹配")
        return source

    async def _describe(self, user, source, view=None, *, version=None) -> dict:
        """按当前权限选择视图和版本，再读取定义配置。"""
        access = await self.permissions.definition_access(
            user, source, view, version=version
        )
        value = await self.repository.get_definition_view(
            source["id"],
            access.view,
            version=access.decision.resource_version or version,
            **access.decision.source_options,
        )
        share = await self.permissions.share_availability(
            user,
            value["definition_type"],
            value["id"],
            version=value["resource_version"],
            view=access.view,
        )
        return {
            **value,
            **share,
            "resource_view_type": access.view,
            "is_editable": access.is_editable,
            "is_deletable": access.is_editable,
            "is_usable": access.decision.is_allowed,
        }

    async def read(
        self, user, identifier: str, view: str = "private", *, version=None
    ) -> dict:
        """展示专家团指定版本，维护人员可读取该版本的主理人配置。"""
        source = await self._team_definition(identifier)
        result = await self._describe(user, source, view, version=version)
        own = source["owner_type"] == "user" and source["owner_id"] == user.user_id
        if result["resource_view_type"] != "shared" and (
            own or user.role_type == "admin"
        ):
            ref = result["payload"]["orchestrator"]
            leader = await self.repository.get_definition(
                ref["definition_id"], version=ref["resource_version"]
            )
            result["orchestrator_config"] = {
                key: leader[key]
                for key in (
                    "agent_name",
                    "agent_display_name",
                    "payload",
                    "owner_version",
                )
            }
        return result

    async def versions(self, user, identifier: str) -> list[dict]:
        """列出用户有权维护或独立使用的专家团版本。"""
        source = await self._team_definition(identifier)
        if not source["is_enabled"]:
            raise Forbidden("资源已停用")
        return await self.permissions.definition_versions(user, source)

    async def list(self, user) -> list[dict]:
        """列出当前用户可见的专家团及共享审核状态。"""
        rows, states = await self.repository.list_definitions_and_reviews("team")
        result = []
        for row in rows:
            if not row["is_enabled"]:
                continue
            try:
                value = await self._describe(user, row)
            except (Forbidden, NotFound):
                continue
            status = states.get((value["id"], value["resource_version"]), "private")
            result.append({**value, "share_status_type": status})
        return result

    async def delete(self, user, identifier: str) -> None:
        """在同一事务中停用专家团及其主理人。"""
        async with self.storage.definition_write() as write:
            row = await write.definitions.get_locked_definition(identifier)
            if row is None or row["definition_type"] != "team":
                raise NotFound()
            self.permissions.require_definition_write(
                user, owner_type=row["owner_type"], owner_id=row["owner_id"]
            )
            await write.definitions.delete_definition(identifier, actor=user.user_id)
            await GrantService(write).retire_definition_grants(
                identifier, actor=user.user_id, previous=row
            )
            leader_id = row["payload"]["orchestrator"]["definition_id"]
            leader = await write.agents.get_definition_for_update(leader_id)
            if leader["agent_type"] != "orchestrator":
                raise Forbidden("专家团主理人类型不匹配")
            self.permissions.require_definition_write(
                user, owner_type=leader["owner_type"], owner_id=leader["owner_id"]
            )
            await write.agents.delete_definition(leader_id, actor=user.user_id)
            await GrantService(write).retire_definition_grants(
                leader_id, actor=user.user_id, previous=leader
            )

    async def save(self, user, data: dict) -> dict:
        """校验专家成员后，原子地保存专家团及主理人的新版本。"""
        owner_type = data.get("owner_type", "user")
        owner_id = data.get("owner_id", user.user_id)
        self.permissions.require_definition_write(
            user, owner_type=owner_type, owner_id=owner_id
        )
        if data.get("orchestrator_id"):
            raise Forbidden("主理人由专家团管理，不能选择一个 expert 充当主理人")
        if not data.get("orchestrator_config") or not data.get("members"):
            raise Forbidden("专家团必须有编排智能体和固定专家成员")
        configuration = self.config.snapshot if self.config else None
        roster = configuration.team.roster if configuration else TeamRosterConfig()
        if len(data["members"]) > roster.max_members:
            raise InvalidRequest(f"专家团最多可配置 {roster.max_members} 位成员")
        policies = configuration.access if configuration else self.permissions.policies
        identifier = data.get("id") or new_id()
        async with self.storage.definition_write() as write:
            permissions = PermissionService(write, policies)
            previous = await write.definitions.get_locked_definition(identifier)
            if previous and previous["definition_type"] != "team":
                raise Forbidden("该 ID 已由 Agent 定义使用")
            if previous and (previous["owner_type"], previous["owner_id"]) != (
                owner_type,
                owner_id,
            ):
                raise Forbidden("不能改变专家团归属")
            expected = previous["config_version"] if previous else 1
            if data.get("config_version", 1) != expected:
                raise Conflict("专家团配置已变更，请刷新后重试")
            orch = data["orchestrator_config"]
            # 只解析一次成员配置，将实际获准使用的版本保存到专家团。
            members = []
            names, seen = {orch["agent_name"]}, set()
            for ref in data["members"]:
                if ref["definition_id"] in seen:
                    raise Forbidden("不能重复添加同一专家")
                seen.add(ref["definition_id"])
                child_options = {"version": ref.get("resource_version")}
                if user.role_type != "admin" or ref["resource_view_type"] == "shared":
                    decision = await permissions.require(
                        user,
                        "agent",
                        ref["definition_id"],
                        ref["resource_view_type"],
                        version=ref.get("resource_version"),
                    )
                    child_options.update(decision.source_options)
                    child_options["version"] = decision.resource_version or ref.get(
                        "resource_version"
                    )
                child = await write.definitions.get_definition_view(
                    ref["definition_id"],
                    ref["resource_view_type"],
                    **child_options,
                )
                if (
                    child["agent_type"] not in EXPERT_TYPES
                    or child["agent_name"] in names
                ):
                    raise Forbidden("成员必须是 expert，且团内英文名唯一")
                if owner_type == "system" and child["owner_type"] != "system":
                    raise Forbidden("系统专家团不能引用用户私有专家")
                names.add(child["agent_name"])
                members.append(
                    {
                        "definition_id": ref["definition_id"],
                        "resource_view_type": ref["resource_view_type"],
                        "resource_version": child["resource_version"],
                    }
                )
            # 主理人维护自己的配置和版本，Team 只接收固定引用。
            saved_leader = await self._save_orchestrator(
                write,
                user,
                policies=policies,
                owner_type=owner_type,
                owner_id=owner_id,
                config=orch,
                previous_ref=previous["payload"]["orchestrator"] if previous else None,
            )
            team = TeamDefinitionRecord(
                id=identifier,
                definition_type="team",
                owner_type=owner_type,
                owner_id=owner_id,
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
                        "definition_id": saved_leader["id"],
                        "resource_version": saved_leader["resource_version"],
                        "resource_view_type": "system"
                        if owner_type == "system"
                        else "private",
                    },
                    "members": members,
                },
            )
            values = team.model_dump(mode="json")
            if previous:
                result = await write.definitions.update_definition(
                    identifier,
                    values,
                    actor=user.user_id,
                )
            else:
                result = await write.definitions.add_definition(
                    values,
                    actor=user.user_id,
                )
            await GrantService(write).record_definition_owner(
                result, actor=user.user_id
            )
            return result

    async def _save_orchestrator(
        self, write, user, *, policies, owner_type, owner_id, config, previous_ref=None
    ):
        """组合主理人核心能力与团队事务，保持团队和主理人原子更新。"""
        # 1. 读取现有主理人，Core 根据团队配置构建同一身份的新配置。
        previous = (
            await write.agents.get_definition_for_update(previous_ref["definition_id"])
            if previous_ref
            else None
        )
        core = OrchestratorDefinition()
        data = core.build_from_team(
            owner_type=owner_type, owner_id=owner_id, orchestrator_config=config, previous_definition=previous
        )
        permissions = PermissionService(write, policies)
        permissions.require_definition_write(
            user, owner_type=data.owner_type, owner_id=data.owner_id
        )
        await permissions.require_agent_bindings(user, data)
        # 2. 使用团队已借用的存储事务，统一提交版本和授权。
        values = data.model_dump(mode="json")
        if previous is None:
            saved = await write.agents.add_definition(values, actor=user.user_id)
        else:
            saved = await write.agents.update_definition(
                data.id, values, actor=user.user_id
            )
        await GrantService(write).record_definition_owner(saved, actor=user.user_id)
        return saved
