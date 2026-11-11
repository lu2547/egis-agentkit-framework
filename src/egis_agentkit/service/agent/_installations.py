"""维护个人助理在技能和连接器广场选用的资源。

核心能力
- 安装、启用和停用资源，状态复用 AgentScope 安装记录。
- 运行时按稳定资源身份查选中项，并使用当前可用发布版。
"""

from agentscope.app._service._mcp_render import render_mcp
from agentscope.app.storage import MCPRecord, SkillRecord

from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service.resource._connector import _verify_mcp_card
from egis_agentkit.storage.model import utcnow


class PersonalInstallations:
    """个人助理的安装选择只对独立授权生效。"""

    def __init__(self, resources):
        self.resources = resources
        self.storage = resources.storage

    async def _selections(self, user, kind):
        if kind == "tool":
            return {}
        if kind == "skill":
            records = await self.storage.list_skills(user.user_id)
            hub = self.resources.skill_hub
        else:
            records = await self.storage.list_mcps(user.user_id)
            hub = self.resources.mcp_hub
        result = {}
        for record in records:
            if record.hub_id != hub.hub_id or not record.card_id:
                continue
            previous = result.get(record.card_id)
            # 原生 SQL 模型的安装记录统一按 UTC 排序。
            position = record.updated_at.replace(tzinfo=None), record.id
            if previous is None or position > previous[0]:
                result[record.card_id] = position, record
        return {identifier: record for identifier, (_, record) in result.items()}

    async def selection(self, user, asset):
        selections = await self._selections(user, asset["resource_type"])
        return selections.get(asset["resource_id"])

    async def assets(
        self, user, resource_type, rows=None, *, enabled_only=False, resource_ids=None
    ):
        """保留个人选择的开关，解析当前独立获准使用的版本。"""
        selections = await self._selections(user, resource_type)
        identifiers = {
            identifier
            for identifier, record in selections.items()
            if (not enabled_only or record.enabled)
            and (resource_ids is None or identifier in resource_ids)
        }
        if not identifiers:
            return {}
        if rows is None:
            rows = await self.resources._versions(
                resource_type, resource_ids=identifiers
            )
        visible = await self.resources._latest_visible_assets(
            user,
            resource_type,
            (row for row in rows if row["resource_id"] in identifiers),
        )
        return {
            identifier: asset
            for identifier, asset in visible.items()
            if await self.resources._usable(user, asset)
        }

    async def install(self, user, resource_type, resource_id, version):
        """为用户安装获准独立使用的 Skill 或 MCP 版本。"""
        if resource_type == "tool":
            raise Forbidden("工具通过平台或专家配置启用")
        asset = await self.resources._asset(resource_type, resource_id, version)
        await self.resources._require(user, asset)
        identifier = self.resources._native_id(
            user, resource_type, resource_id, version
        )
        name = self.resources._native_name(user, resource_type, resource_id, version)
        if resource_type == "skill":
            with self.resources._access(user, asset):
                card = await self.resources.skill_hub.get_skill_version(
                    user.user_id, asset["resource_id"], version
                )
            record = SkillRecord(
                id=identifier,
                user_id=user.user_id,
                name=name,
                display_name=asset["payload"].get("display_name"),
                description=card.description,
                markdown=card.markdown or "",
                hub_id=self.resources.skill_hub.hub_id,
                card_id=asset["resource_id"],
                version=version,
                enabled=True,
            )
            await self.storage.upsert_skill(user.user_id, record)
        else:
            card = self.resources.mcp_hub.card(asset)
            _verify_mcp_card(asset, card)
            client = render_mcp(card, {}, name=name)
            record = MCPRecord(
                id=identifier,
                user_id=user.user_id,
                client=client,
                display_name=asset["payload"].get("display_name"),
                description=card.description,
                hub_id=self.resources.mcp_hub.hub_id,
                card_id=asset["resource_id"],
                version=version,
                enabled=True,
            )
            await self.storage.upsert_mcp(user.user_id, record)
        return await self.resources._view(user, asset)

    async def set_installation(
        self, user, resource_type, resource_id, version, is_enabled
    ):
        """切换已安装 Skill 或 MCP 的启用状态。"""
        if resource_type == "tool":
            raise Forbidden("工具通过平台或专家配置启用")
        asset = await self.resources._asset(resource_type, resource_id, version)
        await self.resources._require(user, asset)
        record = await self.selection(user, asset)
        if record is None:
            raise NotFound("资源尚未安装")
        record.enabled = bool(is_enabled)
        record.updated_at = utcnow()
        method = (
            self.storage.upsert_skill
            if resource_type == "skill"
            else self.storage.upsert_mcp
        )
        await method(user.user_id, record)
        return await self.resources._view(user, asset)
