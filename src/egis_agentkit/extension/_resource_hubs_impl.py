"""实现 SkillHubBase 和 MCPHubBase，按调用者的授权读取平台资源。"""

from agentscope.app.hub import (
    MCPCard,
    MCPHubBase,
    MCPHubPage,
    SkillCard,
    SkillHubBase,
    SkillHubPage,
)
from agentscope.app.hub._skill._base import SkillArchive
from agentscope.mcp import HttpMCPConfig

from egis_agentkit.errors import Forbidden, NotFound


class _LocalHub:
    def __init__(self, resources, kind):
        super().__init__("egis-local", "Egis")
        self.resources, self.kind = resources, kind

    async def _asset(self, user_id, rid, version=None):
        return await self.resources.hub_asset(user_id, self.kind, rid, version)

    def _fields(self, asset):
        payload = asset["payload"]
        return {
            "hub_id": self.hub_id,
            "id": asset["resource_id"],
            "name": asset["resource_id"],
            "version": asset["resource_version"],
            "display_name": payload.get("display_name"),
            "description": payload.get("description", ""),
        }

    async def _list(self, user_id, q, cursor, limit):
        if not 1 <= limit <= 100:
            raise ValueError("limit must be between 1 and 100")
        rows = await self.resources.hub_list(user_id, self.kind)
        needle = (q or "").casefold()
        rows = [
            row
            for row in rows
            if row["resource_id"] > (cursor or "")
            and any(
                needle in str(field).casefold() for field in self._fields(row).values()
            )
        ]
        return rows[:limit], rows[limit - 1]["resource_id"] if len(
            rows
        ) > limit else None


class LocalSkillHub(_LocalHub, SkillHubBase):
    def __init__(self, resources):
        super().__init__(resources, "skill")

    async def list_skills(self, user_id, q=None, cursor=None, limit=20):
        rows, next_cursor = await self._list(user_id, q, cursor, limit)
        return SkillHubPage(
            cards=[SkillCard(**self._fields(row)) for row in rows],
            next_cursor=next_cursor,
        )

    async def get_skill(self, user_id, card_id):
        return await self.get_skill_version(user_id, card_id, None)

    async def get_skill_version(self, user_id, card_id, version):
        asset = await self._asset(user_id, card_id, version)
        return SkillCard(
            **self._fields(asset), markdown=asset["payload"].get("original_markdown")
        )

    async def download(self, user_id, card_id, version=None):
        asset = await self._asset(user_id, card_id, version)
        content = await self.resources.skill_registry.archive(asset)

        async def stream():
            for start in range(0, len(content), 65536):
                yield content[start : start + 65536]

        return SkillArchive("zip", stream())


class LocalMCPHub(_LocalHub, MCPHubBase):
    def __init__(self, resources):
        super().__init__(resources, "mcp")

    def card(self, asset):
        config = self.resources.mcp_registry.config(asset)
        return MCPCard(
            **self._fields(asset),
            auth="none",
            config_template=HttpMCPConfig(url=config["url"]),
        )

    async def list_mcps(self, user_id, q=None, cursor=None, limit=20):
        rows, next_cursor = await self._list(user_id, q, cursor, limit)
        cards = []
        for row in rows:
            try:
                config = self.resources.mcp_registry.config(row)
            except NotFound:
                continue
            if config.get("auth_type", "none") == "none":
                cards.append(self.card(row))
        return MCPHubPage(cards=cards, next_cursor=next_cursor)

    async def get_mcp(self, user_id, card_id):
        return await self.get_mcp_version(user_id, card_id, None)

    async def get_mcp_version(self, user_id, card_id, version):
        asset = await self._asset(user_id, card_id, version)
        if self.resources.mcp_registry.config(asset).get("auth_type", "none") != "none":
            raise Forbidden("请从 Egis 连接器管理授权")
        return self.card(asset)
