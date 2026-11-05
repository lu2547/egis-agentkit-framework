"""Native read/install hubs; enterprise authorization remains outside Nacos."""

import re
from collections.abc import Awaitable, Callable

import httpx
from agentscope.app.hub import (
    MCPCard,
    MCPHubBase,
    MCPHubPage,
    SkillCard,
    SkillHubBase,
    SkillHubPage,
)
from agentscope.app.hub._skill._base import SkillArchive
from agentscope.mcp import HttpMCPConfig, StdioMCPConfig

from ..config import NacosClient, NacosError

Authorization = Callable[[str, str, str, str], Awaitable[bool]]
BASE = "/nacos/v3/admin/ai/"


def _version(item: dict) -> str | None:
    return (
        item.get("version")
        or (item.get("versionDetail") or {}).get("version")
        or (item.get("labels") or {}).get("latest")
    )


def _page(cursor: str | None, limit: int) -> int:
    if not 1 <= limit <= 100:
        raise ValueError("limit must be between 1 and 100")
    try:
        page = int(cursor) if cursor else 1
    except ValueError:
        raise ValueError("Invalid Nacos page cursor") from None
    if page < 1:
        raise ValueError("Invalid Nacos page cursor")
    return page


class _AuthorizedHub:
    def _configure(self, client: NacosClient, authorize: Authorization | None):
        self.client = client
        self.authorize = authorize

    async def _allowed(
        self, user_id: str, kind: str, resource: str, operation: str
    ) -> bool:
        return bool(
            self.authorize and await self.authorize(user_id, kind, resource, operation)
        )

    async def _require(self, user_id: str, kind: str, resource: str, operation: str):
        if not await self._allowed(user_id, kind, resource, operation):
            raise PermissionError("Resource access denied")

    def _params(self, **kwargs):
        return {
            "namespaceId": self.client.bootstrap.namespace_id,
            **{k: v for k, v in kwargs.items() if v is not None},
        }

    async def _read(self, path, **params):
        try:
            return self.client.data(
                await self.client.request(
                    "GET", BASE + path, params=self._params(**params)
                )
            )
        except NacosError as exc:
            if exc.status_code == 404:
                raise KeyError("Nacos resource not found") from None
            raise


class NacosSkillHub(_AuthorizedHub, SkillHubBase):
    def __init__(
        self,
        client: NacosClient,
        authorize: Authorization | None = None,
        *,
        hub_id: str = "nacos-skills",
    ):
        super().__init__(hub_id=hub_id, display_name="Nacos Skills")
        self._configure(client, authorize)

    def _card(self, item: dict, markdown: str | None = None) -> SkillCard:
        front = item.get("frontMatter") or {}
        visible = front.get("is_skill_market_visible", False)
        return SkillCard(
            hub_id=self.hub_id,
            id=item["name"],
            name=item["name"],
            description=item.get("description") or "",
            version=_version(item),
            markdown=markdown,
            metadata={"is_skill_market_visible": visible is True or visible == "true"},
        )

    async def list_skills(self, user_id, q=None, cursor=None, limit=20):
        page = _page(cursor, limit)
        body = await self._read(
            "skills/list", pageNo=page, pageSize=limit, skillName=q, search="blur"
        )
        cards = [
            self._card(item)
            for item in body["pageItems"]
            if await self._allowed(user_id, "skill", item["name"], "list")
        ]
        return SkillHubPage(
            cards=cards,
            next_cursor=str(page + 1)
            if page < body.get("pagesAvailable", page)
            else None,
        )

    async def get_skill(self, user_id, card_id):
        return await self.get_skill_version(user_id, card_id, None)

    async def get_skill_version(self, user_id, card_id, version):
        await self._require(user_id, "skill", card_id, "get")
        meta = await self._read("skills", skillName=card_id)
        selected = version or _version(meta)
        if not selected:
            raise KeyError("Skill has no published version")
        content = await self._read(
            "skills/version", skillName=card_id, version=selected
        )
        return self._card(
            {**meta, **content, "version": selected}, markdown=content.get("skillMd")
        )

    async def download(self, user_id, card_id, version=None):
        await self._require(user_id, "skill", card_id, "download")
        if version is None:
            meta = await self._read("skills", skillName=card_id)
            version = _version(meta)
            if not version:
                raise KeyError("Skill has no published version")
        request = self.client.http.build_request(
            "GET",
            self.client._url(BASE + "skills/version/download"),
            headers=self.client._headers(),
            params=self._params(skillName=card_id, version=version),
        )
        try:
            response = await self.client.http.send(
                request, stream=True, follow_redirects=False
            )
        except httpx.HTTPError:
            raise NacosError("Nacos skill download unavailable", 503) from None
        try:
            if "json" in response.headers.get("content-type", ""):
                await response.aread()
            self.client.check_response(response)
            if "zip" not in response.headers.get("content-type", ""):
                raise NacosError("Nacos skill download is not a ZIP archive")
        except Exception:
            await response.aclose()
            raise

        async def stream():
            try:
                async for chunk in response.aiter_bytes():
                    yield chunk
            except httpx.HTTPError:
                raise NacosError("Nacos skill stream interrupted", 503) from None
            finally:
                await response.aclose()

        return SkillArchive("zip", stream())


class NacosMCPHub(_AuthorizedHub, MCPHubBase):
    def __init__(
        self,
        client: NacosClient,
        authorize: Authorization | None = None,
        *,
        hub_id: str = "nacos-mcps",
    ):
        super().__init__(hub_id=hub_id, display_name="Nacos MCP")
        self._configure(client, authorize)

    def _card(self, item, *, listing=False):
        protocol = item.get("protocol")
        if protocol == "stdio":
            local = item.get("localServerConfig") or {}
            entry = local.get(item["name"]) or next(iter(local.values()), {})
            # Registry source must contain references/placeholders, never resolved credentials.
            env = entry.get("env") or {}
            if any(
                not (str(v).startswith("${") and str(v).endswith("}"))
                for v in env.values()
            ):
                raise NacosError(
                    "MCP environment contains a literal credential; use input references"
                )
            config = StdioMCPConfig(
                command=entry.get("command", "${command}"),
                args=entry.get("args"),
                env=env or None,
            )
        else:
            endpoints = item.get("backendEndpoints") or []
            remote = item.get("remoteServerConfig") or {}
            if endpoints:
                endpoint = endpoints[0]
                scheme = (
                    endpoint.get("protocol")
                    or (remote.get("serviceRef") or {}).get("transportProtocol")
                    or "http"
                )
                host = endpoint["address"]
                if scheme not in {"http", "https"} or any(c in host for c in "/@?#"):
                    raise NacosError("Invalid MCP endpoint")
                if ":" in host and not host.startswith("["):
                    host = "[" + host + "]"
                path = endpoint.get("path") or remote.get("exportPath") or "/mcp"
                if not path.startswith("/") or "?" in path or "#" in path:
                    raise NacosError("Invalid MCP endpoint path")
                config = HttpMCPConfig(
                    url=f"{scheme}://{host}:{int(endpoint['port'])}{path}"
                )
            elif listing:
                # List payload omits endpoints. Native installer always fetches get_mcp.
                config = HttpMCPConfig(url="${endpoint_url}")
            else:
                raise NacosError("MCP registry has no connectable endpoint")
        placeholders = sorted(
            set(re.findall(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}", config.model_dump_json()))
        )
        inputs = (
            {
                "type": "object",
                "properties": {
                    name: {"type": "string", "writeOnly": True, "format": "password"}
                    for name in placeholders
                },
                "required": placeholders,
            }
            if placeholders
            else {}
        )
        return MCPCard(
            hub_id=self.hub_id,
            id=item["name"],
            name=item["name"],
            description=item.get("description") or "",
            version=_version(item),
            config_template=config,
            auth="inputs" if placeholders else "none",
            inputs_schema=inputs,
            is_stateful=True,
        )

    async def list_mcps(self, user_id, q=None, cursor=None, limit=20):
        page = _page(cursor, limit)
        body = await self._read(
            "mcp/list", pageNo=page, pageSize=limit, mcpName=q, search="blur"
        )
        cards = [
            self._card(item, listing=True)
            for item in body["pageItems"]
            if await self._allowed(user_id, "mcp", item["name"], "list")
        ]
        return MCPHubPage(
            cards=cards,
            next_cursor=str(page + 1)
            if page < body.get("pagesAvailable", page)
            else None,
        )

    async def get_mcp(self, user_id, card_id):
        return await self.get_mcp_version(user_id, card_id, None)

    async def get_mcp_version(self, user_id, card_id, version):
        await self._require(user_id, "mcp", card_id, "get")
        item = await self._read("mcp", mcpName=card_id, version=version)
        if version and _version(item) != version:
            raise NacosError("MCP registry did not return the pinned version")
        return self._card(item)
