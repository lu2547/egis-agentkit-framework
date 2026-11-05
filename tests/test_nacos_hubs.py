import io
import zipfile

import httpx
import pytest

from egis_agentkit.config import NacosBootstrap, NacosClient, NacosError
from egis_agentkit.hubs import NacosHubManagement, NacosMCPHub, NacosSkillHub


@pytest.mark.asyncio
async def test_skill_permissions_apply_to_catalog_detail_and_download():
    seen = []

    async def authorize(user, kind, rid, op):
        seen.append((user, kind, rid, op))
        return rid == "allowed"

    def handle(req):
        if req.url.path.endswith("/list"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "pageItems": [
                            {"name": "denied", "description": "hidden"},
                            {
                                "name": "allowed",
                                "description": "ok",
                                "version": "1.0.0",
                                "frontMatter": {"is_skill_market_visible": True},
                                "api_key": "must-not-leak",
                            },
                        ],
                        "pagesAvailable": 1,
                    },
                },
            )
        if req.url.path.endswith("/download"):
            assert req.url.params["version"] == "1.0.0"
            return httpx.Response(
                200, content=b"zip-content", headers={"content-type": "application/zip"}
            )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "name": "allowed",
                    "version": "1.0.0",
                    "skillMd": "# Real instructions",
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = NacosClient(
            NacosBootstrap(server_url="http://registry"), http_client=http
        )
        hub = NacosSkillHub(client, authorize=authorize)
        cards = (await hub.list_skills("alice")).cards
        assert [c.id for c in cards] == ["allowed"]
        assert "api_key" not in cards[0].metadata
        assert (
            await hub.get_skill("alice", "allowed")
        ).markdown == "# Real instructions"
        archive = await hub.download("alice", "allowed", "1.0.0")
        assert archive.format == "zip"
        assert b"".join([chunk async for chunk in archive.stream]) == b"zip-content"
        for operation in (
            lambda: hub.get_skill("alice", "denied"),
            lambda: hub.download("alice", "denied", "1.0.0"),
        ):
            with pytest.raises(PermissionError):
                await operation()
        assert (await NacosSkillHub(client).list_skills("alice")).cards == []
    assert ("alice", "skill", "allowed", "download") in seen


@pytest.mark.asyncio
async def test_mcp_remote_endpoint_resolves_native_template_and_pins_version():
    async def allow(*args):
        return True

    def handle(req):
        assert req.url.params["version"] == "1.0.0"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "name": "tools",
                    "description": "Demo",
                    "versionDetail": {"version": "1.0.0"},
                    "protocol": "streamable",
                    "remoteServerConfig": {"exportPath": "/mcp"},
                    "backendEndpoints": [
                        {"protocol": "http", "address": "127.0.0.1", "port": 8766}
                    ],
                    "defaultCredential": "hidden",
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        hub = NacosMCPHub(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http),
            authorize=allow,
        )
        card = await hub.get_mcp_version("alice", "tools", "1.0.0")
        assert card.config_template.url == "http://127.0.0.1:8766/mcp"
        assert card.version == "1.0.0"
        assert "hidden" not in card.model_dump_json()


@pytest.mark.asyncio
async def test_management_validates_archive_paths_and_uses_draft_lifecycle_private_scope():
    requests = []

    def handle(req):
        requests.append(req)
        return httpx.Response(200, json={"code": 0, "data": "ok"})

    def archive(name):
        output = io.BytesIO()
        with zipfile.ZipFile(output, "w") as z:
            z.writestr(name, "---\nname: report\ndescription: reports\n---\nReport")
        return output.getvalue()

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        manager = NacosHubManagement(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
        )
        with pytest.raises(ValueError):
            await manager.upload_skill(archive("../SKILL.md"), "1.0.0")
        assert requests == []
        await manager.upload_skill(archive("report/SKILL.md"), "1.0.0")
        assert requests[0].url.path.endswith("/skills/upload")
        assert b"autoPublishIfNew" in requests[0].content
        await manager.create_mcp(
            "tools",
            "1.0.0",
            {
                "protocol": "stdio",
                "localServerConfig": {"tools": {"command": "python"}},
            },
        )
        assert requests[1].url.path.endswith("/skills/scope")
        assert requests[2].url.path.endswith("/mcp/draft")
        assert requests[3].url.path.endswith("/mcp/scope")
        assert b"scope=PRIVATE" in requests[3].content


@pytest.mark.asyncio
async def test_mcp_credentials_are_install_inputs_and_literal_secrets_are_rejected():
    async def allow(*args):
        return True

    entry = {
        "name": "private-tools",
        "protocol": "stdio",
        "versionDetail": {"version": "1.0.0"},
        "localServerConfig": {
            "private-tools": {
                "command": "python",
                "args": ["server.py"],
                "env": {"SERVICE_TOKEN": "${service_token}"},
            }
        },
    }

    def handle(req):
        return httpx.Response(200, json={"code": 0, "data": entry})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        hub = NacosMCPHub(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http),
            authorize=allow,
        )
        card = await hub.get_mcp("alice", "private-tools")
        assert card.auth == "inputs"
        assert card.inputs_schema["properties"]["service_token"]["writeOnly"] is True
        assert card.config_template.env["SERVICE_TOKEN"] == "${service_token}"
        entry["localServerConfig"]["private-tools"]["env"]["SERVICE_TOKEN"] = (
            "literal-secret"
        )
        with pytest.raises(NacosError):
            await hub.get_mcp("alice", "private-tools")


@pytest.mark.asyncio
async def test_management_does_not_store_literal_mcp_credentials():
    seen = []

    def handle(req):
        seen.append(req)
        return httpx.Response(200, json={"code": 0, "data": "ok"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        manager = NacosHubManagement(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
        )
        with pytest.raises(ValueError):
            await manager.create_mcp(
                "bad",
                "1.0.0",
                {
                    "protocol": "stdio",
                    "localServerConfig": {
                        "bad": {"command": "python", "env": {"TOKEN": "literal-secret"}}
                    },
                },
            )
        assert seen == []


@pytest.mark.asyncio
async def test_explicit_demo_force_publication_is_separate_from_normal_publish():
    paths = []

    def handle(req):
        paths.append(req.url.path)
        return httpx.Response(200, json={"code": 0, "data": "ok"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        manager = NacosHubManagement(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
        )
        await manager.force_publish_skill("report", "1.0.0")
        await manager.force_publish_mcp("tools", "1.0.0")
    assert paths == [
        "/nacos/v3/admin/ai/skills/force-publish",
        "/nacos/v3/admin/ai/mcp/force-publish",
    ]


@pytest.mark.asyncio
async def test_pagination_authorization_filter_does_not_fetch_individual_skill_files():
    calls = []

    async def allow(user, kind, rid, op):
        return rid == "visible"

    def handle(req):
        calls.append(req)
        assert req.url.path.endswith("/skills/list")
        assert req.url.params["pageNo"] == "2"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "pageItems": [{"name": "hidden"}, {"name": "visible"}],
                    "pageNumber": 2,
                    "pagesAvailable": 3,
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        hub = NacosSkillHub(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http),
            authorize=allow,
        )
        page = await hub.list_skills("alice", cursor="2", limit=2)
        assert page.next_cursor == "3"
        assert [c.name for c in page.cards] == ["visible"]
        assert page.cards[0].markdown is None
        assert len(calls) == 1
        with pytest.raises(ValueError):
            await hub.list_skills("alice", cursor="-1")
        assert len(calls) == 1
