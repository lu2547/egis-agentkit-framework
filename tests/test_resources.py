import asyncio
import io
import json
import zipfile
from types import SimpleNamespace
from urllib.parse import parse_qs

import httpx
import pytest
from sqlalchemy import select
from test_domain import identities

from egis_agentkit.config import NacosBootstrap, NacosClient
from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.integration.nacos.registry import NacosHubManagement
from egis_agentkit.extensions.hub import NacosMCPHub, NacosSkillHub
from egis_agentkit.models import Asset, Grant, ResourceRef
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.resource.service import ResourceService, skill_archive_digest
from egis_agentkit.storage import tables as t


def archive(name="report", extra="references/checklist.md"):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr(
            name + "/SKILL.md",
            "---\nname: " + name + "\ndescription: Demo\n---\n# Report",
        )
        z.writestr(name + "/" + extra, "Actual attached content")
    return out.getvalue()


class Registry:
    def __init__(self):
        self.files = {("report", "1.0.0"): archive()}
        self.calls = []
        self.mcps = {}

    def request(self, req):
        self.calls.append(req)
        params = dict(req.url.params)
        if req.method in ("POST", "PUT") and "multipart" not in req.headers.get(
            "content-type", ""
        ):
            params.update({k: v[0] for k, v in parse_qs(req.content.decode()).items()})
        if req.url.path.endswith("/download"):
            return httpx.Response(
                200,
                content=self.files[(params["skillName"], params["version"])],
                headers={"content-type": "application/zip"},
            )
        if req.url.path.endswith("/skills") and req.method == "GET":
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "name": params["skillName"],
                        "labels": {"latest": "1.0.0"},
                    },
                },
            )
        if req.url.path.endswith("/skills/version"):
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {"name": params["skillName"], "skillMd": "# Report"},
                },
            )
        if req.url.path.endswith("/mcp/draft"):
            spec = json.loads(params["serverSpecification"])
            endpoint = json.loads(params["endpointSpecification"])["data"]
            spec["remoteServerConfig"]["serviceRef"] = {
                "transportProtocol": endpoint.get("transportProtocol")
            }
            self.mcps[(params["mcpName"], params["version"])] = {
                **spec,
                "name": params["mcpName"],
                "version": params["version"],
                "backendEndpoints": [
                    {
                        "address": endpoint["address"],
                        "port": int(endpoint["port"]),
                        "path": endpoint.get("path", "/mcp"),
                        "protocol": None,
                    }
                ],
            }
        if req.url.path.endswith("/mcp") and req.method == "GET":
            result = self.mcps.get(
                (params["mcpName"], params["version"]),
                {
                    "name": params["mcpName"],
                    "version": params["version"],
                    "protocol": "streamable",
                    "backendEndpoints": [
                        {"address": "127.0.0.1", "port": 8766, "path": "/mcp"}
                    ],
                },
            )
            return httpx.Response(200, json={"code": 0, "data": result})
        if req.url.path.endswith("/skills/upload"):
            from email.parser import BytesParser
            from email.policy import default

            message = BytesParser(policy=default).parsebytes(
                (
                    "Content-Type: "
                    + req.headers["content-type"]
                    + "\r\nMIME-Version: 1.0\r\n\r\n"
                ).encode()
                + req.content
            )
            fields = {
                p.get_param("name", header="content-disposition"): p.get_payload(
                    decode=True
                )
                for p in message.iter_parts()
            }
            with zipfile.ZipFile(io.BytesIO(fields["file"])) as z:
                import yaml

                md = z.read(
                    next(n for n in z.namelist() if n.endswith("SKILL.md"))
                ).decode()
                name = yaml.safe_load(md.split("---", 2)[1])["name"]
            self.files[(name, fields["targetVersion"].decode())] = fields["file"]
        return httpx.Response(200, json={"code": 0, "data": "ok"})


async def service(storage, tmp_path, registry):
    http = httpx.AsyncClient(transport=httpx.MockTransport(registry.request))
    client = NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
    skills = NacosSkillHub(client)
    mcps = NacosMCPHub(client)
    result = ResourceService(
        storage, skills, mcps, NacosHubManagement(client), tmp_path
    )
    skills.authorize = result.authorize_hub
    mcps.authorize = result.authorize_hub
    return result, http


async def put_asset(
    storage,
    kind="skill",
    rid="report",
    version="1.0.0",
    owner_type="system",
    owner_id="demo",
    checksum="",
):
    asset = Asset(
        resource_type=kind,
        resource_id=rid,
        resource_version=version,
        owner_type=owner_type,
        owner_id=owner_id,
        checksum_sha256=checksum,
        payload={
            "nacos_name": rid,
            "display_name": "Report",
            "description": "Demo",
            "is_skill_market_visible": True,
        },
    )
    async with storage.transaction() as conn:
        await conn.execute(
            t.assets.insert().values(
                **asset.model_dump(), created_by="test", updated_by="test"
            )
        )
    return asset


@pytest.mark.asyncio
async def test_system_catalog_denied_install_not_grant_and_exact_install_version(
    storage, tmp_path
):
    _, admin, alice, _ = await identities(storage)
    source = Registry()
    resources, http = await service(storage, tmp_path, source)
    async with http:
        await put_asset(storage)
        view = (await resources.list(alice, "skill"))[0]
        assert view["is_usable"] is False and view["is_editable"] is False
        with pytest.raises(Forbidden):
            await resources.install(alice, "skill", "report", "1.0.0")
        await PermissionService(storage).grant(
            admin,
            Grant(
                principal_type="user",
                principal_id="alice",
                resource_type="skill",
                resource_id="report",
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="demo",
            ),
        )
        installed = await resources.install(alice, "skill", "report", "1.0.0")
        assert installed["is_installed"] and installed["is_enabled"]
        assert len(await storage.list_skills("alice")) == 1
        assert len(await PermissionService(storage).list_grants(admin)) == 1
        await resources.set_installation(alice, "skill", "report", "1.0.0", False)
        assert (await resources.detail(alice, "skill", "report", "1.0.0"))[
            "is_enabled"
        ] is False


@pytest.mark.asyncio
async def test_own_private_publish_immutable_concurrent_save_and_other_owner_denied(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    source = Registry()
    resources, http = await service(storage, tmp_path, source)
    data = {
        "resource_id": "own-report",
        "resource_version": "1.0.0",
        "display_name": "Own",
        "description": "Private",
        "markdown": "# Facts",
        "is_skill_market_visible": False,
    }
    async with http:
        results = await asyncio.gather(
            resources.save_skill(alice, data),
            resources.save_skill(alice, data),
            return_exceptions=True,
        )
        assert sum(isinstance(r, Conflict) for r in results) == 1
        own = next(r for r in results if isinstance(r, dict))
        assert own["is_editable"] and own["is_usable"]
        assert any(r.url.path.endswith("/force-publish") for r in source.calls)
        assert (await resources.list(bob, "skill")) == []
        with pytest.raises(Forbidden):
            await resources.save_skill(bob, {**data, "resource_version": "1.1.0"})
        with pytest.raises(Conflict):
            await resources.save_skill(alice, data)
        async with storage.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        select(t.assets).where(t.assets.c.resource_id == "own-report")
                    )
                )
                .mappings()
                .one()
            )
        assert "markdown" not in row["payload"]
        assert row["checksum_sha256"]


@pytest.mark.asyncio
async def test_materialization_preserves_files_checks_checksum_and_exact_inherited_scope(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    source = Registry()
    resources, http = await service(storage, tmp_path, source)
    await put_asset(storage, checksum=skill_archive_digest(archive()))
    from egis_agentkit.models import Definition
    from egis_agentkit.services import AgentService
    from egis_agentkit.service.access.snapshot import SnapshotService

    root = await AgentService(storage).save(
        admin,
        Definition(
            owner_type="system",
            owner_id="sys",
            agent_name="expert",
            agent_display_name="专家",
            payload={
                "bindings": [
                    {
                        "resource_type": "skill",
                        "resource_id": "report",
                        "resource_version": "1.0.0",
                        "resource_view_type": "system",
                    }
                ]
            },
        ),
    )
    snapshot = await SnapshotService(storage).capture(root["id"], "system")
    scope = SimpleNamespace(user_id="alice", snapshot=snapshot, definition=root)
    async with http:
        with pytest.raises(Forbidden):
            await resources.materialize_skill(alice, "report", "1.0.0")
        await PermissionService(storage).grant(
            admin,
            Grant(
                principal_type="user",
                principal_id="alice",
                resource_type="agent",
                resource_id=root["id"],
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="sys",
            ),
        )
        materialized = await resources.materialize_skill(
            alice, "report", "1.0.0", authorization_scope=scope
        )
        assert (
            __import__("pathlib").Path(materialized.dir) / "references/checklist.md"
        ).read_text() == "Actual attached content"
        with pytest.raises(Forbidden):
            await resources.materialize_skill(
                bob, "report", "1.0.0", authorization_scope=scope
            )
        await put_asset(storage, version="1.1.0")
        with pytest.raises(Forbidden):
            await resources.materialize_skill(
                alice, "report", "1.1.0", authorization_scope=scope
            )
        scope.snapshot["nodes"][snapshot["root_key"]]["edges"] = []
        with pytest.raises(Forbidden):
            await resources.materialize_skill(
                alice, "report", "1.0.0", authorization_scope=scope
            )


@pytest.mark.asyncio
async def test_hostile_skill_archives_and_command_mcp_rejected(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    source = Registry()
    resources, http = await service(storage, tmp_path, source)
    async with http:
        with pytest.raises(ValueError):
            await resources.upload_skill(
                alice, archive(extra="../../outside.txt"), "1.0.0"
            )
        with pytest.raises(ValueError):
            await resources.save_mcp(
                alice,
                {
                    "resource_id": "bad",
                    "resource_version": "1.0.0",
                    "config": {"transport": "stdio", "command": "sh"},
                },
            )
        with pytest.raises(ValueError):
            await resources.save_mcp(
                alice,
                {
                    "resource_id": "bad",
                    "resource_version": "1.0.0",
                    "config": {
                        "transport": "streamable_http",
                        "url": "http://user:password@host/mcp",
                    },
                },
            )


@pytest.mark.asyncio
async def test_https_mcp_keeps_transport_when_registry_omits_endpoint_protocol(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    source = Registry()
    resources, http = await service(storage, tmp_path, source)
    async with http:
        saved = await resources.save_mcp(
            alice,
            {
                "resource_id": "secure",
                "resource_version": "1.0.0",
                "config": {
                    "transport": "streamable_http",
                    "url": "https://tools.example.com/mcp",
                },
            },
        )
        clients = await resources.mcps_for_refs(
            alice,
            [
                ResourceRef(
                    resource_type="mcp", resource_id="secure", resource_version="1.0.0"
                )
            ],
        )
        assert clients[0].mcp_config.url == "https://tools.example.com:443/mcp"
        assert saved["is_usable"] and saved["is_editable"]


@pytest.mark.asyncio
async def test_shared_resource_uses_only_approved_asset_version(storage, tmp_path):
    _, admin, alice, bob = await identities(storage)
    source = Registry()
    resources, http = await service(storage, tmp_path, source)
    async with http:
        await resources.save_skill(
            alice,
            {
                "resource_id": "shared-report",
                "resource_version": "1.0.0",
                "markdown": "# Approved",
            },
        )
        await resources.save_skill(
            alice,
            {
                "resource_id": "shared-report",
                "resource_version": "1.1.0",
                "markdown": "# Private newer",
            },
        )
        asset = await resources._asset("skill", "shared-report", "1.0.0")
        from egis_agentkit.models import ShareReview

        approved = ShareReview(
            resource_type="skill",
            resource_id="shared-report",
            owner_id="alice",
            review_status_type="approved",
            is_current=True,
            shared_version=1,
            source_versions={"shared-report": "1.0.0"},
            snapshot={"asset": {k: asset[k] for k in Asset.model_fields}},
            submitted_by="alice",
            reviewed_by="admin",
        )
        async with storage.transaction() as conn:
            await conn.execute(t.reviews.insert().values(**approved.model_dump()))
        await PermissionService(storage).grant(
            admin,
            Grant(
                principal_type="user",
                principal_id="bob",
                resource_type="skill",
                resource_id="shared-report",
                resource_view_type="shared",
                resource_owner_type="user",
                resource_owner_id="alice",
            ),
        )
        assert (await resources.list(bob, "skill"))[0]["resource_version"] == "1.0.0"
        assert (await resources.detail(bob, "skill", "shared-report", "1.0.0"))[
            "is_usable"
        ]
        with pytest.raises(Forbidden):
            await resources.detail(bob, "skill", "shared-report", "1.1.0")
        # Native callback lacks version; it must not admit a private-newer guess.
        assert not await resources.authorize_hub(
            "bob", "skill", asset["payload"]["nacos_name"], "download"
        )
        installed = await resources.install(bob, "skill", "shared-report", "1.0.0")
        assert installed["is_installed"]


@pytest.mark.asyncio
async def test_materialize_failure_never_exposes_partial_cache(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    source = Registry()
    resources, http = await service(storage, tmp_path, source)
    await put_asset(storage, owner_type="user", owner_id="alice", checksum="0" * 64)
    async with http:
        with pytest.raises(Conflict):
            await resources.materialize_skill(alice, "report", "1.0.0")
        assert not (tmp_path / "common/skills/report").exists()
        source.files[("report", "1.0.0")] = archive(extra="../../outside.txt")
        with pytest.raises(ValueError):
            await resources.materialize_skill(alice, "report", "1.0.0")
        assert not (tmp_path / "outside.txt").exists()


@pytest.mark.asyncio
@pytest.mark.skipif(
    __import__("os").environ.get("AGENTKIT_RESOURCE_LIVE_NACOS") != "1",
    reason="explicit real Nacos/MCP acceptance",
)
async def test_live_private_save_skill_mcp_and_native_probe(storage, tmp_path):
    from uuid import uuid4

    _, _, alice, _ = await identities(storage)
    suffix = uuid4().hex[:12]
    client = NacosClient(
        NacosBootstrap(
            server_url="http://127.0.0.1:8848", console_url="http://127.0.0.1:8080"
        )
    )
    manager = NacosHubManagement(client)
    resources = ResourceService(
        storage, NacosSkillHub(client), NacosMCPHub(client), manager, tmp_path
    )
    resources.skill_hub.authorize = resources.authorize_hub
    resources.mcp_hub.authorize = resources.authorize_hub
    created = []
    async with client:
        try:
            skill = await resources.save_skill(
                alice,
                {
                    "resource_id": "live-report-" + suffix,
                    "resource_version": "1.0.0",
                    "display_name": "Live private report",
                    "description": "Actual source acceptance",
                    "markdown": "# Actual private report",
                    "is_skill_market_visible": False,
                },
            )
            asset = await resources._asset("skill", skill["resource_id"], "1.0.0")
            created.append(("skill", asset["payload"]["nacos_name"]))
            assert skill["is_usable"] and skill["is_editable"]
            materialized = await resources.materialize_skill(
                alice, skill["resource_id"], "1.0.0"
            )
            assert "# Actual private report" in materialized.markdown
            uploaded = await resources.upload_skill(alice, archive(), "1.0.0", False)
            asset = await resources._asset("skill", uploaded["resource_id"], "1.0.0")
            created.append(("skill", asset["payload"]["nacos_name"]))
            materialized = await resources.materialize_skill(
                alice, uploaded["resource_id"], "1.0.0"
            )
            from pathlib import Path

            assert (
                Path(materialized.dir) / "references/checklist.md"
            ).read_text() == "Actual attached content"
            mcp = await resources.save_mcp(
                alice,
                {
                    "resource_id": "live-tools-" + suffix,
                    "resource_version": "1.0.0",
                    "display_name": "Live private MCP",
                    "config": {
                        "transport": "streamable_http",
                        "url": "http://127.0.0.1:8766/mcp",
                    },
                },
            )
            asset = await resources._asset("mcp", mcp["resource_id"], "1.0.0")
            created.append(("mcp", asset["payload"]["nacos_name"]))
            secure = await resources.save_mcp(
                alice,
                {
                    "resource_id": "live-https-" + suffix,
                    "resource_version": "1.0.0",
                    "config": {
                        "transport": "streamable_http",
                        "url": "https://tools.example.com/mcp",
                    },
                },
            )
            asset = await resources._asset("mcp", secure["resource_id"], "1.0.0")
            created.append(("mcp", asset["payload"]["nacos_name"]))
            secure_clients = await resources.mcps_for_refs(
                alice,
                [
                    ResourceRef(
                        resource_type="mcp",
                        resource_id=secure["resource_id"],
                        resource_version="1.0.0",
                    )
                ],
            )
            assert (
                secure_clients[0].mcp_config.url == "https://tools.example.com:443/mcp"
            )
            installed = await resources.install(
                alice, "mcp", mcp["resource_id"], "1.0.0"
            )
            assert installed["is_installed"]
            probe = await resources.test_mcp(alice, mcp["resource_id"], "1.0.0")
            assert probe["is_connected"]
            assert any(t["name"].endswith("sum_numbers") for t in probe["tools"])
            from sqlalchemy import func

            async with storage.engine.connect() as connection:
                assert (
                    await connection.scalar(select(func.count()).select_from(t.grants))
                    == 0
                )
            print(
                "REAL_PRIVATE_RESOURCE_ACCEPTANCE: skill text+attachment checksum verified; MCP native list_tools connected; no grants created"
            )
        finally:
            for kind, name in created:
                await (
                    manager.delete_skill(name)
                    if kind == "skill"
                    else manager.delete_mcp(name)
                )


@pytest.mark.asyncio
async def test_oversized_download_closes_upstream_stream(storage, tmp_path):
    from agentscope.app.hub._skill._base import SkillArchive

    _, _, alice, _ = await identities(storage)
    source = Registry()
    resources, http = await service(storage, tmp_path, source)
    await put_asset(storage, owner_type="user", owner_id="alice")
    resources.policies = SimpleNamespace(max_skill_archive_bytes=4)
    closed = []

    async def stream():
        try:
            yield b"oversized"
        finally:
            closed.append(True)

    async def download(*args):
        return SkillArchive("zip", stream())

    resources.skill_hub.download = download
    async with http:
        with pytest.raises(ValueError):
            await resources.materialize_skill(alice, "report", "1.0.0")
        assert closed == [True]


@pytest.mark.asyncio
async def test_mcp_fails_closed_if_registry_endpoint_changes_or_checksum_missing(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    source = Registry()
    resources, http = await service(storage, tmp_path, source)
    async with http:
        await resources.save_mcp(
            alice,
            {
                "resource_id": "pinned",
                "resource_version": "1.0.0",
                "config": {
                    "transport": "streamable_http",
                    "url": "https://tools.example.com/mcp",
                },
            },
        )
        ref = ResourceRef(
            resource_type="mcp", resource_id="pinned", resource_version="1.0.0"
        )
        assert (await resources.mcps_for_refs(alice, [ref]))[
            0
        ].mcp_config.url == "https://tools.example.com:443/mcp"
        asset = await resources._asset("mcp", "pinned", "1.0.0")
        source.mcps[(asset["payload"]["nacos_name"], "1.0.0")]["backendEndpoints"][0][
            "address"
        ] = "attacker.example.com"
        with pytest.raises(Conflict):
            await resources.mcps_for_refs(alice, [ref])
        with pytest.raises(Conflict):
            await resources.install(alice, "mcp", "pinned", "1.0.0")
        async with storage.transaction() as conn:
            await conn.execute(
                t.assets.update()
                .where(t.assets.c.resource_id == "pinned")
                .values(checksum_sha256="")
            )
        with pytest.raises(Conflict):
            await resources.mcps_for_refs(alice, [ref])


def test_team_scope_only_contains_current_agent_subgraph():
    resources = ResourceService(None, None, None, None, ".")
    asset = {
        "resource_type": "skill",
        "resource_id": "other",
        "resource_version": "1.0.0",
        "owner_type": "system",
        "owner_id": "sys",
        "checksum_sha256": "hash",
    }
    nodes = {
        "team": {"edges": ["first", "second"]},
        "first": {"edges": []},
        "second": {"edges": ["asset"]},
        "asset": {
            "resource_type": "skill",
            "resource_id": "other",
            "data": asset,
            "edges": [],
        },
    }
    scope = SimpleNamespace(
        user_id="alice",
        snapshot={"root_key": "team", "nodes": nodes},
        binding={"agent_node_key": "first"},
    )
    assert not resources._scope_allows(SimpleNamespace(user_id="alice"), asset, scope)


@pytest.mark.asyncio
async def test_download_skill_authorizes_exact_version_and_checks_archive(
    storage, tmp_path
):
    _, admin, alice, _ = await identities(storage)
    source = Registry()
    resources, http = await service(storage, tmp_path, source)
    await put_asset(storage, checksum=skill_archive_digest(archive()))
    async with http:
        with pytest.raises(Forbidden):
            await resources.download_skill(alice, "report", "1.0.0")
        await PermissionService(storage).grant(
            admin,
            Grant(
                principal_type="user",
                principal_id="alice",
                resource_type="skill",
                resource_id="report",
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="demo",
            ),
        )
        content = await resources.download_skill(alice, "report", "1.0.0")
        assert (
            zipfile.ZipFile(io.BytesIO(content))
            .read("report/SKILL.md")
            .endswith(b"# Report")
        )
        source.files[("report", "1.0.0")] = archive(extra="changed.md")
        with pytest.raises(Conflict):
            await resources.download_skill(alice, "report", "1.0.0")
