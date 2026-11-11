import asyncio
import io
import zipfile
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from test_domain import identities

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill import _archive_files, skill_archive_digest
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset, Grant, ResourceRef


def archive(name="report", extra="references/checklist.md"):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr(
            name + "/SKILL.md",
            "---\nname: " + name + "\ndescription: Demo\n---\n# Report",
        )
        z.writestr(name + "/" + extra, "Actual attached content")
    return out.getvalue()


async def put_asset(
    storage,
    rid="report",
    version="1.0.0",
    owner_type="system",
    owner_id="demo",
    checksum=None,
):
    content = archive()
    files = _archive_files(content)
    checksum = skill_archive_digest(content) if checksum is None else checksum
    asset = Asset(
        resource_type="skill",
        resource_id=rid,
        resource_version=version,
        owner_type=owner_type,
        owner_id=owner_id,
        checksum_sha256=checksum,
        payload={
            "original_markdown": files["SKILL.md"].decode(),
            "files": [
                {"path": name, "size": len(value)}
                for name, value in sorted(files.items())
            ],
            "display_name": "Report",
            "description": "Demo",
            "is_skill_market_visible": True,
        },
    )
    async with storage.transaction() as conn:
        await storage.assets.put_archive(checksum, content, conn=conn)
        await conn.execute(
            t.assets.insert().values(
                **asset.model_dump(), created_by="test", updated_by="test"
            )
        )
    return asset


@pytest.mark.asyncio
async def test_system_skill_catalog_and_install_require_exact_version_grant(
    storage, tmp_path
):
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage)
    assert await resources.list(alice, "skill") == []
    assert await resources.list_asset_versions(alice, "skill", "report") == []
    with pytest.raises(Forbidden):
        await resources.detail(alice, "skill", "report", "1.0.0")
    assert (await resources.list(admin, "skill"))[0]["is_editable"]
    with pytest.raises(Forbidden):
        await resources.installations.install(alice, "skill", "report", "1.0.0")
    grant = await GrantService(storage).grant(
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
    assert (await resources.list(alice, "skill"))[0]["is_usable"]
    installed = await resources.installations.install(alice, "skill", "report", "1.0.0")
    assert installed["is_installed"] and installed["is_enabled"]
    assert len(await storage.list_skills("alice")) == 1
    assert len(await GrantService(storage).list_grants(admin)) == 1
    await resources.installations.set_installation(alice, "skill", "report", "1.0.0", False)
    assert (await resources.detail(alice, "skill", "report", "1.0.0"))[
        "is_enabled"
    ] is False
    await GrantService(storage).revoke(admin, grant["id"])
    assert await resources.list(alice, "skill") == []
    assert await resources.list_asset_versions(alice, "skill", "report") == []


@pytest.mark.asyncio
async def test_own_private_publish_immutable_concurrent_save_and_other_owner_denied(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    data = {
        "resource_id": "own-report",
        "resource_version": "1.0.0",
        "display_name": "Own",
        "description": "Private",
        "markdown": "# Facts",
        "is_skill_market_visible": False,
    }
    results = await asyncio.gather(
        resources.save_skill(alice, data),
        resources.save_skill(alice, data),
        return_exceptions=True,
    )
    assert sum(isinstance(r, Conflict) for r in results) == 1
    own = next(r for r in results if isinstance(r, dict))
    assert own["is_editable"] and own["is_usable"]
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
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage, checksum=skill_archive_digest(archive()))
    from egis_agentkit.service.agent._expert_definition import ExpertDefinition
    from egis_agentkit.storage.model import Definition

    root = await ExpertDefinition(storage).build(
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
    snapshot = await storage.definitions.read_view(root["id"], "system")
    scope = SimpleNamespace(user_id="alice", root_definition=snapshot, definition=root)
    with pytest.raises(Forbidden):
        await resources.materialize_skill(alice, "report", "1.0.0")
    await GrantService(storage).grant(
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
    scope.definition = {**root, "payload": {"bindings": []}}
    with pytest.raises(Forbidden):
        await resources.materialize_skill(
            alice, "report", "1.0.0", authorization_scope=scope
        )


@pytest.mark.asyncio
async def test_hostile_skill_archives_and_command_mcp_rejected(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    with pytest.raises(ValueError):
        await resources.upload_skill(alice, archive(extra="../../outside.txt"), "1.0.0")
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
async def test_shared_resource_uses_only_approved_asset_version(storage, tmp_path):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_skill(
        alice,
        {
            "resource_id": "shared-report",
            "resource_version": "1.0.0",
            "markdown": "# Approved",
        },
    )
    from egis_agentkit.service.access._share import ShareService
    from egis_agentkit.storage.model import ShareTarget

    sharing = ShareService(storage)
    submitted = await sharing.submit(
        alice,
        "skill",
        "shared-report",
        target=ShareTarget(principal_type="user", principal_id="bob"),
    )
    await sharing.decide(admin, submitted["review_id"], True, "approved")
    await resources.save_skill(
        alice,
        {
            "resource_id": "shared-report",
            "resource_version": "1.1.0",
            "base_version": "1.0.0",
            "markdown": "# Private newer",
        },
    )
    assert (await resources.list(bob, "skill"))[0]["resource_version"] == "1.0.0"
    assert (await resources.detail(bob, "skill", "shared-report", "1.0.0"))["is_usable"]
    with pytest.raises(Forbidden):
        await resources.detail(bob, "skill", "shared-report", "1.1.0")
    with pytest.raises(Forbidden):
        await resources.skill_hub.download("bob", "shared-report", "1.1.0")
    installed = await resources.installations.install(bob, "skill", "shared-report", "1.0.0")
    assert installed["is_installed"]


@pytest.mark.asyncio
async def test_materialize_failure_never_exposes_partial_cache(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage, owner_type="user", owner_id="alice", checksum="0" * 64)
    with pytest.raises(Conflict):
        await resources.materialize_skill(alice, "report", "1.0.0")
    assert not (tmp_path / "common/skills/report").exists()
    async with storage.transaction() as conn:
        await conn.execute(
            t.skill_archives.update().values(archive=archive(extra="../../outside.txt"))
        )
    with pytest.raises(ValueError):
        await resources.materialize_skill(alice, "report", "1.0.0")
    assert not (tmp_path / "outside.txt").exists()


@pytest.mark.asyncio
async def test_oversized_download_closes_upstream_stream(storage, tmp_path):
    from agentscope.app.hub._skill._base import SkillArchive

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
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
    with pytest.raises(ValueError):
        await resources.materialize_skill(alice, "report", "1.0.0")
    assert closed == [True]


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "endpoint,changed_endpoint",
    [
        ("https://tools.example.com/mcp", "https://attacker.example.com/mcp"),
        ("https://tools.example.com", "https://tools.example.com/mcp"),
    ],
)
async def test_mcp_fails_closed_if_stored_endpoint_changes_or_checksum_missing(
    storage, tmp_path, endpoint, changed_endpoint
):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_mcp(
        alice,
        {
            "resource_id": "pinned",
            "resource_version": "1.0.0",
            "config": {
                "transport": "streamable_http",
                "url": endpoint,
            },
        },
    )
    ref = ResourceRef(
        resource_type="mcp", resource_id="pinned", resource_version="1.0.0"
    )
    assert (await resources.mcps_for_refs(alice, [ref]))[0].mcp_config.url == endpoint
    asset = await resources._asset("mcp", "pinned", "1.0.0")
    payload = asset["payload"]
    changed = {
        **payload,
        "config": {**payload["config"], "url": changed_endpoint},
    }
    async with storage.transaction() as conn:
        await conn.execute(
            t.assets.update()
            .where(t.assets.c.resource_id == "pinned")
            .values(payload=changed)
        )
    with pytest.raises(Conflict):
        await resources.mcps_for_refs(alice, [ref])
    with pytest.raises(Conflict):
        await resources.installations.install(alice, "mcp", "pinned", "1.0.0")
    async with storage.transaction() as conn:
        await conn.execute(
            t.assets.update()
            .where(t.assets.c.resource_id == "pinned")
            .values(checksum_sha256="", payload=payload)
        )
    with pytest.raises(Conflict):
        await resources.mcps_for_refs(alice, [ref])


def test_team_scope_only_uses_current_agent_bindings():
    asset = {
        "resource_type": "skill",
        "resource_id": "other",
        "resource_version": "1.0.0",
        "owner_type": "system",
        "owner_id": "sys",
        "checksum_sha256": "hash",
    }
    scope = SimpleNamespace(
        user_id="alice",
        definition={"id": "first", "payload": {"bindings": []}},
        root_definition={"definition_type": "team"},
        member_definitions={"second": {"payload": {"bindings": [asset]}}},
    )
    assert not ResourceService._scope_allows(
        SimpleNamespace(user_id="alice"), asset, scope
    )


@pytest.mark.asyncio
async def test_download_skill_authorizes_exact_version_and_checks_archive(
    storage, tmp_path
):
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage, checksum=skill_archive_digest(archive()))
    with pytest.raises(Forbidden):
        await resources.download_skill(alice, "report", "1.0.0")
    await GrantService(storage).grant(
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
    async with storage.transaction() as conn:
        await conn.execute(
            t.skill_archives.update().values(archive=archive(extra="changed.md"))
        )
    with pytest.raises(Conflict):
        await resources.download_skill(alice, "report", "1.0.0")
