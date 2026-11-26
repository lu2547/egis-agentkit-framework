import asyncio
import io
import zipfile
from pathlib import Path
from types import SimpleNamespace

import pytest
from sqlalchemy import select
from test_domain import identities

from egis_agentkit.client.nacos._settings import SkillArchiveConfig
from egis_agentkit.core.skill._archive import (
    calculate_archive_digest,
    read_archive_files,
)
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset, Grant, ResourceRef, ShareTarget


def archive(name="report", extra="references/checklist.md"):
    out = io.BytesIO()
    with zipfile.ZipFile(out, "w") as z:
        z.writestr(
            name + "/SKILL.md",
            "---\nname: " + name + "\ndescription: Demo\n---\n# Report",
        )
        z.writestr(name + "/" + extra, "Actual attached content")
    return out.getvalue()


async def test_owner_deletes_skill_after_approved_withdrawal_and_revokes_all_versions(storage, tmp_path):
    import httpx
    from egis_agentkit_app.bootstrap._router import manager_router
    from egis_agentkit_app.controller.base import current_user
    from egis_agentkit_app.controller.base._exception import register_exception_handlers
    from fastapi import FastAPI

    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    shares = ShareService(storage)
    skill = await resources.save_skill(alice, {
        "resource_id": "daily-report",
        "markdown": "# Daily report",
    })
    rid, version = skill["resource_id"], skill["resource_version"]
    approved = await shares.submit(
        alice, "skill", rid, target=ShareTarget(principal_type="user", principal_id="bob")
    )
    await shares.decide(admin, approved["review_id"], True, "")
    pending = await shares.submit(
        alice, "skill", rid, target=ShareTarget(principal_type="group", principal_id="company")
    )
    await resources.install(bob, "skill", rid)
    await resources.save_skill(alice, {
        "resource_id": rid, "base_version": version, "resource_version": "1.1.0",
        "markdown": "# Daily report v2",
    })
    app = FastAPI()
    app.state.egis_services = SimpleNamespace(resources=resources)
    app.include_router(manager_router)
    register_exception_handlers(app)
    url = f"/manager/skills/{rid}"
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        app.dependency_overrides[current_user] = lambda: bob
        assert (await client.delete(url)).status_code == 403
        app.dependency_overrides[current_user] = lambda: alice
        assert (await client.delete(url)).status_code == 409
        withdrawal = await shares.withdraw(alice, approved["review_id"])
        assert (await client.delete(url)).status_code == 409
        assert (await resources.detail(bob, "skill", rid, version))["is_usable"]
        await shares.decide(admin, withdrawal["review_id"], True, "批准撤回后删除")
        assert (await client.delete(url)).status_code == 204
        assert (await client.delete(url)).status_code == 404
    assert not await resources.list(alice, "skill")
    assert not await resources.list(bob, "skill")
    assert not await resources.personal.list_resources(bob, "skill")
    with pytest.raises(NotFound):
        await resources.detail(alice, "skill", rid, version)
    with pytest.raises(Conflict):
        await resources.save_skill(alice, {"resource_id": rid, "markdown": "# Reuse"})
    assert all(
        row["revoked_at"] is not None for row in await GrantService(storage).list_grants(admin)
        if row["resource_id"] == rid
    )
    assert next(row for row in await shares.list(alice) if row["review_id"] == pending["review_id"])[
        "review_status_type"
    ] == "cancelled"
    async with storage.engine.connect() as conn:
        assert len((await conn.execute(
            select(t.skill_detail.c.version).where(t.skill_detail.c.skill_id == rid)
        )).all()) == 2
    assert (tmp_path / "user" / "alice" / "skills" / rid / version / "SKILL.md").exists()


async def put_asset(
    storage,
    rid="report",
    version="1.0.0",
    owner_type="system",
    owner_id="demo",
    checksum=None,
    revision=1,
):
    content = archive()
    files = read_archive_files(content)
    asset = Asset(
        resource_type="skill",
        resource_id=rid,
        resource_version=version,
        publication_revision=revision,
        owner_type=owner_type,
        owner_id=owner_id,
        checksum_sha256=calculate_archive_digest(content),
        payload={
            "files": [
                {"path": name, "size": len(value)}
                for name, value in sorted(files.items())
            ],
            "display_name": "Report",
            "description": "Demo",
        },
    )
    from egis_agentkit.storage.skill._version import SkillVersionStorage

    SkillVersionStorage(storage.test_workspace_root).publish(
        asset.model_dump(), files, max_bytes=SkillArchiveConfig().expanded_bytes_limit
    )
    if checksum is not None:
        asset = asset.model_copy(update={"checksum_sha256": checksum})
    async with storage.transaction() as conn:
        await storage.assets.insert_asset(asset, actor="test", conn=conn)
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
        await resources.install(alice, "skill", "report")
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
    (system_skill,) = await resources.list(alice, "skill")
    assert system_skill["is_usable"]
    assert system_skill["owner_display_name"] == "系统"
    (descriptor,) = await resources.skill_descriptors(
        alice,
        [
            {
                "resource_type": "skill",
                "resource_id": "report",
                "resource_version": "1.0.0",
                "resource_view_type": "system",
            }
        ],
    )
    assert system_skill["runtime_name"] == descriptor.name
    installed = await resources.install(alice, "skill", "report")
    assert installed["is_installed"] and installed["is_enabled"]
    assert (await resources.personal.list_resources(alice, "skill"))["report"][
        "is_enabled"
    ]
    assert len(await GrantService(storage).list_grants(admin)) == 1
    await resources.update_installation(alice, "skill", "report", False)
    detail = await resources.detail(alice, "skill", "report", "1.0.0")
    assert detail["is_enabled"] is False
    assert detail["owner_display_name"] == "系统"
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
                    select(t.skill_detail).where(
                        t.skill_detail.c.skill_id == "own-report"
                    )
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
    await put_asset(storage, checksum=calculate_archive_digest(archive()))
    from egis_agentkit.storage.model import AgentDefinition

    root = await AgentManager(storage).add_definition(
        admin,
        AgentDefinition(
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
    snapshot = await storage.definitions.get_definition_view(root["id"], "system")
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
        Path(materialized.dir) / "references/checklist.md"
    ).read_text() == "Actual attached content"
    with pytest.raises(Forbidden):
        await resources.materialize_skill(
            bob, "report", "1.0.0", authorization_scope=scope
        )
    await put_asset(storage, version="1.1.0", revision=2)
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
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    with pytest.raises(ValueError):
        await resources.upload_skill(alice, archive(extra="../../outside.txt"), "1.0.0")
    with pytest.raises(ValueError):
        await resources.save_mcp(
            admin,
            {
                "resource_id": "bad",
                "config": {"transport": "stdio", "command": "sh"},
            },
        )
    with pytest.raises(ValueError):
        await resources.save_mcp(
            admin,
            {
                "resource_id": "bad",
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
    # An existing version with the former display flag remains governed by grants.
    async with storage.engine.begin() as conn:
        detail = t.skill_detail
        entry = (detail.c.skill_id == "shared-report", detail.c.version == 1)
        payload = await conn.scalar(select(detail.c.payload).where(*entry))
        await conn.execute(
            detail.update()
            .where(*entry)
            .values(payload={**payload, "is_skill_market_visible": False})
        )
    (owned,) = await resources.list(alice, "skill")
    assert owned["resource_id"] == "shared-report"
    assert not owned["is_installed"]
    assert await resources.list(bob, "skill") == []
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
    (shared,) = await resources.list(bob, "skill")
    assert shared["resource_version"] == "1.0.0"
    assert (await resources.detail(bob, "skill", "shared-report", "1.0.0"))["is_usable"]
    with pytest.raises(Forbidden):
        await resources.detail(bob, "skill", "shared-report", "1.1.0")
    with pytest.raises(Forbidden):
        await resources.skill_hub.download("bob", "shared-report", "1.1.0")
    installed = await resources.install(bob, "skill", "shared-report")
    assert installed["is_installed"]


@pytest.mark.asyncio
async def test_materialize_refuses_corrupt_version(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage, owner_type="user", owner_id="alice", checksum="0" * 64)
    with pytest.raises(Conflict):
        await resources.materialize_skill(alice, "report", "1.0.0")


@pytest.mark.asyncio
async def test_skill_download_enforces_zip_limit(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage, owner_type="user", owner_id="alice")
    with pytest.raises(ValueError):
        await resources.download_skill(
            alice,
            "report",
            "1.0.0",
            archive_limits=SkillArchiveConfig(max_bytes=128, max_expanded_bytes=1024),
        )


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
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_mcp(
        admin,
        {
            "resource_id": "pinned",
            "config": {
                "transport": "streamable_http",
                "url": endpoint,
            },
        },
    )
    ref = ResourceRef(resource_type="mcp", resource_id="pinned")
    assert (await resources.mcps_for_refs(alice, [ref]))[0].mcp_config.url == endpoint
    asset = await resources._asset("mcp", "pinned", None)
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
        await resources.install(alice, "mcp", "pinned")
    with pytest.raises(Conflict):
        await resources.mcp_auth_action(alice, "pinned", "start")
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
    await put_asset(storage, checksum=calculate_archive_digest(archive()))
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
    (tmp_path / "common/skills/report/1.0.0/references/checklist.md").write_bytes(
        b"tampered"
    )
    with pytest.raises(Conflict):
        await resources.download_skill(alice, "report", "1.0.0")
