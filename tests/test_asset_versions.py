"""验证资源版本授权和 Agent 工具的目录边界。"""

import pytest
from pydantic import ValidationError
from test_domain import identities

from egis_agentkit.bootstrap._demo_tools import deploy_demo_report_tools
from egis_agentkit.errors import Forbidden
from egis_agentkit.service._user import UserService
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset
from egis_agentkit.storage.project._files import ProjectFileStorage


def test_skill_asset_requires_publication_revision():
    with pytest.raises(
        ValidationError, match="Skill assets require a positive publication revision"
    ):
        Asset(
            resource_type="skill",
            resource_id="missing-revision",
            resource_version="1.0.0",
            owner_type="user",
            owner_id="alice",
        )


async def test_current_storage_rejects_skill_without_publication_revision(storage):
    async with storage.engine.begin() as conn:
        await conn.execute(
            t.assets.insert().values(
                resource_type="skill",
                resource_id="missing-revision",
                resource_version="1.0.0",
                owner_type="user",
                owner_id="alice",
                checksum_sha256="",
                is_enabled=True,
                payload={},
                created_by="test",
                updated_by="test",
            )
        )
    with pytest.raises(
        ValueError, match="Skill assets require a positive publication revision"
    ):
        await storage.assets.get("skill", "missing-revision", "1.0.0")
    with pytest.raises(
        ValueError, match="Skill assets require a positive publication revision"
    ):
        await storage.assets.find_asset("skill", "missing-revision")


async def test_disabling_one_skill_edition_keeps_stable_owner_grant(storage, tmp_path):
    users = UserService(storage)
    await users.sync_identity("alice", "小明")
    await users.sync_identity("admin", "管理员")
    await users.provision_admin("admin", actor="bootstrap")
    alice, admin = await users.get("alice"), await users.get("admin")
    resources = ResourceService(storage, tmp_path)
    identity = "versions"
    data = {"resource_id": identity, "markdown": "# Report"}
    await resources.save_skill(alice, {**data, "resource_version": "1.0.0"})
    await resources.save_skill(
        alice,
        {
            **data,
            "resource_version": "1.1.0",
            "base_version": "1.0.0",
        },
    )
    await resources.set_asset_enabled(alice, "skill", identity, "1.0.0", False)
    grants = await resources.grants.list_grants(admin)
    (grant,) = [row for row in grants if row["resource_id"] == identity]
    assert (grant["principal_id"], grant["grant_source_type"]) == ("alice", "owner")
    assert grant["revoked_at"] is None
    assert "resource_version" not in grant


async def test_tool_catalog_distinguishes_platform_and_agent_tools(storage, tmp_path):
    users = UserService(storage)
    await users.sync_identity("alice", "小明")
    alice = await users.get("alice")
    resources = ResourceService(storage, tmp_path)
    deploy_demo_report_tools(resources.tool_registry, ProjectFileStorage(tmp_path))
    await resources.seed_builtin_tools()
    cards = {row["resource_id"]: row for row in await resources.list(alice, "tool")}
    assert cards["Read"]["is_system_default"]
    assert cards["Read"]["version_label"] == cards["Read"]["resource_version"]
    assert not cards["Read"]["is_agent_tool"]
    assert cards["CreateDemoReport"]["is_agent_tool"]
    assert not cards["CreateDemoReport"]["is_installed"]
    assert "AgentInvite" not in cards
    with pytest.raises(Forbidden):
        await resources.install(alice, "tool", "CreateDemoReport")


@pytest.mark.asyncio
async def test_system_grant_exposes_current_skill_version(storage, tmp_path):
    from egis_agentkit.service.access._grant import GrantService
    from egis_agentkit.storage.model import Grant

    _, admin, _, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    first = {
        "resource_id": "system-skill",
        "resource_version": "2.0.0",
        "owner_type": "system",
        "markdown": "# First",
    }
    await resources.save_skill(admin, first)
    await resources.save_skill(
        admin, {**first, "resource_version": "1.0.0", "base_version": "2.0.0"}
    )
    assert [
        (row["resource_version"], row["version_label"])
        for row in await resources.list_asset_versions(admin, "skill", "system-skill")
    ] == [("1.0.0", "1.1"), ("2.0.0", "1.0")]
    assert await resources.list(bob, "skill") == []
    await GrantService(storage).grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=bob.user_id,
            resource_type="skill",
            resource_id="system-skill",
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="system",
        ),
    )
    (card,) = await resources.list(bob, "skill")
    assert card["resource_version"] == "1.0.0"
    assert card["is_usable"]
    versions = await resources.list_asset_versions(bob, "skill", "system-skill")
    assert [row["resource_version"] for row in versions] == ["1.0.0"]
