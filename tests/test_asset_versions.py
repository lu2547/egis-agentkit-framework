"""验证资源版本授权和 Agent 工具的目录边界。"""

import pytest
from test_domain import identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.extension.demo._demo_report_tool_impl import deploy_demo_report_tools
from egis_agentkit.service._user import UserService
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.project._files import ProjectFileStorage


@pytest.mark.parametrize("kind", ["skill", "mcp"])
async def test_disabling_one_asset_edition_keeps_stable_owner_grant(
    storage, tmp_path, kind
):
    users = UserService(storage)
    await users.sync_identity("alice", "小明")
    await users.sync_identity("admin", "管理员")
    await users.provision_admin("admin", actor="bootstrap")
    alice, admin = await users.get("alice"), await users.get("admin")
    resources = ResourceService(storage, tmp_path)
    if kind == "skill":
        data = {"markdown": "# Report"}
        save = resources.save_skill
    else:
        data = {
            "config": {"transport": "streamable_http", "url": "http://example.com/mcp"}
        }
        save = resources.save_mcp
    identity = "versions"
    await save(alice, {**data, "resource_id": identity, "resource_version": "1.0.0"})
    await save(
        alice,
        {
            **data,
            "resource_id": identity,
            "resource_version": "1.1.0",
            "base_version": "1.0.0",
        },
    )
    await resources.set_asset_enabled(alice, kind, identity, "1.0.0", False)
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
    assert not cards["Read"]["is_agent_tool"]
    assert cards["CreateDemoReport"]["is_agent_tool"]
    assert not cards["CreateDemoReport"]["is_installed"]
    assert "AgentInvite" not in cards
    with pytest.raises(Forbidden):
        await resources.install(alice, "tool", "CreateDemoReport")


@pytest.mark.asyncio
async def test_system_grant_exposes_current_catalog_version(storage, tmp_path):
    from egis_agentkit.service.access._grant import GrantService
    from egis_agentkit.storage.model import Grant

    _, admin, _, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    first = {
        "resource_id": "shared-connector",
        "resource_version": "1.0.0",
        "owner_type": "system",
        "config": {"transport": "streamable_http", "url": "https://mcp.example.com/"},
    }
    await resources.save_mcp(admin, first)
    await resources.save_mcp(
        admin, {**first, "resource_version": "2.0.0", "base_version": "1.0.0"}
    )
    assert await resources.list(bob, "mcp") == []
    await GrantService(storage).grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=bob.user_id,
            resource_type="mcp",
            resource_id="shared-connector",
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="system",
        ),
    )
    (card,) = await resources.list(bob, "mcp")
    assert card["resource_version"] == "2.0.0"
    assert card["is_usable"]
    versions = await resources.list_asset_versions(bob, "mcp", "shared-connector")
    assert [row["resource_version"] for row in versions] == ["2.0.0"]
