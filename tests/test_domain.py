"""Regressions pin enterprise authorization and sharing behavior, not SQL shape."""

import asyncio

import pytest

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.models import Definition
from egis_agentkit.services import AgentService, PermissionService, ShareService, UserService


async def identities(storage):
    users = UserService(storage)
    await users.sync_identity("admin", "管理员")
    # Role provisioning is a trusted bootstrap operation, not a request field.
    await users.provision_admin("admin", actor="bootstrap")
    await users.sync_identity("alice", "小明")
    await users.sync_identity("bob", "小明")
    return (
        users,
        await users.get("admin"),
        await users.get("alice"),
        await users.get("bob"),
    )


def expert(owner="alice", owner_type="user", name="writer"):
    return Definition(
        owner_type=owner_type,
        owner_id=owner,
        agent_name=name,
        agent_display_name="写作专家",
        payload={"system_prompt": "旧版", "starter_prompts": ["写一份报告"]},
    )


@pytest.mark.asyncio
async def test_login_does_not_overwrite_admin_or_duplicate_personal(storage):
    users, admin, alice, _ = await identities(storage)
    await users.sync_identity("admin", "新姓名")
    assert (await users.get("admin")).role_type == "admin"
    service = AgentService(storage)
    rows = await asyncio.gather(*(service.personal(alice) for _ in range(6)))
    assert len({row["id"] for row in rows}) == 1


@pytest.mark.asyncio
async def test_system_register_defaults_denied_including_admin(storage):
    _, admin, alice, _ = await identities(storage)
    agents = AgentService(storage)
    saved = await agents.save(admin, expert("egis-demo", "system"))
    permissions = PermissionService(storage)
    assert not (
        await permissions.explain(alice, "agent", saved["id"], "system")
    ).is_allowed
    assert not (
        await permissions.explain(admin, "agent", saved["id"], "system")
    ).is_allowed
    assert (
        await permissions.explain(
            alice, "agent", (await agents.save(alice, expert()))["id"], "private"
        )
    ).is_allowed


@pytest.mark.asyncio
async def test_own_save_needs_no_review_shared_snapshot_is_immutable(storage):
    _, admin, alice, bob = await identities(storage)
    agents, shares = AgentService(storage), ShareService(storage)
    source = await agents.save(alice, expert())
    assert (await agents.resolve(alice, source["id"], "private"))["payload"][
        "system_prompt"
    ] == "旧版"
    with pytest.raises(Forbidden):
        await agents.resolve(bob, source["id"], "shared")
    review = await shares.submit(alice, "agent", source["id"])
    approved = await shares.decide(admin, review["review_id"], True, "通过")
    changed = expert()
    changed.id, changed.config_version = source["id"], source["config_version"]
    changed.payload["system_prompt"] = "未审核的新内容"
    await agents.save(alice, changed)
    assert (await agents.resolve(bob, source["id"], "shared"))["payload"][
        "system_prompt"
    ] == "旧版"
    again = await shares.decide(admin, review["review_id"], True, "重试")
    assert again["shared_version"] == approved["shared_version"] == 1
    assert len(await PermissionService(storage).list_grants(admin)) == 1


@pytest.mark.asyncio
async def test_compare_and_swap_prevents_lost_updates_and_owner_spoofing(storage):
    _, _, alice, bob = await identities(storage)
    agents = AgentService(storage)
    row = await agents.save(alice, expert())
    changed = expert()
    changed.id = row["id"]
    changed.payload["system_prompt"] = "已修改"
    await agents.save(alice, changed)
    with pytest.raises(Conflict):
        await agents.save(alice, changed)
    with pytest.raises(Forbidden):
        await agents.save(bob, changed)


@pytest.mark.asyncio
async def test_plain_user_cannot_register_code_or_grant(storage):
    _, _, alice, _ = await identities(storage)
    payload = expert()
    payload.payload["implementation"] = {
        "assembly_ref": "malicious",
        "assembly_version": "1",
    }
    with pytest.raises(Forbidden):
        await AgentService(storage).save(alice, payload)
    with pytest.raises(Forbidden):
        await PermissionService(storage).list_grants(alice)
