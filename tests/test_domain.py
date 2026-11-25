"""Regressions pin enterprise authorization and sharing behavior, not SQL shape."""

import asyncio

import pytest
from sqlalchemy import text

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service._user import UserService
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.storage.model import AgentDefinition, Group, ShareTarget
from egis_agentkit.storage.model.agent._update import AgentUpdate


def agent_changes(definition):
    """测试管理更新时只提交允许编辑的配置。"""
    return AgentUpdate(**definition.model_dump(include=set(AgentUpdate.model_fields)))


COMPANY = ShareTarget(principal_type="group", principal_id="company")


async def identities(storage):
    users = UserService(storage)
    await users.sync_identity("admin", "管理员")
    # Role provisioning is a trusted bootstrap operation, not a request field.
    await users.provision_admin("admin", actor="bootstrap")
    await users.sync_identity("alice", "小明")
    await users.sync_identity("bob", "小明")
    admin = await users.get("admin")
    await users.save_group(admin, Group(group_id="company", group_name="全公司"))
    for user_id in ("alice", "bob"):
        user = await users.get(user_id)
        await users.update_user(
            admin, user.model_copy(update={"group_ids": ["company"]})
        )
    return (
        users,
        await users.get("admin"),
        await users.get("alice"),
        await users.get("bob"),
    )


def expert(owner="alice", owner_type="user", name="writer"):
    return AgentDefinition(
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
    service = PersonalManager(storage)
    rows = await asyncio.gather(*(service.get_or_add_definition(alice) for _ in range(6)))
    assert len({row["id"] for row in rows}) == 1


@pytest.mark.asyncio
async def test_system_register_defaults_denied_including_admin(storage):
    _, admin, alice, _ = await identities(storage)
    agents = AgentManager(storage)
    saved = await agents.add_definition(admin, expert("egis-demo", "system"))
    permissions = PermissionService(storage)
    assert not (
        await permissions.explain(alice, "agent", saved["id"], "system")
    ).is_allowed
    assert not (
        await permissions.explain(admin, "agent", saved["id"], "system")
    ).is_allowed
    assert (
        await permissions.explain(
            alice, "agent", (await agents.add_definition(alice, expert()))["id"], "private"
        )
    ).is_allowed


@pytest.mark.asyncio
async def test_own_save_needs_no_review_shared_version_is_immutable(storage):
    _, admin, alice, bob = await identities(storage)
    agents, shares = AgentManager(storage), ShareService(storage)
    source = await agents.add_definition(alice, expert())
    assert (await AgentManager(storage).get_definition_for_use(alice, source["id"], "private"))[
        "payload"
    ]["system_prompt"] == "旧版"
    with pytest.raises(Forbidden):
        await AgentManager(storage).get_definition_for_use(bob, source["id"], "shared")
    review = await shares.submit(alice, "agent", source["id"], target=COMPANY)
    approved = await shares.decide(admin, review["review_id"], True, "通过")
    changed = expert()
    changed.id, changed.owner_version = source["id"], source["owner_version"]
    changed.payload["system_prompt"] = "未审核的新内容"
    await agents.update_definition(alice, (changed).id, agent_changes(changed))
    assert (await AgentManager(storage).get_definition_for_use(bob, source["id"], "shared"))[
        "payload"
    ]["system_prompt"] == "旧版"
    again = await shares.decide(admin, review["review_id"], True, "重试")
    assert again["resource_version"] == approved["resource_version"] == "1"
    grants = await GrantService(storage).list_grants(admin)
    assert len([row for row in grants if row["grant_source_type"] == "owner"]) == 1
    assert all("resource_version" not in row for row in grants)
    assert len([row for row in grants if row["grant_source_type"] == "share"]) == 1


@pytest.mark.asyncio
async def test_failed_approval_keeps_previous_review_and_grant_current(storage):
    _, admin, alice, _ = await identities(storage)
    agents, shares = AgentManager(storage), ShareService(storage)
    value = expert()
    source = await agents.add_definition(alice, value)
    previous = await shares.submit(alice, "agent", source["id"], target=COMPANY)
    await shares.decide(admin, previous["review_id"], True, "通过")
    value.payload["description"] = "待审核的内容修改"
    await agents.update_definition(alice, (value).id, agent_changes(value))
    pending = await shares.submit(alice, "agent", source["id"])
    async with storage.transaction() as conn:
        await conn.execute(
            text(
                "ALTER TABLE ak_approve ADD CONSTRAINT test_reject_review_reason "
                "CHECK (review_reason <> 'force failure')"
            )
        )

    with pytest.raises(Conflict):
        await shares.decide(admin, pending["review_id"], True, "force failure")

    reviews = {review["review_id"]: review for review in await shares.list(alice)}
    assert reviews[previous["review_id"]]["is_current"]
    assert reviews[pending["review_id"]]["review_status_type"] == "pending"
    grants = await GrantService(storage).list_grants(admin)
    assert [
        grant["source_review_id"]
        for grant in grants
        if grant["grant_source_type"] == "share"
    ] == [previous["review_id"]]


@pytest.mark.asyncio
async def test_compare_and_swap_prevents_lost_updates_and_owner_spoofing(storage):
    _, _, alice, bob = await identities(storage)
    agents = AgentManager(storage)
    row = await agents.add_definition(alice, expert())
    changed = expert()
    changed.id = row["id"]
    changed.payload["system_prompt"] = "已修改"
    await agents.update_definition(alice, (changed).id, agent_changes(changed))
    with pytest.raises(Conflict):
        await agents.update_definition(alice, (changed).id, agent_changes(changed))
    with pytest.raises(Forbidden):
        await agents.update_definition(bob, (changed).id, agent_changes(changed))


@pytest.mark.asyncio
async def test_plain_user_cannot_grant_resources(storage):
    _, _, alice, _ = await identities(storage)
    with pytest.raises(Forbidden):
        await GrantService(storage).list_grants(alice)
