"""Definition detail and team authoring reuse current scoped authorization rows."""

import pytest
from sqlalchemy import update
from test_domain import COMPANY, agent_changes, expert, identities
from test_permission_queries import queries

from egis_agentkit.errors import Forbidden
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.storage import tables as t


def team_data(members, *, name="delivery"):
    return {
        "team_name": name,
        "team_display_name": name,
        "orchestrator_config": {
            "agent_name": f"leader-{name}",
            "agent_display_name": "Leader",
        },
        "members": [
            {"definition_id": member["id"], "resource_view_type": "private"}
            for member in members
        ],
    }


@pytest.mark.parametrize("count", [1, 8])
async def test_team_save_read_and_list_batch_member_references(storage, count):
    _, _, alice, _ = await identities(storage)
    agents, teams = AgentManager(storage), TeamDefinition(storage)
    members = [
        await agents.add_definition(alice, expert(name=f"writer{index}"))
        for index in range(count)
    ]
    data = team_data(members)
    with queries(storage) as selected:
        root = await teams.save(alice, data)
    print(f"DEFINITION_SQL team_save members={count} queries={len(selected)}")
    assert len(root["payload"]["members"]) == count
    assert all(ref["resource_version"] == "1" for ref in root["payload"]["members"])
    assert len(selected) <= 23, len(selected)
    with queries(storage) as selected:
        detail = await teams.read(alice, root["id"])
    assert detail["orchestrator_config"]["agent_name"] == "leader-delivery"
    assert detail["is_editable"] and detail["is_usable"]
    print(f"DEFINITION_SQL team_read members={count} queries={len(selected)}")
    assert len(selected) <= 5, len(selected)
    for index in range(count - 1):
        await teams.save(alice, team_data(members, name=f"delivery{index}"))
    with queries(storage) as selected:
        listing = await teams.list(alice)
    assert len(listing) == count and all(row["is_editable"] for row in listing)
    assert {len(row["payload"]["members"]) for row in listing} == {count}
    print(
        f"DEFINITION_SQL team_list teams={count} members={count} queries={len(selected)}"
    )
    assert len(selected) <= 5, len(selected)


async def test_agent_detail_use_and_history_reuse_loaded_source(storage):
    _, admin, alice, bob = await identities(storage)
    agents, shares = AgentManager(storage), ShareService(storage)
    value = expert()
    row = await agents.add_definition(alice, value)
    with queries(storage) as selected:
        detail = await agents.get_definition(alice, row["id"])
    assert detail["is_editable"] and detail["is_usable"] and detail["can_share"]
    print(f"DEFINITION_SQL agent_private queries={len(selected)}")
    assert len(selected) <= 4, len(selected)
    with queries(storage) as selected:
        usable = await agents.get_definition_for_use(alice, row["id"], "private")
    assert usable["resource_version"] == "1"
    assert len(selected) <= 3, len(selected)
    review = await shares.submit(alice, "agent", row["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "")
    value.payload["system_prompt"] = "new unshared"
    await agents.update_definition(alice, value.id, agent_changes(value))
    with queries(storage) as selected:
        shared = await agents.get_definition(bob, row["id"], "shared")
    assert (
        shared["resource_version"] == "1"
        and shared["payload"]["system_prompt"] == "旧版"
    )
    assert (
        not shared["is_editable"] and shared["is_usable"] and shared["access_grant_id"]
    )
    print(f"DEFINITION_SQL agent_shared queries={len(selected)}")
    assert len(selected) <= 4, len(selected)
    with queries(storage) as selected:
        historical = await agents.get_definition(
            alice, row["id"], "private", version="1"
        )
    assert (
        not historical["is_editable"]
        and historical["payload"]["system_prompt"] == "旧版"
    )
    assert not historical["can_share"] and not historical["can_share_update"]
    assert len(selected) <= 4, len(selected)


async def test_definition_details_refresh_editing_role_and_disabled_identity(storage):
    _, admin, alice, _ = await identities(storage)
    agents, teams = AgentManager(storage), TeamDefinition(storage)
    member = await agents.add_definition(alice, expert())
    root = await teams.save(alice, team_data([member]))
    async with storage.transaction() as conn:
        await conn.execute(
            update(t.users)
            .where(t.users.c.user_id == admin.user_id)
            .values(role_type="user")
        )
    with pytest.raises(Forbidden):
        await agents.get_definition(admin, member["id"], "private")
    with pytest.raises(Forbidden):
        await teams.read(admin, root["id"], "private")
    async with storage.transaction() as conn:
        await conn.execute(
            update(t.users)
            .where(t.users.c.user_id == alice.user_id)
            .values(is_enabled=False)
        )
    with pytest.raises(Forbidden):
        await agents.get_definition(alice, member["id"], "private")
    assert await teams.list(alice) == []


async def test_private_self_use_policy_does_not_remove_editing_or_history(storage):
    from types import SimpleNamespace

    _, admin, alice, _ = await identities(storage)
    agents, teams = AgentManager(storage), TeamDefinition(storage)
    source = expert()
    member = await agents.add_definition(alice, source)
    root = await teams.save(alice, team_data([member]))
    policies = SimpleNamespace(allow_private_self_use=False)
    blocked_agents, blocked_teams = (
        AgentManager(storage, policies),
        TeamDefinition(storage, policies),
    )
    detail = await blocked_agents.get_definition(alice, member["id"])
    assert detail["is_editable"] and not detail["is_usable"]
    with pytest.raises(Forbidden):
        await blocked_agents.get_definition_for_use(alice, member["id"], "private")
    team = await blocked_teams.read(alice, root["id"])
    assert team["is_editable"] and not team["is_usable"]
    assert team["orchestrator_config"]["agent_name"] == "leader-delivery"
    admin_detail = await blocked_agents.get_definition(admin, member["id"])
    assert admin_detail["is_editable"] and not admin_detail["is_usable"]
    assert await blocked_agents.list_versions(alice, member["id"]) == [
        {"resource_version": "1", "resource_view_type": "private"}
    ]
    assert await blocked_teams.versions(alice, root["id"]) == [
        {"resource_version": "1", "resource_view_type": "private"}
    ]
