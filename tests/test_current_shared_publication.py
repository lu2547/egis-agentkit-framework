"""Shared publication moves once for every stable recipient grant."""

import pytest
from test_domain import COMPANY, agent_changes, expert, identities

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.storage.model import Grant


@pytest.mark.asyncio
async def test_update_review_pins_edition_and_reuses_all_existing_grants(storage):
    _, admin, alice, bob = await identities(storage)
    definitions, shares = AgentManager(storage), ShareService(storage)
    grants = GrantService(storage)
    permissions = PermissionService(storage)
    value = expert()
    first = await definitions.add_definition(alice, value)
    review = await shares.submit(alice, "agent", value.id, target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "")
    direct = await grants.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=bob.user_id,
            resource_type="agent",
            resource_id=value.id,
            resource_view_type="shared",
            resource_owner_type="user",
            resource_owner_id=alice.user_id,
        ),
    )
    before = await grants.list_grants(admin)
    value.payload["system_prompt"] = "second"
    second = await definitions.update_definition(alice, (value).id, agent_changes(value))
    update = await shares.submit(alice, "agent", value.id)
    assert update["review_type"] == "update"
    assert update["principal_id"] is None
    with pytest.raises(Conflict):
        await shares.submit(alice, "agent", value.id)
    value.owner_version = second["owner_version"]
    value.payload["system_prompt"] = "third"
    await definitions.update_definition(alice, (value).id, agent_changes(value))
    assert (
        await permissions.require(bob, "agent", value.id, "shared")
    ).resource_version == first["resource_version"]

    approved = await shares.decide(admin, update["review_id"], True, "")
    assert approved["resource_version"] == second["resource_version"]
    assert await grants.list_grants(admin) == before
    assert all("resource_version" not in row for row in before)
    assert (
        await permissions.require(bob, "agent", value.id, "shared")
    ).resource_version == second["resource_version"]
    with pytest.raises(Forbidden):
        await permissions.require(
            bob, "agent", value.id, "shared", version=first["resource_version"]
        )
    assert (
        await permissions.require(
            bob,
            "agent",
            value.id,
            "shared",
            version=first["resource_version"],
            grant_id=direct["id"],
            allow_pinned=True,
        )
    ).resource_version == first["resource_version"]


@pytest.mark.asyncio
async def test_update_replaces_dependency_paths_without_changing_root_grants(storage):
    from egis_agentkit.storage.model import Asset

    users, admin, alice, bob = await identities(storage)
    carol = await users.sync_identity("carol", "Carol")
    definitions, shares = AgentManager(storage), ShareService(storage)
    grants, permissions = GrantService(storage), PermissionService(storage)
    async with storage.definition_write() as write:
        for identifier in ("previous", "replacement"):
            await write.assets.insert_asset(
                Asset(
                    resource_type="skill",
                    resource_id=identifier,
                    resource_version="1.0.0",
                    publication_revision=1,
                    owner_type="user",
                    owner_id=alice.user_id,
                ),
                actor=alice.user_id,
            )

    def binding(identifier):
        return {
            "resource_type": "skill",
            "resource_id": identifier,
            "resource_version": "1.0.0",
            "resource_view_type": "private",
        }

    value = expert()
    value.payload["bindings"] = [binding("previous")]
    await definitions.add_definition(alice, value)
    review = await shares.submit(alice, "agent", value.id, target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "")
    await grants.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=carol.user_id,
            resource_type="agent",
            resource_id=value.id,
            resource_view_type="shared",
            resource_owner_type="user",
            resource_owner_id=alice.user_id,
        ),
    )
    root = await permissions.require(bob, "agent", value.id, "shared")
    pinned = await storage.definitions.get_definition_view(
        value.id, "shared", **root.source_options
    )
    await permissions.require_execution(bob, pinned)
    value.payload["bindings"] = [binding("replacement")]
    await definitions.update_definition(alice, (value).id, agent_changes(value))
    update = await shares.submit(alice, "agent", value.id)
    await shares.decide(admin, update["review_id"], True, "")

    rows = await grants.list_grants(admin)
    roots = {
        row["id"] for row in rows if row["grant_source_type"] in ("share", "manual")
    }
    assert root.grant_id in roots
    assert {
        row["root_grant_id"]
        for row in rows
        if row["resource_id"] == "replacement" and row["revoked_at"] is None
    } == roots
    assert all(
        row["revoked_at"] is not None
        for row in rows
        if row["resource_id"] == "previous"
    )
    for recipient in (bob, carol):
        current = await permissions.require(recipient, "agent", value.id, "shared")
        definition = await storage.definitions.get_definition_view(
            value.id, "shared", **current.source_options
        )
        await permissions.require_execution(recipient, definition)
        assert not (
            await permissions.explain(recipient, "skill", "replacement", "private")
        ).is_allowed
    with pytest.raises(Forbidden):
        await permissions.require_execution(bob, pinned, allow_pinned=True)


@pytest.mark.asyncio
async def test_revoking_last_recipient_cancels_pending_update(storage):
    _, admin, alice, bob = await identities(storage)
    definitions, shares = AgentManager(storage), ShareService(storage)
    grants = GrantService(storage)
    value = expert()
    await definitions.add_definition(alice, value)
    initial = await shares.submit(alice, "agent", value.id, target=COMPANY)
    await shares.decide(admin, initial["review_id"], True, "")
    initial_grant = next(
        row
        for row in await grants.list_grants(admin)
        if row["grant_source_type"] == "share"
    )
    second = await definitions.update_definition(alice, (value).id, agent_changes(value))
    stale = await shares.submit(alice, "agent", value.id)
    await grants.revoke(admin, initial_grant["id"])
    value.owner_version = second["owner_version"]
    third = await definitions.update_definition(alice, (value).id, agent_changes(value))
    await grants.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=bob.user_id,
            resource_type="agent",
            resource_id=value.id,
            resource_view_type="shared",
            resource_owner_type="user",
            resource_owner_id=alice.user_id,
        ),
    )
    assert (await shares.list(alice))[0]["review_status_type"] == "cancelled"
    with pytest.raises(Conflict):
        await shares.decide(admin, stale["review_id"], True, "")
    decision = await PermissionService(storage).require(
        bob, "agent", value.id, "shared"
    )
    assert decision.resource_version == third["resource_version"]
