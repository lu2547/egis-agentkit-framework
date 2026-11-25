import pytest
from test_domain import COMPANY, agent_changes, expert, identities

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.storage.model import Asset, Grant, Group, ShareTarget


@pytest.mark.asyncio
async def test_share_uses_submitted_recipient_and_resolves_its_review(storage):
    users, admin, alice, bob = await identities(storage)
    await users.save_group(admin, Group(group_id="ops", group_name="运营"))
    agents, shares = AgentManager(storage), ShareService(storage)
    first = await agents.add_definition(alice, expert())
    review = await shares.submit(
        alice,
        "agent",
        first["id"],
        target=ShareTarget(principal_type="user", principal_id=bob.user_id),
    )
    await shares.decide(admin, review["review_id"], True, "")
    changed = expert()
    changed.id = first["id"]
    changed.payload["system_prompt"] = "组内版本"
    await agents.update_definition(alice, (changed).id, agent_changes(changed))
    team_review = await shares.submit(
        alice,
        "agent",
        first["id"],
        target=ShareTarget(principal_type="group", principal_id="ops"),
    )
    await shares.decide(admin, team_review["review_id"], True, "")
    assert (await AgentManager(storage).get_definition_for_use(bob, first["id"], "shared"))[
        "payload"
    ]["system_prompt"] == "旧版"
    with pytest.raises(Forbidden):
        await AgentManager(storage).get_definition_for_use(admin, first["id"], "shared")
    await users.update_user(admin, admin.model_copy(update={"group_ids": ["ops"]}))
    assert (await AgentManager(storage).get_definition_for_use(admin, first["id"], "shared"))[
        "payload"
    ]["system_prompt"] == "旧版"
    update = await shares.submit(alice, "agent", first["id"])
    await shares.decide(admin, update["review_id"], True, "")
    for recipient in (admin, bob):
        assert (
            await AgentManager(storage).get_definition_for_use(recipient, first["id"], "shared")
        )["payload"]["system_prompt"] == "组内版本"

    company = await shares.submit(
        alice,
        "agent",
        first["id"],
        target=COMPANY,
    )
    assert company["principal_id"] == COMPANY.principal_id
    approved = await shares.decide(admin, company["review_id"], True, "")
    again = await shares.decide(admin, company["review_id"], True, "retry")
    assert approved["resource_version"] == again["resource_version"]
    with pytest.raises(Conflict):
        await shares.submit(alice, "agent", first["id"], target=COMPANY)


@pytest.mark.asyncio
async def test_three_expert_paths_and_marketplace_grant_are_revoked_independently(
    storage,
):
    _, admin, _, bob = await identities(storage)
    agents, permissions = AgentManager(storage), PermissionService(storage)
    asset = Asset(
        resource_type="skill",
        resource_id="report",
        resource_version="1",
        publication_revision=1,
        owner_type="system",
        owner_id="platform",
    )
    async with storage.transaction() as conn:
        await storage.assets.insert_asset(asset, actor=admin.user_id, conn=conn)

    async def grant(kind, identifier):
        return await GrantService(storage).grant(
            admin,
            Grant(
                principal_type="user",
                principal_id=bob.user_id,
                resource_type=kind,
                resource_id=identifier,
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="platform",
            ),
        )

    roots = []
    for name in ("first", "second", "third"):
        value = expert("platform", "system", name)
        value.payload["bindings"] = [
            {
                "resource_type": "skill",
                "resource_id": asset.resource_id,
                "resource_version": "1",
                "resource_view_type": "system",
            }
        ]
        row = await agents.add_definition(admin, value)
        roots.append(await grant("agent", row["id"]))

    async def can_use(root=None):
        return (
            await permissions.explain(
                bob,
                "skill",
                asset.resource_id,
                "system",
                version="1",
                agent_id=root["resource_id"] if root else None,
                root_grant_id=root["id"] if root else None,
            )
        ).is_allowed

    assert not await can_use()
    for root in roots:
        assert await can_use(root)
    direct = await grant("skill", asset.resource_id)
    await agents.delete_definition(admin, roots[0]["resource_id"])
    await GrantService(storage).revoke(admin, roots[1]["id"])
    assert not await can_use(roots[0])
    assert not await can_use(roots[1])
    assert await can_use(roots[2])
    assert await can_use()
    await GrantService(storage).revoke(admin, direct["id"])
    assert not await can_use()
    assert await can_use(roots[2])
    await agents.delete_definition(admin, roots[2]["resource_id"])
    assert not await can_use(roots[2])


@pytest.mark.asyncio
async def test_admin_direct_grant_cannot_publish_private_resource_without_review(storage):
    _, admin, alice, bob = await identities(storage)
    agents, shares = AgentManager(storage), ShareService(storage)
    row = await agents.add_definition(alice, expert())
    with pytest.raises(Forbidden):
        await GrantService(storage).grant(admin, Grant(
            principal_type="user",
            principal_id=bob.user_id,
            resource_type="agent",
            resource_id=row["id"],
            resource_view_type="shared",
            resource_owner_type="user",
            resource_owner_id=alice.user_id,
        ))
    assert await shares.list(admin, admin=True) == []
    with pytest.raises(Forbidden):
        await AgentManager(storage).get_definition_for_use(bob, row["id"], "shared")
