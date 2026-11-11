import pytest
from sqlalchemy.dialects.postgresql import insert
from test_domain import COMPANY, expert, identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._expert_definition import ExpertDefinition
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset, Grant, Group


@pytest.mark.asyncio
async def test_group_revocation_does_not_use_stale_turn_user(storage):
    users, admin, alice, _ = await identities(storage)
    row = await ExpertDefinition(storage).build(admin, expert("sys", "system"))
    await users.save_group(admin, Group(group_id="g", group_name="研发"))
    alice.group_ids = ["g"]
    await users.update_user(admin, alice)
    permissions = PermissionService(storage)
    await GrantService(storage).grant(
        admin,
        Grant(
            principal_type="group",
            principal_id="g",
            resource_type="agent",
            resource_id=row["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="sys",
        ),
    )
    assert (await permissions.explain(alice, "agent", row["id"], "system")).is_allowed
    await users.update_user(admin, alice.model_copy(update={"group_ids": []}))
    assert not (
        await permissions.explain(alice, "agent", row["id"], "system")
    ).is_allowed


@pytest.mark.asyncio
async def test_unreviewed_binding_does_not_expand_shared_inherited_use(storage):
    _, admin, alice, bob = await identities(storage)
    permissions, agents = PermissionService(storage), ExpertDefinition(storage)
    async with storage.transaction() as conn:
        for name in ("a", "b"):
            asset = Asset(
                resource_type="skill",
                resource_id=name,
                resource_version="1.0.0",
                owner_type="system",
                owner_id="sys",
            )
            await conn.execute(
                insert(t.assets).values(
                    **asset.model_dump(), created_by="admin", updated_by="admin"
                )
            )
    for name in ("a", "b"):
        await GrantService(storage).grant(
            admin,
            Grant(
                principal_type="user",
                principal_id="alice",
                resource_type="skill",
                resource_id=name,
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="sys",
            ),
        )
    value = expert()
    value.payload["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": "a",
            "resource_version": "1.0.0",
            "resource_view_type": "system",
        }
    ]
    row = await agents.build(alice, value)
    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", row["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "通过")
    value.id = row["id"]
    value.payload["bindings"][0]["resource_id"] = "b"
    await agents.save(alice, value)
    assert not (await permissions.explain(bob, "skill", "a", "system")).is_allowed
    assert not (
        await permissions.explain(
            bob,
            "skill",
            "a",
            "system",
            agent_id=row["id"],
        )
    ).is_allowed
    assert not (await permissions.explain(bob, "skill", "b", "system")).is_allowed


@pytest.mark.asyncio
async def test_private_binding_does_not_keep_revoked_external_permission(storage):
    _, admin, alice, _ = await identities(storage)
    asset = Asset(
        resource_type="skill",
        resource_id="external",
        resource_version="1.0.0",
        owner_type="system",
        owner_id="sys",
    )
    async with storage.transaction() as conn:
        await conn.execute(
            insert(t.assets).values(
                **asset.model_dump(), created_by="admin", updated_by="admin"
            )
        )
    permissions = PermissionService(storage)
    grant = await GrantService(storage).grant(
        admin,
        Grant(
            principal_type="user",
            principal_id="alice",
            resource_type="skill",
            resource_id="external",
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="sys",
        ),
    )
    value = expert()
    value.payload["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": "external",
            "resource_version": "1.0.0",
            "resource_view_type": "system",
        }
    ]
    row = await ExpertDefinition(storage).build(alice, value)
    snapshot = await storage.definitions.read_view(row["id"], "private")
    await permissions.require_execution(alice, snapshot)
    await GrantService(storage).revoke(admin, grant["id"])
    with pytest.raises(Forbidden):
        await permissions.require_execution(alice, snapshot)


@pytest.mark.asyncio
async def test_team_capture_retains_both_versions_from_real_approved_experts(storage):
    from egis_agentkit.service.team._definition import TeamDefinition

    _, admin, alice, bob = await identities(storage)
    permissions = PermissionService(storage)
    agents, shares = ExpertDefinition(storage), ShareService(storage)
    members = []
    for name, version in [("first", "1.0.0"), ("second", "2.0.0")]:
        asset = Asset(
            resource_type="skill",
            resource_id="report",
            resource_version=version,
            owner_type="system",
            owner_id="sys",
        )
        async with storage.transaction() as conn:
            await conn.execute(
                insert(t.assets).values(
                    **asset.model_dump(), created_by="admin", updated_by="admin"
                )
            )
        await GrantService(storage).grant(
            admin,
            Grant(
                principal_type="user",
                principal_id="alice",
                resource_type="skill",
                resource_id="report",
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="sys",
            ),
        )
        value = expert(name=name)
        value.payload["bindings"] = [
            {
                "resource_type": "skill",
                "resource_id": "report",
                "resource_version": version,
                "resource_view_type": "system",
            }
        ]
        saved = await agents.build(alice, value)
        review = await shares.submit(alice, "agent", saved["id"], target=COMPANY)
        await shares.decide(admin, review["review_id"], True, "approved")
        members.append({"definition_id": saved["id"], "resource_view_type": "shared"})
    team = await TeamDefinition(storage).save(
        alice,
        {
            "team_name": "testteam",
            "team_display_name": "Test",
            "orchestrator_config": {
                "agent_name": "leader",
                "agent_display_name": "Leader",
            },
            "members": members,
        },
    )
    review = await shares.submit(alice, "team", team["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "approved")
    decision = await permissions.require(bob, "team", team["id"], "shared")
    snapshot = await storage.definitions.read_view(
        team["id"],
        "shared",
        version=decision.resource_version,
        **decision.source_options,
    )
    from egis_agentkit.service.team._definition import team_member_definitions

    definitions = await team_member_definitions(storage, snapshot)
    for member, version in zip(members, ("1.0.0", "2.0.0")):
        assert definitions[member["definition_id"]]["payload"]["bindings"][0]["resource_version"] == version
    await permissions.require_execution(bob, snapshot)
