"""用同一资源的多个版本和授权路径验证审核、使用与撤销。"""

import pytest
from test_domain import agent_changes, expert, identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import Grant, Group, ShareTarget


@pytest.mark.asyncio
async def test_personal_grant_requires_review_and_uses_existing_shared_publication(storage):
    users, admin, alice, bob = await identities(storage)
    carol = await users.sync_identity("carol", "Carol")
    definitions = AgentManager(storage)
    query = AgentManager(storage)
    shares = ShareService(storage)

    original = expert()
    await definitions.add_definition(alice, original)
    review = await shares.submit(
        alice,
        "agent",
        original.id,
        target=ShareTarget(principal_type="user", principal_id=bob.user_id),
    )
    updated = original.model_copy(update={"payload": {"system_prompt": "新版"}})
    await definitions.update_definition(alice, (updated).id, agent_changes(updated))
    approved = await shares.decide(admin, review["review_id"], True, "通过")
    assert approved["resource_version"] == "1"
    assert "snapshot" not in approved
    assert (await query.get_definition(bob, original.id, "shared"))["payload"] == original.payload

    with pytest.raises(Forbidden):
        await GrantService(storage).grant(
            admin,
            Grant(
                principal_type="user",
                principal_id=carol.user_id,
                resource_type="agent",
                resource_id=original.id,
                resource_view_type="shared",
                resource_owner_type="user",
                resource_owner_id=alice.user_id,
            ),
        )
    recipient = await shares.submit(
        alice, "agent", original.id,
        target=ShareTarget(principal_type="user", principal_id=carol.user_id),
    )
    approved_recipient = await shares.decide(admin, recipient["review_id"], True, "通过")
    assert approved_recipient["resource_version"] == "1"
    grant = next(
        row for row in await GrantService(storage).list_grants(admin)
        if row["source_review_id"] == recipient["review_id"]
    )
    assert "resource_version" not in grant
    with pytest.raises(Forbidden):
        await query.get_definition(carol, original.id, "shared", version="2")
    assert (await query.get_definition(carol, original.id, "shared"))["payload"] == original.payload
    await GrantService(storage).revoke(admin, grant["id"])
    with pytest.raises(Forbidden):
        await query.get_definition(carol, original.id, "shared")
    assert (await query.get_definition(bob, original.id, "shared"))["resource_version"] == "1"


@pytest.mark.asyncio
async def test_expert_share_adds_independent_dependency_paths_for_group_and_user(
    storage, tmp_path,
):
    users, admin, alice, bob = await identities(storage)
    await users.save_group(admin, Group(group_id="team1", group_name="团队一"))
    await users.update_user(
        admin, bob.model_copy(update={"group_ids": ["team1", "company"]})
    )
    carol = await users.sync_identity("carol", "C")
    await users.update_user(
        admin, carol.model_copy(update={"group_ids": ["team1", "company"]})
    )
    dave = await users.sync_identity("dave", "D")
    ed = await users.sync_identity("ed", "E")
    carol, bob = await users.get("carol"), await users.get("bob")
    permissions = PermissionService(storage)
    resources = ResourceService(storage, tmp_path)
    shares = ShareService(storage, publish_skill=resources.publish_shared_skill)

    async def publish_skill(version):
        await resources.save_skill(
            alice,
            {
                "resource_id": "skill1",
                "resource_version": version,
                "markdown": f"# Skill workflow {version}",
                **({"base_version": "1.0.0"} if version != "1.0.0" else {}),
            },
        )

    async def share(owner, kind, identifier, group):
        review = await shares.submit(
            owner,
            kind,
            identifier,
            target=ShareTarget(principal_type="group", principal_id=group),
        )
        return await shares.decide(admin, review["review_id"], True, "通过")

    await publish_skill("1.0.0")
    skill_review = await share(alice, "skill", "skill1", "team1")
    await publish_skill("2.0.0")
    definition = expert(owner=bob.user_id, name="expert2")
    definition.payload["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": "skill1",
            "resource_version": "1.0.0",
            "resource_view_type": "shared",
        }
    ]
    await AgentManager(storage).add_definition(bob, definition)
    company_review = await share(bob, "agent", definition.id, "company")
    direct_review = await shares.submit(
        bob, "agent", definition.id,
        target=ShareTarget(principal_type="user", principal_id=dave.user_id),
    )
    await shares.decide(admin, direct_review["review_id"], True, "通过")
    grants = await GrantService(storage).list_grants(admin)
    assert len(grants) == 7
    direct = next(
        row for row in grants
        if row["source_review_id"] == direct_review["review_id"]
        and row["root_grant_id"] is None
    )
    company_grant = next(
        row
        for row in grants
        if row["source_review_id"] == company_review["review_id"]
        and row["root_grant_id"] is None
    )
    inherited = [row for row in grants if row["grant_source_type"] == "dependency"]
    assert {
        (
            row["resource_type"],
            row["resource_id"],
            row["parent_grant_id"],
            row["principal_id"],
        )
        for row in inherited
    } == {
        ("skill", "skill1", company_grant["id"], "company"),
        ("skill", "skill1", direct["id"], dave.user_id),
    }
    assert not (await permissions.explain(dave, "skill", "skill1", "shared")).is_allowed
    assert (
        await permissions.explain(carol, "skill", "skill1", "shared")
    ).resource_version == "1.0.0"
    root = await storage.definitions.get_definition_view(
        definition.id, "shared", version="1", grant_id=direct["id"]
    )
    await permissions.require_execution(dave, root)
    pending = await shares.submit(
        bob,
        "agent",
        definition.id,
        target=ShareTarget(principal_type="user", principal_id=ed.user_id),
    )
    skill_grant = next(
        row for row in grants if row["source_review_id"] == skill_review["review_id"]
    )
    await GrantService(storage).revoke(admin, skill_grant["id"])
    with pytest.raises(Forbidden):
        await shares.decide(admin, pending["review_id"], True, "旧授权已撤销")
    await permissions.require_execution(dave, root)
    await GrantService(storage).revoke(admin, direct["id"])
    with pytest.raises(Forbidden):
        await permissions.require_execution(dave, root)
    assert (
        await permissions.explain(carol, "agent", definition.id, "shared")
    ).is_allowed
    assert not (
        await permissions.explain(carol, "skill", "skill1", "shared")
    ).is_allowed
