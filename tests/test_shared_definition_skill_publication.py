"""An approved expert/team must run without the author's private NAS."""

import shutil
from pathlib import Path
from types import SimpleNamespace

import pytest
from test_domain import expert, identities
from test_resources import archive

from egis_agentkit.errors import AgentKitError, Conflict
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.storage.model import ShareTarget
from egis_agentkit.storage.model.agent._update import AgentUpdate


async def setup_definition(storage, root, kind):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, root)
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="report")
    data = expert()
    data.payload["bindings"] = [dict(
        resource_type="skill", resource_id="report", resource_version="1.0.0",
        resource_view_type="private",
    )]
    member = await AgentManager(storage).add_definition(alice, data)
    definition = member
    if kind == "team":
        definition = await TeamDefinition(storage).save(alice, dict(
            team_name="delivery", team_display_name="Delivery",
            orchestrator_config=dict(
                agent_name="coordinator", agent_display_name="Coordinator", payload={},
            ),
            members=[dict(definition_id=member["id"], resource_view_type="private")],
        ))
    shares = ShareService(storage, publish_skill=resources.publish_shared_skill)
    review = await shares.submit(alice, kind, definition["id"], target=ShareTarget(
        principal_type="user", principal_id="bob",
    ))
    return admin, alice, bob, resources, shares, review, definition, member


@pytest.mark.parametrize("kind", ["agent", "team"])
async def test_approved_definition_publishes_private_skill_dependency(storage, tmp_path, kind):
    admin, _, bob, resources, shares, review, definition, member = await setup_definition(
        storage, tmp_path, kind,
    )
    await shares.decide(admin, review["review_id"], True, "approved")
    common = tmp_path / "common/skills/report/1.0.0"
    assert (common / "SKILL.md").is_file()
    shutil.rmtree(tmp_path / "user/alice/skills")
    decision = await PermissionService(storage).require(bob, kind, definition["id"], "shared")
    root = await storage.definitions.get_definition_view(
        definition["id"], "shared", version=decision.resource_version,
        **decision.source_options,
    )
    scope = SimpleNamespace(
        user_id="bob", root_definition=root, definition=member,
        member_definitions={member["id"]: member},
        binding={"agent_definition_id": member["id"]},
    )
    skill = await resources.materialize_skill(bob, "report", "1.0.0", authorization_scope=scope)
    assert Path(skill.dir) == common
    asset = await storage.assets.get("skill", "report", "1.0.0")
    assert asset["owner_id"] == "alice" and asset["shared_version"] is None


@pytest.mark.parametrize("kind", ["agent", "team"])
async def test_dependency_copy_failure_cannot_approve_or_grant(storage, tmp_path, kind):
    admin, _, bob, _, shares, review, definition, _ = await setup_definition(storage, tmp_path, kind)

    async def unavailable(asset):
        raise AgentKitError("NAS unavailable", 503)

    shares._publish_skill = unavailable
    with pytest.raises(AgentKitError):
        await shares.decide(admin, review["review_id"], True, "approved")
    stored = (await storage.access.list_reviews(owner_id="alice"))[0]
    assert stored["review_status_type"] == "pending"
    assert await storage.access.shared_version(kind, definition["id"]) is None
    assert not (await PermissionService(storage).explain(bob, kind, definition["id"], "shared")).is_allowed


async def test_identical_skill_upload_provenance_does_not_create_shared_update(storage, tmp_path):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.upload_skill(
        alice, archive(), "1.0.0", resource_id="report", import_job_id="first-job",
    )
    shares = ShareService(storage, publish_skill=resources.publish_shared_skill)
    review = await shares.submit(alice, "skill", "report", target=ShareTarget(
        principal_type="user", principal_id=bob.user_id,
    ))
    await shares.decide(admin, review["review_id"], True, "approved")
    await resources.upload_skill(
        alice, archive(), "1.1.0", resource_id="report",
        import_job_id="second-job",
    )
    card = (await resources.catalog_page(alice, "skill"))["items"][0]
    assert card["resource_version"] == "1.1.0"
    assert card["can_share_update"] is False
    with pytest.raises(Conflict, match="内容没有变化"):
        await shares.submit(alice, "skill", "report")


async def test_definition_update_publishes_new_dependency_before_switching_shared_version(storage, tmp_path):
    admin, alice, bob, resources, shares, review, definition, _ = await setup_definition(
        storage, tmp_path, "agent",
    )
    await shares.decide(admin, review["review_id"], True, "approved")
    await resources.save_skill(alice, dict(
        resource_id="report", resource_version="2.0.0", base_version="1.0.0",
        markdown="# New report",
    ))
    definition["payload"]["bindings"][0]["resource_version"] = "2.0.0"
    updated = await AgentManager(storage).update_definition(
        alice, definition["id"], AgentUpdate(**{
            field: definition[field] for field in AgentUpdate.model_fields
        }),
    )
    pending = await shares.submit(alice, "agent", definition["id"])

    async def unavailable(asset):
        raise AgentKitError("NAS unavailable", 503)

    shares._publish_skill = unavailable
    with pytest.raises(AgentKitError):
        await shares.decide(admin, pending["review_id"], True, "approved")
    permissions = PermissionService(storage)
    assert (await permissions.require(bob, "agent", definition["id"], "shared")).resource_version == definition["resource_version"]
    assert not (tmp_path / "common/skills/report/2.0.0").exists()
    shares._publish_skill = resources.publish_shared_skill
    await shares.decide(admin, pending["review_id"], True, "retried")
    assert (await permissions.require(bob, "agent", definition["id"], "shared")).resource_version == updated["resource_version"]
    assert (tmp_path / "common/skills/report/2.0.0/SKILL.md").is_file()
    assert (tmp_path / "common/skills/report/1.0.0/SKILL.md").is_file()
