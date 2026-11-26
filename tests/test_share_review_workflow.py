"""Review revocation and inspect immutable submissions without recipient access."""

import copy

import pytest
from sqlalchemy import text
from test_domain import COMPANY, expert, identities
from test_management_authorization import shared_agent
from test_resources import archive
from test_shared_definition_skill_publication import setup_definition

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import ShareTarget
from egis_agentkit.storage.model.agent._update import AgentUpdate


async def test_withdrawal_requires_independent_approval_and_is_idempotent(storage):
    _, admin, alice, bob, _, sharing, source, original = await shared_agent(storage)
    other = await sharing.submit(alice, "agent", source["id"], target=ShareTarget(
        principal_type="user", principal_id=admin.user_id,
    ))
    await sharing.decide(admin, other["review_id"], True, "")
    first = await sharing.withdraw(alice, original["review_id"])
    again = await sharing.withdraw(alice, original["review_id"])
    assert first["review_type"] == "revoke"
    assert first["original_review_id"] == original["review_id"]
    assert first["review_id"] == again["review_id"]
    permissions = PermissionService(storage)
    assert (await permissions.explain(bob, "agent", source["id"], "shared")).is_allowed
    rows = {row["review_id"]: row for row in await sharing.list(alice)}
    assert rows[original["review_id"]]["is_current"]
    await sharing.decide(admin, first["review_id"], True, "withdraw approved")
    await sharing.decide(admin, first["review_id"], True, "retry")
    assert not (await permissions.explain(bob, "agent", source["id"], "shared")).is_allowed
    assert (await permissions.explain(admin, "agent", source["id"], "shared")).is_allowed
    rows = {row["review_id"]: row for row in await sharing.list(alice)}
    assert rows[original["review_id"]]["review_status_type"] == "approved"
    assert not rows[original["review_id"]]["is_current"]


@pytest.mark.parametrize("outcome", ["rejected", "cancelled"])
async def test_unapproved_revocation_retains_access_and_can_be_resubmitted(storage, outcome):
    _, admin, alice, bob, _, sharing, source, original = await shared_agent(storage)
    revoke = await sharing.withdraw(alice, original["review_id"])
    if outcome == "rejected":
        await sharing.decide(admin, revoke["review_id"], False, "retain sharing")
    else:
        await sharing.withdraw(alice, revoke["review_id"])
    assert (await PermissionService(storage).explain(bob, "agent", source["id"], "shared")).is_allowed
    retry = await sharing.withdraw(alice, original["review_id"])
    assert retry["review_id"] != revoke["review_id"]


async def test_owner_admin_cannot_approve_own_revocation(storage):
    users, admin, _, bob = await identities(storage)
    await users.sync_identity("reviewer", "Reviewer")
    await users.provision_admin("reviewer", actor="bootstrap")
    second = await users.get("reviewer")
    source = await AgentManager(storage).add_definition(admin, expert("admin"))
    sharing = ShareService(storage)
    initial = await sharing.submit(admin, "agent", source["id"], target=COMPANY)
    await sharing.decide(second, initial["review_id"], True, "")
    revoke = await sharing.withdraw(admin, initial["review_id"])
    with pytest.raises(Forbidden, match="自己.*申请"):
        await sharing.decide(admin, revoke["review_id"], True, "")
    assert (await PermissionService(storage).explain(bob, "agent", source["id"], "shared")).is_allowed


async def test_cannot_delete_shared_expert_before_withdrawal_is_approved(storage):
    _, admin, alice, _, agents, sharing, source, original = await shared_agent(storage)
    with pytest.raises(Conflict, match="撤回.*审核"):
        await agents.delete_definition(alice, source["id"])
    revoke = await sharing.withdraw(alice, original["review_id"])
    with pytest.raises(Conflict, match="撤回.*审核"):
        await agents.delete_definition(alice, source["id"])
    await sharing.decide(admin, revoke["review_id"], True, "")
    await agents.delete_definition(alice, source["id"])


async def test_review_content_is_frozen_and_scoped_with_exact_dependencies(storage, tmp_path):
    admin, alice, bob, _, sharing, review, definition, _ = await setup_definition(storage, tmp_path, "agent")
    payload = copy.deepcopy(definition["payload"])
    payload["system_prompt"] = "Changed after submission"
    await AgentManager(storage).update_definition(alice, definition["id"], AgentUpdate(
        agent_name=definition["agent_name"], agent_display_name="Changed name",
        owner_version=definition["owner_version"], payload=payload,
    ))
    with pytest.raises(Forbidden):
        await sharing.detail(bob, review["review_id"])
    detail = await sharing.detail(admin, review["review_id"])
    snapshot = detail["content"]
    assert snapshot["origin"] == "submitted"
    assert snapshot["root"]["resource_version"] == review["resource_version"]
    assert snapshot["root"]["payload"]["system_prompt"] != "Changed after submission"
    skill = next(node for node in snapshot["dependencies"] if node["resource_type"] == "skill")
    assert skill["resource_version"] == "1.0.0"
    assert skill["checksum_sha256"]
    assert skill["payload"]["files"]
    assert "markdown" in skill["payload"]
    assert not (await PermissionService(storage).explain(admin, "agent", definition["id"], "private")).is_allowed


async def test_shared_skill_delete_blocked_until_withdrawal_approval(storage, tmp_path):
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="audit")
    sharing = ShareService(storage, publish_skill=resources.publish_shared_skill)
    original = await sharing.submit(alice, "skill", "audit", target=COMPANY)
    await sharing.decide(admin, original["review_id"], True, "")
    with pytest.raises(Conflict, match="撤回.*审核"):
        await resources.delete_skill(alice, "audit")
    revoke = await sharing.withdraw(alice, original["review_id"])
    await sharing.decide(admin, revoke["review_id"], True, "")
    await resources.delete_skill(alice, "audit")


async def test_legacy_review_marks_content_as_reconstructed(storage):
    _, admin, alice, _, _, sharing, _, original = await shared_agent(storage)
    async with storage.engine.begin() as conn:
        await conn.execute(text("UPDATE ak_approve SET content_snapshot=NULL WHERE review_id=:id"), {"id": original["review_id"]})
    detail = await sharing.detail(admin, original["review_id"])
    assert detail["content"]["origin"] == "reconstructed"


async def test_shared_dependency_cannot_be_deleted_to_bypass_withdrawal(storage, tmp_path):
    admin, alice, _, resources, sharing, review, _, member = await setup_definition(storage, tmp_path, "team")
    await sharing.decide(admin, review["review_id"], True, "")
    with pytest.raises(Conflict, match="撤回.*审核"):
        await resources.delete_skill(alice, "report")
    with pytest.raises(Conflict, match="撤回.*审核"):
        await AgentManager(storage).delete_definition(alice, member["id"])


async def test_unshared_private_dependency_does_not_require_administrator_withdrawal(storage, tmp_path):
    _, alice, _, resources, sharing, review, _, _ = await setup_definition(storage, tmp_path, "agent")
    await sharing.withdraw(alice, review["review_id"])
    await resources.delete_skill(alice, "report")


async def test_review_schema_upgrade_preserves_approvals_and_is_explicit(storage):
    from egis_agentkit.storage.migration._share_reviews import migrate_share_reviews

    _, _, alice, _, _, sharing, _, original = await shared_agent(storage)
    async with storage.engine.begin() as conn:
        schema = await conn.scalar(text("SELECT current_schema()"))
        await conn.execute(text("ALTER TABLE ak_approve DROP COLUMN original_review_id CASCADE, DROP COLUMN content_snapshot"))
        assert await migrate_share_reviews(conn, schema) == "pending"
        columns = await conn.scalar(text("SELECT count(*) FROM information_schema.columns WHERE table_schema=:schema AND table_name='ak_approve' AND column_name='content_snapshot'"), {"schema": schema})
        assert columns == 0
        assert await migrate_share_reviews(conn, schema, apply=True) == "upgraded"
        assert await migrate_share_reviews(conn, schema, apply=True) == "current"
    original_row = next(row for row in await sharing.list(alice) if row["review_id"] == original["review_id"])
    assert original_row["is_current"]
    assert original_row["review_status_type"] == "approved"


async def test_review_http_endpoint_restricts_content_to_admin(storage):
    from types import SimpleNamespace

    import httpx
    from egis_agentkit_app.controller.access import router
    from egis_agentkit_app.controller.base import (
        current_user,
        register_exception_handlers,
    )
    from fastapi import FastAPI, Request

    _, admin, alice, _, _, sharing, _, original = await shared_agent(storage)
    app = FastAPI()
    app.include_router(router, prefix="/manager")
    register_exception_handlers(app)
    app.state.egis_services = SimpleNamespace(sharing=sharing)
    async def identity(request: Request):
        return admin if request.headers["x-user"] == "admin" else alice
    app.dependency_overrides[current_user] = identity
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        url = f"/manager/admin/share-reviews/{original['review_id']}"
        denied = await client.get(url, headers={"x-user": "alice"})
        assert denied.status_code == 403
        result = await client.get(url, headers={"x-user": "admin"})
        assert result.status_code == 200
        assert result.json()["content"]["root"]["payload"]["system_prompt"]


async def test_reviewer_can_download_only_submitted_skill_dependencies(storage, tmp_path):
    import io
    import zipfile

    from egis_agentkit.errors import NotFound

    admin, alice, bob, resources, sharing, review, _, _ = await setup_definition(storage, tmp_path, "agent")
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="unrelated")
    with pytest.raises(Forbidden):
        await sharing.skill_archive(bob, review["review_id"], "report", "1.0.0")
    with pytest.raises(NotFound):
        await sharing.skill_archive(admin, review["review_id"], "unrelated", "1.0.0")
    result = await sharing.skill_archive(admin, review["review_id"], "report", "1.0.0")
    with zipfile.ZipFile(io.BytesIO(result)) as package:
        assert any(name.endswith("SKILL.md") for name in package.namelist())


async def test_submitted_connector_configuration_stays_frozen_and_legacy_is_current(storage, tmp_path):
    from test_managed_connectors import CONFIG

    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_mcp(admin, CONFIG)
    value = expert()
    value.payload["bindings"] = [{"resource_type": "mcp", "resource_id": "reports", "resource_view_type": "system"}]
    definition = await AgentManager(storage).add_definition(alice, value)
    sharing = ShareService(storage)
    review = await sharing.submit(alice, "agent", definition["id"], target=COMPANY)
    await resources.save_mcp(admin, {**CONFIG, "config": {**CONFIG["config"], "url": "https://changed.example/mcp"}})
    frozen = await sharing.detail(admin, review["review_id"])
    connector = next(node for node in frozen["content"]["dependencies"] if node["resource_type"] == "mcp")
    assert connector["payload"]["config"]["url"] == CONFIG["config"]["url"]
    assert connector["configuration_revision"] == 1
    assert set(connector["payload"]["config"]) <= {"url", "transport", "auth_type", "key_header", "oauth_client_id", "oauth_scope"}
    async with storage.engine.begin() as conn:
        await conn.execute(text("UPDATE ak_approve SET content_snapshot=NULL WHERE review_id=:id"), {"id": review["review_id"]})
    rebuilt = await sharing.detail(admin, review["review_id"])
    connector = next(node for node in rebuilt["content"]["dependencies"] if node["resource_type"] == "mcp")
    assert connector["origin"] == "reconstructed"
    assert connector["payload"]["config"]["url"] == "https://changed.example/mcp"


@pytest.mark.parametrize("admin_route", [False, True])
async def test_owner_admin_cannot_disable_shared_skill_but_independent_admin_can(storage, tmp_path, admin_route):
    users, admin, _, _ = await identities(storage)
    await users.sync_identity("reviewer", "Reviewer")
    await users.provision_admin("reviewer", actor="bootstrap")
    reviewer = await users.get("reviewer")
    resources = ResourceService(storage, tmp_path)
    await resources.upload_skill(admin, archive(), "1.0.0", resource_id="owned-admin")
    sharing = ShareService(storage, publish_skill=resources.publish_shared_skill)
    original = await sharing.submit(admin, "skill", "owned-admin", target=COMPANY)
    await sharing.decide(reviewer, original["review_id"], True, "")
    with pytest.raises(Conflict, match="撤回.*审核"):
        await resources.set_asset_enabled(admin, "skill", "owned-admin", "1.0.0", False, admin=admin_route)
    await resources.set_asset_enabled(reviewer, "skill", "owned-admin", "1.0.0", False, admin=True)


async def test_owner_can_disable_unshared_edition_while_dependency_edition_is_protected(storage, tmp_path):
    admin, alice, _, resources, sharing, review, _, _ = await setup_definition(storage, tmp_path, "team")
    await sharing.decide(admin, review["review_id"], True, "")
    await resources.upload_skill(alice, archive(), "1.1.0", resource_id="report")
    await resources.set_asset_enabled(alice, "skill", "report", "1.1.0", False)
    with pytest.raises(Conflict, match="撤回.*审核"):
        await resources.set_asset_enabled(alice, "skill", "report", "1.0.0", False)


async def test_release_sql_adds_review_columns_without_changing_live_grants(storage):
    from pathlib import Path

    _, admin, alice, bob, _, sharing, source, original = await shared_agent(storage)
    before = await storage.access.direct_grants("agent", source["id"])
    async with storage.engine.begin() as conn:
        await conn.execute(text("ALTER TABLE ak_approve DROP COLUMN original_review_id CASCADE, DROP COLUMN content_snapshot"))
    script = (Path(__file__).resolve().parents[1] / "scripts/releases/1.0.3/04_share_review_workflow.sql").read_text()
    async with storage.engine.connect() as conn:
        raw = await conn.get_raw_connection()
        pg = raw.driver_connection
        await pg.execute(script)
        await pg.execute(script)
    assert await storage.access.direct_grants("agent", source["id"]) == before
    assert (await PermissionService(storage).explain(bob, "agent", source["id"], "shared")).is_allowed
    revoke = await sharing.withdraw(alice, original["review_id"])
    assert revoke["review_type"] == "revoke"
    await sharing.decide(admin, revoke["review_id"], True, "")


@pytest.mark.parametrize("legacy", [False, True])
async def test_owner_can_request_withdrawal_after_dependency_is_disabled(storage, tmp_path, legacy):
    admin, alice, _, resources, sharing, review, definition, _ = await setup_definition(storage, tmp_path, "agent")
    await sharing.decide(admin, review["review_id"], True, "")
    if legacy:
        async with storage.engine.begin() as conn:
            await conn.execute(text("UPDATE ak_approve SET content_snapshot=NULL WHERE review_id=:id"), {"id": review["review_id"]})
    await resources.set_asset_enabled(admin, "skill", "report", "1.0.0", False, admin=True)
    revoke = await sharing.withdraw(alice, review["review_id"])
    detail = await sharing.detail(admin, revoke["review_id"])
    assert detail["content"]["root"]["resource_id"] == definition["id"]
    assert any(node["resource_id"] == "report" for node in detail["content"]["dependencies"])
    await sharing.decide(admin, revoke["review_id"], True, "cleanup disabled dependency")
    await AgentManager(storage).delete_definition(alice, definition["id"])


async def test_review_download_uses_verified_shared_copy_when_owner_copy_is_corrupt(storage, tmp_path):
    import io
    import zipfile

    admin, _, _, _, sharing, review, _, _ = await setup_definition(storage, tmp_path, "agent")
    await sharing.decide(admin, review["review_id"], True, "")
    (tmp_path / "user/alice/skills/report/1.0.0/SKILL.md").write_text("corrupt owner version")
    detail = await sharing.detail(admin, review["review_id"])
    skill = next(node for node in detail["content"]["dependencies"] if node["resource_type"] == "skill")
    assert "corrupt owner version" not in skill["payload"]["markdown"]
    result = await sharing.skill_archive(admin, review["review_id"], "report", "1.0.0")
    with zipfile.ZipFile(io.BytesIO(result)) as package:
        markdown = next(name for name in package.namelist() if name.endswith("SKILL.md"))
        assert b"corrupt owner version" not in package.read(markdown)


async def test_owner_audience_state_tracks_active_grants_after_publication_update(storage):
    _, admin, alice, bob, agents, sharing, source, original = await shared_agent(storage)
    other = await sharing.submit(alice, "agent", source["id"], target=ShareTarget(
        principal_type="user", principal_id=admin.user_id,
    ))
    await sharing.decide(admin, other["review_id"], True, "")
    await agents.update_definition(alice, source["id"], AgentUpdate(
        agent_name=source["agent_name"], agent_display_name=source["agent_display_name"],
        owner_version=source["owner_version"],
        payload={**source["payload"], "description": "new shared publication"},
    ))
    update = await sharing.submit(alice, "agent", source["id"])
    await sharing.decide(admin, update["review_id"], True, "")
    rows = {row["review_id"]: row for row in await sharing.list(alice)}
    assert not rows[original["review_id"]]["is_current"]
    assert rows[original["review_id"]]["has_active_share"]
    assert rows[other["review_id"]]["has_active_share"]
    assert not rows[update["review_id"]]["has_active_share"]
    revoke = await sharing.withdraw(alice, original["review_id"])
    rows = {row["review_id"]: row for row in await sharing.list(alice)}
    assert rows[original["review_id"]]["has_active_share"]
    await sharing.decide(admin, revoke["review_id"], True, "")
    rows = {row["review_id"]: row for row in await sharing.list(alice)}
    assert not rows[original["review_id"]]["has_active_share"]
    assert rows[other["review_id"]]["has_active_share"]
    permissions = PermissionService(storage)
    assert not (await permissions.explain(bob, "agent", source["id"], "shared")).is_allowed
    assert (await permissions.explain(admin, "agent", source["id"], "shared")).is_allowed
