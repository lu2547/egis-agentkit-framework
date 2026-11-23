"""Bounded authorization reads preserve live revocation and pinned references."""

from contextlib import contextmanager

import pytest
from sqlalchemy import event, update
from test_composer import composer
from test_domain import COMPANY, agent_changes, expert, identities
from test_resources import put_asset

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.team._candidates import ExpertCandidates
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import utcnow


@contextmanager
def queries(storage):
    statements = []

    def record(conn, cursor, statement, parameters, context, executemany):
        statements.append(statement)

    event.listen(storage.engine.sync_engine, "before_cursor_execute", record)
    try:
        yield statements
    finally:
        event.remove(storage.engine.sync_engine, "before_cursor_execute", record)


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 10, 50])
async def test_expert_candidate_and_composer_reads_are_bounded(
    storage, tmp_path, count
):
    _, _, alice, _ = await identities(storage)
    candidates = ExpertCandidates(PermissionService(storage))
    expected = set()
    for index in range(count):
        value = expert(name=f"writer{index:02}")
        value.id = f"expert{index:02}"
        await AgentManager(storage).add_definition(alice, value)
        await storage.expert_candidates.add(alice.user_id, value.id, "private")
        expected.add(value.id)
    with queries(storage) as selected:
        rows = await candidates.resolve(alice)
    assert {row["id"] for row in rows} == expected
    print(f"PERMISSION_SQL candidates count={count} queries={len(selected)}")
    assert len(selected) <= 7, len(selected)
    from types import SimpleNamespace

    from test_app_http import app_snapshot

    service, _ = composer(
        storage,
        tmp_path,
        config=SimpleNamespace(
            snapshot=app_snapshot(
                tmp_path,
                session={
                    "composer": {"max_page_size": 50, "candidate_batch_size": 100}
                },
            )
        ),
    )
    with queries(storage) as selected:
        page = await service.options(alice, "experts", limit=min(count, 50))
    assert {item["id"] for item in page["items"]} == expected
    assert all(
        item["definition"]["payload"]["system_prompt"] == "旧版"
        for item in page["items"]
    )
    print(f"PERMISSION_SQL composer_experts count={count} queries={len(selected)}")
    assert len(selected) <= 7, len(selected)
    async with storage.transaction() as conn:
        await conn.execute(
            update(t.users)
            .where(t.users.c.user_id == alice.user_id)
            .values(is_enabled=False)
        )
    assert await candidates.resolve(alice) == []
    assert (await service.options(alice, "experts"))["items"] == []


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 10, 50])
async def test_execution_and_sharing_batch_dependencies(storage, count):
    _, admin, alice, bob = await identities(storage)
    definition = expert()
    definition.payload["bindings"] = []
    for index in range(count):
        identifier = f"skill{index:02}"
        await put_asset(
            storage, rid=identifier, owner_type="user", owner_id=alice.user_id
        )
        definition.payload["bindings"].append(
            {
                "resource_type": "skill",
                "resource_id": identifier,
                "resource_version": "1.0.0",
                "resource_view_type": "private",
            }
        )
    with queries(storage) as selected:
        row = await AgentManager(storage).add_definition(alice, definition)
    print(f"PERMISSION_SQL definition_save bindings={count} queries={len(selected)}")
    assert len(selected) <= 14, len(selected)
    permissions = PermissionService(storage)
    with queries(storage) as selected:
        await permissions.require_execution(
            alice, {**row, "resource_view_type": "private"}
        )
    print(f"PERMISSION_SQL execution bindings={count} queries={len(selected)}")
    assert len(selected) <= 6, len(selected)
    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", row["id"], target=COMPANY)
    with queries(storage) as selected:
        await shares.decide(admin, review["review_id"], True, "")
    print(f"PERMISSION_SQL share_approve bindings={count} queries={len(selected)}")
    assert len(selected) <= 30, len(selected)
    root = await storage.definitions.get_definition_view(row["id"], "shared")
    await permissions.require_execution(bob, root)
    grants = await storage.access.direct_grants("agent", row["id"])
    await GrantService(storage).revoke(admin, grants[0]["id"])
    with pytest.raises(Forbidden):
        await permissions.require_execution(bob, root)


@pytest.mark.asyncio
async def test_read_snapshot_preserves_pinned_versions_and_live_group_status(storage):
    users, admin, alice, bob = await identities(storage)
    agents, shares = AgentManager(storage), ShareService(storage)
    value = expert()
    row = await agents.add_definition(alice, value)
    review = await shares.submit(alice, "agent", row["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "")
    grant = (await storage.access.direct_grants("agent", row["id"]))[0]
    reference = {
        "resource_type": "agent",
        "resource_id": row["id"],
        "resource_view_type": "shared",
        "resource_version": "1",
        "grant_id": grant["id"],
        "allow_pinned": True,
    }
    value.payload["system_prompt"] = "新版"
    await agents.update_definition(alice, value.id, agent_changes(value))
    pending = await shares.submit(alice, "agent", value.id)
    await shares.decide(admin, pending["review_id"], True, "")
    snapshot = await PermissionService(storage).read_snapshot(bob, [reference])
    assert (await snapshot.definition_view(reference))["payload"][
        "system_prompt"
    ] == "旧版"
    current = {
        "resource_type": "agent",
        "resource_id": row["id"],
        "resource_view_type": "shared",
    }
    assert (await snapshot.definition_view(current))["access_grant_id"] == grant["id"]
    assert (
        await snapshot.explain(
            "agent",
            row["id"],
            "shared",
            version="1",
            grant_id=grant["id"],
            allow_pinned=True,
        )
    ).is_allowed
    async with storage.transaction() as conn:
        await conn.execute(
            update(t.groups)
            .where(t.groups.c.group_id == "company")
            .values(is_enabled=False)
        )
    snapshot = await PermissionService(storage).read_snapshot(bob, [reference])
    assert not (
        await snapshot.explain(
            "agent",
            row["id"],
            "shared",
            version="1",
            grant_id=grant["id"],
            allow_pinned=True,
        )
    ).is_allowed


@pytest.mark.asyncio
async def test_snapshot_resolves_dependency_source_without_explicit_version(storage):
    _, admin, alice, bob = await identities(storage)
    await put_asset(storage, rid="bound", owner_type="user", owner_id="alice")
    value = expert()
    value.payload["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": "bound",
            "resource_version": "1.0.0",
            "resource_view_type": "private",
        }
    ]
    row = await AgentManager(storage).add_definition(alice, value)
    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", row["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "")
    root = (await storage.access.direct_grants("agent", row["id"]))[0]
    reference = {
        "resource_type": "skill",
        "resource_id": "bound",
        "resource_view_type": "private",
        "root_grant_id": root["id"],
        "agent_id": row["id"],
    }
    snapshot = await PermissionService(storage).read_snapshot(bob, [reference])
    decision = await snapshot.explain(
        "skill", "bound", "private", root_grant_id=root["id"], agent_id=row["id"]
    )
    assert decision.is_allowed and decision.resource_version == "1.0.0"
    async with storage.transaction() as conn:
        await conn.execute(
            update(t.agent_definition_summary)
            .where(t.agent_definition_summary.c.id == row["id"])
            .values(deleted_at=utcnow())
        )
    snapshot = await PermissionService(storage).read_snapshot(bob, [reference])
    assert not (
        await snapshot.explain(
            "skill", "bound", "private", root_grant_id=root["id"], agent_id=row["id"]
        )
    ).is_allowed


@pytest.mark.asyncio
async def test_large_grant_refresh_preserves_ids_and_revokes_removed_edges(storage):
    _, admin, alice, _ = await identities(storage)
    definition = expert("platform", "system", "bulk")
    definition.payload["bindings"] = []
    for index in range(50):
        identifier = f"skill{index:02}"
        await put_asset(storage, rid=identifier)
        definition.payload["bindings"].append(
            {
                "resource_type": "skill",
                "resource_id": identifier,
                "resource_version": "1.0.0",
                "resource_view_type": "system",
            }
        )
    row = await AgentManager(storage).add_definition(admin, definition)
    async with storage.definition_write() as write:
        roots = await write.access.upsert_grants(
            [
                {
                    "id": f"root{index:02}",
                    "principal_type": "user",
                    "principal_id": f"recipient{index:02}",
                    "resource_type": "agent",
                    "resource_id": row["id"],
                    "resource_view_type": "system",
                    "resource_owner_type": "system",
                    "resource_owner_id": "platform",
                    "grant_source_type": "manual",
                    "origin_key": "manual",
                }
                for index in range(50)
            ],
            actor=admin.user_id,
        )
        with queries(storage) as selected:
            await GrantService(write).refresh_dependencies(
                row, actor=admin.user_id, write=write
            )
    print(f"PERMISSION_SQL grant_refresh roots=50 bindings=50 queries={len(selected)}")
    assert len(selected) <= 6, len(selected)
    before = await storage.access.active_grants_for_resources(
        "skill", {f"skill{index:02}" for index in range(50)}
    )
    assert len(before) == 2500
    assert {grant["root_grant_id"] for grant in before} == {
        root["id"] for root in roots
    }
    row["payload"]["bindings"] = row["payload"]["bindings"][:1]
    async with storage.definition_write() as write:
        await GrantService(write).refresh_dependencies(
            row, actor=admin.user_id, write=write
        )
    after = await storage.access.active_grants_for_resources(
        "skill", {f"skill{index:02}" for index in range(50)}
    )
    assert len(after) == 50
    assert {grant["id"] for grant in after} == {
        grant["id"] for grant in before if grant["resource_id"] == "skill00"
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("count", [1, 10, 50])
async def test_composer_skill_page_reuses_authorized_selection(
    storage, tmp_path, count
):
    from types import SimpleNamespace

    from test_app_http import app_snapshot

    from egis_agentkit.service.agent._personal_manager import PersonalManager

    _, _, alice, _ = await identities(storage)
    personal = await PersonalManager(storage).get_or_add_definition(alice)
    for index in range(count):
        identifier = f"skill{index:02}"
        await put_asset(storage, rid=identifier, owner_type="user", owner_id="alice")
        await storage.personal_resources.save_resource(
            "alice", personal["id"], "skill", identifier, True, actor="alice"
        )
    service, _ = composer(
        storage,
        tmp_path,
        config=SimpleNamespace(
            snapshot=app_snapshot(
                tmp_path,
                session={
                    "composer": {"max_page_size": 50, "candidate_batch_size": 100}
                },
            )
        ),
    )
    with queries(storage) as selected:
        result = await service.options(alice, "skills", limit=count)
    assert {item["id"] for item in result["items"]} == {
        f"skill{index:02}@1.0.0" for index in range(count)
    }
    print(f"PERMISSION_SQL composer_skills count={count} queries={len(selected)}")
    assert len(selected) <= 7, len(selected)


@pytest.mark.asyncio
async def test_loaded_tool_still_checks_current_version_with_semantic_order(storage):
    from egis_agentkit.storage.model import Asset

    _, _, alice, _ = await identities(storage)
    async with storage.transaction() as conn:
        for version in ("2.9.0", "2.10.0rc1", "2.10.0"):
            await storage.assets.insert_asset(
                Asset(
                    resource_type="tool",
                    resource_id="VersionedTool",
                    resource_version=version,
                    owner_type="system",
                    owner_id="system",
                    payload={"exposure": "model", "source": "toolkit"},
                ),
                actor="system",
                conn=conn,
            )
    historical = await storage.assets.get("tool", "VersionedTool", "2.9.0")
    reference = {
        "resource_type": "tool",
        "resource_id": "VersionedTool",
        "resource_version": "2.9.0",
        "resource_view_type": "system",
    }
    snapshot = await PermissionService(storage).read_snapshot(
        alice, [reference], loaded=[historical]
    )
    assert not (
        await snapshot.explain("tool", "VersionedTool", "system", version="2.9.0")
    ).is_allowed
    assert (
        await snapshot.explain(
            "tool", "VersionedTool", "system", version="2.9.0", allow_pinned=True
        )
    ).is_allowed
    assert (
        await snapshot.explain("tool", "VersionedTool", "system", version="2.10.0")
    ).is_allowed


@pytest.mark.asyncio
async def test_personal_definition_uses_current_read_and_locked_creation(storage):
    from egis_agentkit.service.agent._personal_manager import PersonalManager

    _, _, alice, _ = await identities(storage)
    manager = PersonalManager(storage)
    with queries(storage) as selected:
        first = await manager.get_or_add_definition(alice)
    assert first["agent_type"] == "personal" and first["owner_id"] == "alice"
    assert len(selected) <= 7, len(selected)
    with queries(storage) as selected:
        second = await manager.get_or_add_definition(alice)
    assert second["id"] == first["id"]
    assert len(selected) <= 1, len(selected)

    async with storage.transaction() as conn:
        await conn.execute(
            update(t.agent_definition_summary)
            .where(t.agent_definition_summary.c.id == first["id"])
            .values(deleted_at=utcnow())
        )
    assert (await manager.get_or_add_definition(alice))["deleted_at"] is not None
    async with storage.transaction() as conn:
        await conn.execute(
            update(t.agent_definition_summary)
            .where(t.agent_definition_summary.c.id == first["id"])
            .values(agent_type="expert")
        )
    with pytest.raises(Conflict, match="个人助理 ID 已被占用"):
        await manager.get_or_add_definition(alice)
