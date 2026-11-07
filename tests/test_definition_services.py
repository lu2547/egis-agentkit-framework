import asyncio
import importlib
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from test_domain import expert, identities
from test_packages import release

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.models import Definition, Grant
from egis_agentkit.packages import PackageLoader
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.access.snapshot import SnapshotService
from egis_agentkit.service.agent import AgentService
from egis_agentkit.service.agent.package import PackageService
from egis_agentkit.service.team import TeamService
from egis_agentkit.services import ShareService


def entry(module, symbol):
    try:
        return getattr(importlib.import_module(module), symbol)
    except (ImportError, AttributeError) as exc:
        pytest.fail(f"Missing definition-service entry point: {module}.{symbol}: {exc}")


@pytest.mark.parametrize(
    ("module", "symbol", "facade"),
    [
        ("egis_agentkit.services", "AgentService", "agent"),
        ("egis_agentkit.services.agents", "AgentService", "agent"),
        ("egis_agentkit.service.agent", "AgentService", "agent"),
        ("egis_agentkit.services", "TeamService", "team"),
        ("egis_agentkit.services.teams", "TeamService", "team"),
        ("egis_agentkit.service.team", "TeamService", "team"),
    ],
)
def test_legacy_entry_points_export_object_facades(module, symbol, facade):
    service = entry(f"egis_agentkit.service.{facade}.service", symbol)
    assert entry(module, symbol) is service


@pytest.mark.asyncio
async def test_agent_facade_and_definitions_share_state_and_policy(storage):
    _, _, alice, bob = await identities(storage)
    agents = AgentService(storage)
    assert hasattr(agents, "definitions"), "AgentService must compose definitions"
    assert agents.packages is None
    row = await agents.definitions.save(alice, expert())
    assert (await agents.raw_view(row["id"], "private"))["agent_name"] == "writer"
    assert (await agents.resolve(alice, row["id"], "private"))["id"] == row["id"]
    with pytest.raises(Forbidden):
        await agents.resolve(bob, row["id"], "private")
    catalog = await agents.list(alice)
    assert [(r["id"], r["is_usable"], r["is_editable"]) for r in catalog] == [
        (row["id"], True, True)
    ]
    blocked = AgentService(storage, SimpleNamespace(allow_private_self_use=False))
    with pytest.raises(Forbidden):
        await blocked.resolve(alice, row["id"], "private")
    personal = await asyncio.gather(*(agents.personal(alice) for _ in range(3)))
    assert len({r["id"] for r in personal}) == 1
    assert personal[0]["agent_type"] == "personal"
    await agents.delete(alice, row["id"])
    with pytest.raises(Forbidden):
        await agents.definitions.raw_view(row["id"], "private")


@pytest.mark.asyncio
async def test_agent_facade_composes_optional_package_service(storage, tmp_path):
    _, admin, alice, _ = await identities(storage)
    packages = PackageService(storage, PackageLoader([tmp_path]))
    agents = AgentService(storage, packages=packages)
    registered = await agents.packages.register(admin, release(tmp_path, "1.0.0"))
    assert registered["resource_id"] == "test.report"
    assembly = await agents.packages.resolve("test.report", "1.0.0")
    assert await assembly.tools_factory(None) == ["1.0.0"]
    row = await agents.save(alice, expert())
    assert [r["id"] for r in await agents.list(alice)] == [row["id"]]
    with pytest.raises(Forbidden):
        await agents.packages.list(alice)


@pytest.mark.asyncio
async def test_facade_save_preserves_caller_transaction_and_team_role(storage):
    _, _, alice, _ = await identities(storage)
    agents = AgentService(storage)
    assert hasattr(agents, "definitions"), "AgentService must compose definitions"
    leader = expert(name="leader")
    leader.agent_type = "orchestrator"
    with pytest.raises(Forbidden):
        await agents.save(alice, leader)
    with pytest.raises(RuntimeError, match="rollback"):
        async with storage.transaction() as conn:
            row = await agents.save(alice, leader, conn=conn, allow_team_role=True)
            assert (await agents.raw_view(row["id"], "private", conn=conn))[
                "agent_type"
            ] == "orchestrator"
            raise RuntimeError("rollback")
    with pytest.raises(NotFound):
        await agents.raw_view(leader.id, "private")


@pytest.mark.asyncio
async def test_team_facade_preserves_atomic_updates_and_owner_checks(storage):
    _, _, alice, bob = await identities(storage)
    agents, teams = AgentService(storage), TeamService(storage)
    assert hasattr(teams, "definitions"), "TeamService must compose definitions"
    member = await agents.save(alice, expert())
    data = {
        "team_name": "delivery",
        "team_display_name": "Delivery",
        "orchestrator_config": {"agent_name": "leader", "agent_display_name": "Leader"},
        "members": [{"definition_id": member["id"], "resource_view_type": "private"}],
    }
    team = await teams.definitions.save(alice, data)
    leader_id = team["payload"]["orchestrator"]["definition_id"]
    data.update(id=team["id"], config_version=team["config_version"])
    assert (await teams.save(alice, data))["config_version"] == 2
    with pytest.raises(Conflict):
        await teams.save(alice, data)
    assert (await agents.raw_view(leader_id, "private"))["config_version"] == 2
    assert (await agents.list(alice, definition_type="team"))[0]["id"] == team["id"]
    with pytest.raises(Forbidden):
        await teams.save(bob, {**data, "config_version": 2})
    with pytest.raises(Forbidden):
        await teams.save(alice, {**data, "id": None, "members": data["members"] * 2})


@pytest.mark.asyncio
async def test_definition_views_are_engine_only_reads_not_use_authorization(storage):
    views_type = entry("egis_agentkit.service.agent.views", "DefinitionViews")
    _, admin, alice, bob = await identities(storage)
    agents, shares = AgentService(storage), ShareService(storage)
    value = expert()
    row = await agents.save(alice, value)
    views = views_type(SimpleNamespace(engine=storage.engine))
    assert (await views.raw_view(row["id"], "private"))["owner_id"] == "alice"
    with pytest.raises(Forbidden):
        await agents.resolve(bob, row["id"], "private")
    with pytest.raises(Forbidden):
        await views.raw_view(row["id"], "system")
    with pytest.raises(Forbidden):
        await views.raw_view(row["id"], "shared")
    review = await shares.submit(alice, "agent", row["id"])
    await shares.decide(admin, review["review_id"], True, "approved")
    value.payload["system_prompt"] = "unreviewed"
    await agents.save(alice, value)
    shared = await views.raw_view(row["id"], "shared")
    assert shared["payload"]["system_prompt"] == "旧版"
    assert shared["shared_review_id"] == review["review_id"]
    assert shared["shared_version"] == 1
    shared["payload"]["system_prompt"] = "mutated copy"
    assert (await views.raw_view(row["id"], "shared"))["payload"]["system_prompt"] == "旧版"
    await agents.delete(alice, row["id"])
    with pytest.raises(Forbidden):
        await views.raw_view(row["id"], "shared")
    with pytest.raises(NotFound):
        await views.raw_view("missing", "private")


@pytest.mark.asyncio
async def test_permissions_and_snapshots_do_not_call_definition_service(storage, monkeypatch):
    manager = entry("egis_agentkit.service.agent.definition", "AgentDefinitionManager")
    _, admin, alice, _ = await identities(storage)
    agents = AgentService(storage)
    row = await agents.save(admin, expert("sys", "system"))
    permissions = PermissionService(storage)
    await permissions.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id="alice",
            resource_type="agent",
            resource_id=row["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="sys",
        ),
    )

    async def unavailable(*args, **kwargs):
        raise AssertionError("Snapshot/Permission must not call Agent definition logic")

    monkeypatch.setattr(manager, "raw_view", unavailable)
    monkeypatch.setattr(AgentService, "raw_view", unavailable)
    snapshot = await SnapshotService(storage).capture(row["id"], "system")
    assert snapshot["root"]["agent_name"] == "writer"
    assert (await permissions.explain(alice, "agent", row["id"], "system")).is_allowed
    await permissions.validate_snapshot(alice, snapshot)


@pytest.mark.asyncio
async def test_definition_storage_reads_catalog_reviews_and_caller_transaction(storage):
    repository_type = entry("egis_agentkit.storage.definition", "DefinitionStorage")
    _, admin, alice, _ = await identities(storage)
    repository = repository_type(storage.engine)
    agents, shares = AgentService(storage), ShareService(storage)
    value = expert()
    row = await agents.save(alice, value)
    assert (await repository.get_definition(row["id"]))["config_version"] == 1
    assert await repository.approved_review(row["id"]) is None
    async with storage.transaction() as conn:
        assert await repository.locked_definition("missing", conn=conn) is None
        assert (await repository.get_definition(row["id"], conn=conn))["id"] == row["id"]
    review = await shares.submit(alice, "agent", row["id"])
    await shares.decide(admin, review["review_id"], True, "approved")
    assert (await repository.approved_review(row["id"]))["review_id"] == review["review_id"]
    assert await repository.approved_review(row["id"], resource_type="skill") is None
    rows, states = await repository.read_catalog()
    assert [r["id"] for r in rows] == [row["id"]]
    assert states == {row["id"]: "approved"}
    assert await repository.read_catalog("team") == ([], states)
    with pytest.raises(NotFound):
        await repository.get_definition("missing")


@pytest.mark.asyncio
async def test_locked_definition_retains_row_lock_and_concurrent_save_conflicts(storage):
    repository_type = entry("egis_agentkit.storage.definition", "DefinitionStorage")
    _, _, alice, _ = await identities(storage)
    agents = AgentService(storage)
    row = await agents.save(alice, expert())
    repository = repository_type(storage.engine)
    async with storage.transaction() as first:
        await repository.locked_definition(row["id"], conn=first)
        with pytest.raises(DBAPIError) as error:
            async with storage.transaction() as second:
                await second.execute(text("SET LOCAL lock_timeout = '100ms'"))
                await repository.locked_definition(row["id"], conn=second)
        assert error.value.orig.sqlstate == "55P03"
    changed = Definition.model_validate({key: row[key] for key in Definition.model_fields})
    changed.payload["system_prompt"] = "new"
    results = await asyncio.gather(
        agents.save(alice, changed), agents.save(alice, changed), return_exceptions=True
    )
    assert sum(isinstance(result, Conflict) for result in results) == 1
    assert [result["config_version"] for result in results if isinstance(result, dict)] == [2]
