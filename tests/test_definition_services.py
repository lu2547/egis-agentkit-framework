import asyncio
import importlib
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from test_domain import COMPANY, expert, identities
from test_remote_agents import config as remote_config

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._expert_definition import ExpertDefinition
from egis_agentkit.service.agent._orchestrator_definition import OrchestratorDefinition
from egis_agentkit.service.agent._personal_definition import PersonalDefinition
from egis_agentkit.service.agent._query import AgentQuery
from egis_agentkit.service.agent._remote_definition import RemoteDefinition
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.storage._definition import DefinitionStorage
from egis_agentkit.storage.model import Definition, Grant


def entry(module, symbol):
    try:
        return getattr(importlib.import_module(module), symbol)
    except (ImportError, AttributeError) as exc:
        pytest.fail(f"Missing definition-service entry point: {module}.{symbol}: {exc}")


@pytest.mark.parametrize(
    ("module", "symbol"),
    [
        ("egis_agentkit.service.agent._definition_base", "AgentDefinitionBase"),
        ("egis_agentkit.service.agent._query", "AgentQuery"),
        ("egis_agentkit.service.agent._personal_definition", "PersonalDefinition"),
        ("egis_agentkit.service.agent._expert_definition", "ExpertDefinition"),
        ("egis_agentkit.service.agent._remote_definition", "RemoteDefinition"),
        (
            "egis_agentkit.service.agent._orchestrator_definition",
            "OrchestratorDefinition",
        ),
        ("egis_agentkit.service.team._definition", "TeamDefinition"),
    ],
)
def test_definition_entry_points_export_services(module, symbol):
    assert entry(module, symbol).__module__ == module


@pytest.mark.asyncio
async def test_agent_query_and_definitions_share_state_and_policy(storage):
    _, _, alice, bob = await identities(storage)
    agents = ExpertDefinition(storage)
    definition = expert()
    with pytest.raises(NotFound):
        await agents.save(alice, definition)
    row = await agents.build(alice, definition)
    with pytest.raises(Conflict):
        await agents.build(alice, definition)
    assert (await AgentQuery(storage).read(alice, row["id"], "private"))[
        "agent_name"
    ] == "writer"
    assert (await AgentQuery(storage).read_for_use(alice, row["id"], "private"))[
        "id"
    ] == row["id"]
    with pytest.raises(Forbidden):
        await AgentQuery(storage).read_for_use(bob, row["id"], "private")
    catalog = (await AgentQuery(storage).list(alice))["items"]
    assert [(r["id"], r["is_usable"], r["is_editable"]) for r in catalog] == [
        (row["id"], True, True)
    ]
    blocked = AgentQuery(storage, SimpleNamespace(allow_private_self_use=False))
    with pytest.raises(Forbidden):
        await blocked.read_for_use(alice, row["id"], "private")
    personal = await asyncio.gather(
        *(PersonalDefinition(storage).build(alice) for _ in range(3))
    )
    assert len({r["id"] for r in personal}) == 1
    assert personal[0]["agent_type"] == "personal"
    await agents.disable(alice, row["id"])
    with pytest.raises(Forbidden):
        await AgentQuery(storage).read(alice, row["id"], "private")


@pytest.mark.asyncio
async def test_orchestrator_build_uses_team_transaction(storage):
    _, _, alice, _ = await identities(storage)
    agents = ExpertDefinition(storage)
    leader = expert(name="leader")
    leader.agent_type = "orchestrator"
    with pytest.raises(Forbidden):
        await agents.build(alice, leader)
    with pytest.raises(RuntimeError, match="rollback"):
        async with storage.definition_write() as write:
            row = await OrchestratorDefinition(write).build(alice, leader)
            assert (await write.definitions.read_view(row["id"], "private"))[
                "agent_type"
            ] == "orchestrator"
            raise RuntimeError("rollback")
    with pytest.raises(NotFound):
        await storage.definitions.read_view(leader.id, "private")


@pytest.mark.asyncio
async def test_team_preserves_atomic_updates_and_owner_checks(storage):
    _, _, alice, bob = await identities(storage)
    agents, teams = ExpertDefinition(storage), TeamDefinition(storage)
    member = await agents.build(alice, expert())
    data = {
        "team_name": "delivery",
        "team_display_name": "Delivery",
        "orchestrator_config": {"agent_name": "leader", "agent_display_name": "Leader"},
        "members": [{"definition_id": member["id"], "resource_view_type": "private"}],
    }
    team = await teams.save(alice, data)
    leader_id = team["payload"]["orchestrator"]["definition_id"]
    data.update(id=team["id"], config_version=team["config_version"])
    assert (await teams.save(alice, data))["config_version"] == 2
    with pytest.raises(Conflict):
        await teams.save(alice, data)
    assert (await storage.definitions.read_view(leader_id, "private"))[
        "config_version"
    ] == 2
    assert (await teams.list(alice))[0]["id"] == team["id"]
    with pytest.raises(Conflict):
        await teams.save(
            alice,
            {
                **data,
                "id": None,
                "config_version": 1,
                "orchestrator_config": {
                    "agent_name": "rolled-back",
                    "agent_display_name": "Rolled back",
                },
            },
        )
    agents_after_conflict, _ = await storage.definitions.read_catalog("agent")
    assert {row["agent_name"] for row in agents_after_conflict} == {"leader", "writer"}
    with pytest.raises(Forbidden):
        await teams.save(bob, {**data, "config_version": 2})
    with pytest.raises(Forbidden):
        await teams.save(alice, {**data, "id": None, "members": data["members"] * 2})
    await teams.delete(alice, team["id"])
    assert not (await storage.get_definition(team["id"]))["is_enabled"]
    assert not (await storage.get_definition(leader_id))["is_enabled"]
    assert (await storage.get_definition(member["id"]))["is_enabled"]


@pytest.mark.asyncio
async def test_definition_views_are_engine_only_reads_not_use_authorization(storage):
    _, admin, alice, bob = await identities(storage)
    agents, shares = ExpertDefinition(storage), ShareService(storage)
    value = expert()
    row = await agents.build(alice, value)
    views = DefinitionStorage(storage.engine)
    assert (await views.read_view(row["id"], "private"))["owner_id"] == "alice"
    with pytest.raises(Forbidden):
        await AgentQuery(storage).read_for_use(bob, row["id"], "private")
    with pytest.raises(Forbidden):
        await views.read_view(row["id"], "system")
    with pytest.raises(Forbidden):
        await views.read_view(row["id"], "shared")
    review = await shares.submit(alice, "agent", row["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "approved")
    value.payload["system_prompt"] = "unreviewed"
    await agents.save(alice, value)
    shared = await views.read_view(row["id"], "shared", review_id=review["review_id"])
    assert shared["payload"]["system_prompt"] == "旧版"
    assert shared["shared_review_id"] == review["review_id"]
    assert shared["resource_version"] == "1"
    shared["payload"]["system_prompt"] = "mutated copy"
    assert (await views.read_view(row["id"], "shared", version="1"))["payload"][
        "system_prompt"
    ] == "旧版"
    await agents.disable(alice, row["id"])
    with pytest.raises(Forbidden):
        await views.read_view(row["id"], "shared")
    with pytest.raises(NotFound):
        await views.read_view("missing", "private")


@pytest.mark.asyncio
async def test_permissions_and_snapshots_do_not_call_definition_service(
    storage, monkeypatch
):
    _, admin, alice, _ = await identities(storage)
    agents = ExpertDefinition(storage)
    row = await agents.build(admin, expert("sys", "system"))
    permissions = PermissionService(storage)
    await GrantService(storage).grant(
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

    monkeypatch.setattr(AgentQuery, "read", unavailable)
    monkeypatch.setattr(AgentQuery, "read_for_use", unavailable)
    snapshot = await storage.definitions.read_view(row["id"], "system")
    assert snapshot["agent_name"] == "writer"
    assert (await permissions.explain(alice, "agent", row["id"], "system")).is_allowed
    await permissions.require_execution(alice, snapshot)


@pytest.mark.asyncio
async def test_definition_storage_reads_catalog_reviews_and_caller_transaction(storage):
    repository_type = entry("egis_agentkit.storage._definition", "DefinitionStorage")
    _, admin, alice, _ = await identities(storage)
    repository = repository_type(storage.engine)
    agents, shares = ExpertDefinition(storage), ShareService(storage)
    value = expert()
    row = await agents.build(alice, value)
    assert (await repository.get_definition(row["id"]))["config_version"] == 1
    assert await repository.approved_review(row["id"]) is None
    async with storage.transaction() as conn:
        assert await repository.locked_definition("missing", conn=conn) is None
        assert (await repository.get_definition(row["id"], conn=conn))["id"] == row[
            "id"
        ]
    review = await shares.submit(alice, "agent", row["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "approved")
    assert (await repository.approved_review(row["id"]))["review_id"] == review[
        "review_id"
    ]
    assert await repository.approved_review(row["id"], resource_type="skill") is None
    rows, states = await repository.read_catalog()
    assert [r["id"] for r in rows] == [row["id"]]
    assert states == {(row["id"], "1"): "approved"}
    assert await repository.read_catalog("team") == ([], {})
    with pytest.raises(NotFound):
        await repository.get_definition("missing")


@pytest.mark.asyncio
async def test_locked_definition_retains_row_lock_and_concurrent_save_conflicts(
    storage,
):
    repository_type = entry("egis_agentkit.storage._definition", "DefinitionStorage")
    _, _, alice, _ = await identities(storage)
    agents = ExpertDefinition(storage)
    row = await agents.build(alice, expert())
    repository = repository_type(storage.engine)
    async with storage.transaction() as first:
        await repository.locked_definition(row["id"], conn=first)
        with pytest.raises(DBAPIError) as error:
            async with storage.transaction() as second:
                await second.execute(text("SET LOCAL lock_timeout = '100ms'"))
                await repository.locked_definition(row["id"], conn=second)
        assert error.value.orig.sqlstate == "55P03"
    changed = Definition.model_validate(
        {key: row[key] for key in Definition.model_fields}
    )
    changed.payload["system_prompt"] = "new"
    results = await asyncio.gather(
        agents.save(alice, changed), agents.save(alice, changed), return_exceptions=True
    )
    assert sum(isinstance(result, Conflict) for result in results) == 1
    assert [
        result["config_version"] for result in results if isinstance(result, dict)
    ] == [2]


@pytest.mark.asyncio
async def test_agent_query_pages_visible_rows_and_filters_definition_type(storage):
    _, admin, alice, bob = await identities(storage)
    experts, query = ExpertDefinition(storage), AgentQuery(storage)
    older = await experts.build(alice, expert(name="older"))
    newer = await experts.build(alice, expert(name="newer"))
    await experts.build(bob, expert("bob", name="hidden"))
    personal = await PersonalDefinition(storage).build(alice)
    remote = await RemoteDefinition(storage).build(
        admin,
        Definition(
            owner_id=alice.user_id,
            agent_type="remote",
            agent_name="finance",
            agent_display_name="财务专家",
            payload={
                "remote_config": remote_config().model_dump(mode="json"),
            },
        ),
    )

    page = await query.list(alice, types=("expert",), limit=1)
    assert [row["id"] for row in page["items"]] == [newer["id"]]
    assert page["next_cursor"]
    final = await query.list(
        alice, types=("expert",), limit=1, cursor=page["next_cursor"]
    )
    assert [row["id"] for row in final["items"]] == [older["id"]]
    assert final["next_cursor"] is None
    personal_page = await query.list(alice, types=("personal",))
    assert [row["id"] for row in personal_page["items"]] == [personal["id"]]
    remote_page = await query.list(alice, types=("remote",))
    assert [row["id"] for row in remote_page["items"]] == [remote["id"]]
    default_page = await query.list(alice)
    assert [row["id"] for row in default_page["items"]] == [
        remote["id"],
        newer["id"],
        older["id"],
    ]
    with pytest.raises(ValueError):
        await query.list(alice, types=("personal",), cursor=page["next_cursor"])

    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", remote["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "approved")
    native = Definition.model_validate(
        {
            **{key: remote[key] for key in Definition.model_fields},
            "agent_type": "expert",
            "payload": {},
        }
    )
    with pytest.raises(Forbidden, match="改变资源归属或类型"):
        await experts.save(alice, native)
    with pytest.raises(Forbidden, match="改变资源归属或类型"):
        await experts.save(admin, native)
    assert (await query.list(bob, types=("expert",)))["items"][0]["id"] != remote["id"]
    shared_remote = (await query.list(bob, types=("remote",)))["items"]
    assert [row["id"] for row in shared_remote] == [remote["id"]]
    assert shared_remote[0]["resource_view_type"] == "shared"


@pytest.mark.asyncio
async def test_agent_catalog_uses_configured_default_page_and_cap(storage, tmp_path):
    from test_app_http import app_snapshot

    _, _, alice, _ = await identities(storage)
    await ExpertDefinition(storage).build(alice, expert(name="older"))
    latest = await ExpertDefinition(storage).build(alice, expert(name="latest"))
    provider = SimpleNamespace(
        snapshot=app_snapshot(
            tmp_path,
            agent={
                "catalog": {
                    "default_page_size": 1,
                    "max_page_size": 2,
                    "candidate_batch_size": 1,
                }
            },
        )
    )
    query = AgentQuery(storage, config_provider=provider)
    page = await query.list(alice)
    assert [row["id"] for row in page["items"]] == [latest["id"]]
    assert page["next_cursor"]
    with pytest.raises(ValueError, match="2"):
        await query.list(alice, limit=3)


@pytest.mark.asyncio
async def test_team_roster_enforces_configured_limit_before_writing(storage, tmp_path):
    from test_app_http import app_snapshot

    _, _, alice, _ = await identities(storage)
    provider = SimpleNamespace(
        snapshot=app_snapshot(tmp_path, team={"roster": {"max_members": 1}})
    )
    with pytest.raises(ValueError, match="1"):
        await TeamDefinition(storage, config_provider=provider).save(
            alice,
            {
                "orchestrator_config": {"agent_name": "leader"},
                "members": [{"definition_id": "one"}, {"definition_id": "two"}],
            },
        )
