import asyncio
import importlib
from types import SimpleNamespace

import pytest
from sqlalchemy import text
from sqlalchemy.exc import DBAPIError
from test_domain import COMPANY, agent_changes, expert, identities
from test_remote_agents import config as remote_config

from egis_agentkit.core.agent._expert_definition import ExpertDefinition
from egis_agentkit.core.agent._orchestrator_definition import OrchestratorDefinition
from egis_agentkit.core.agent._personal_definition import PersonalDefinition
from egis_agentkit.core.agent._remote_definition import RemoteDefinition
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.storage._definition import DefinitionStorage
from egis_agentkit.storage.model import AgentDefinition, Grant


def entry(module, symbol):
    try:
        return getattr(importlib.import_module(module), symbol)
    except (ImportError, AttributeError) as exc:
        pytest.fail(f"Missing definition-service entry point: {module}.{symbol}: {exc}")


@pytest.mark.parametrize(
    ("module", "symbol"),
    [
        ("egis_agentkit.core.agent._definition_base", "AgentDefinitionBase"),
        ("egis_agentkit.service.agent._agent_manager", "AgentManager"),
        ("egis_agentkit.core.agent._personal_definition", "PersonalDefinition"),
        ("egis_agentkit.core.agent._expert_definition", "ExpertDefinition"),
        ("egis_agentkit.core.agent._remote_definition", "RemoteDefinition"),
        (
            "egis_agentkit.core.agent._orchestrator_definition",
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
    agents = AgentManager(storage)
    definition = expert()
    with pytest.raises(NotFound):
        await agents.update_definition(alice, (definition).id, agent_changes(definition))
    row = await agents.add_definition(alice, definition)
    with pytest.raises(Conflict):
        await agents.add_definition(alice, definition)
    assert (await AgentManager(storage).get_definition(alice, row["id"], "private"))[
        "agent_name"
    ] == "writer"
    assert (await AgentManager(storage).get_definition_for_use(alice, row["id"], "private"))[
        "id"
    ] == row["id"]
    with pytest.raises(Forbidden):
        await AgentManager(storage).get_definition_for_use(bob, row["id"], "private")
    catalog = (await AgentManager(storage).page_definitions(alice))["items"]
    assert [(r["id"], r["is_usable"], r["is_editable"]) for r in catalog] == [
        (row["id"], True, True)
    ]
    blocked = AgentManager(storage, SimpleNamespace(allow_private_self_use=False))
    with pytest.raises(Forbidden):
        await blocked.get_definition_for_use(alice, row["id"], "private")
    personal = await asyncio.gather(
        *(PersonalManager(storage).get_or_add_definition(alice) for _ in range(3))
    )
    assert len({r["id"] for r in personal}) == 1
    assert personal[0]["agent_type"] == "personal"
    await agents.delete_definition(alice, row["id"])
    with pytest.raises(Forbidden):
        await AgentManager(storage).get_definition(alice, row["id"], "private")


@pytest.mark.asyncio
async def test_orchestrator_build_uses_team_transaction(storage):
    _, _, alice, _ = await identities(storage)
    agents = AgentManager(storage)
    leader = expert(name="leader")
    leader.agent_type = "orchestrator"
    with pytest.raises(Forbidden):
        await agents.add_definition(alice, leader)
    member = await agents.add_definition(alice, expert())
    with pytest.raises(RuntimeError, match="rollback"):
        async with storage.definition_write() as write:
            row = await TeamDefinition(write).save(
                alice,
                {
                    "team_name": "delivery",
                    "team_display_name": "交付团队",
                    "orchestrator_config": {
                        "agent_name": "leader",
                        "agent_display_name": "主理人",
                    },
                    "members": [
                        {"definition_id": member["id"], "resource_view_type": "private"}
                    ],
                },
            )
            leader_id = row["payload"]["orchestrator"]["definition_id"]
            assert (await write.definitions.get_definition_view(leader_id, "private"))[
                "agent_type"
            ] == "orchestrator"
            raise RuntimeError("rollback")
    with pytest.raises(NotFound):
        await storage.definitions.get_definition_view(leader_id, "private")
    with pytest.raises(NotFound):
        await storage.definitions.get_definition_view(row["id"], "private")


@pytest.mark.asyncio
async def test_team_preserves_atomic_updates_and_owner_checks(storage):
    _, _, alice, bob = await identities(storage)
    agents, teams = AgentManager(storage), TeamDefinition(storage)
    member = await agents.add_definition(alice, expert())
    data = {
        "team_name": "delivery",
        "team_display_name": "Delivery",
        "orchestrator_config": {"agent_name": "leader", "agent_display_name": "Leader"},
        "members": [{"definition_id": member["id"], "resource_view_type": "private"}],
    }
    team = await teams.save(alice, data)
    leader_id = team["payload"]["orchestrator"]["definition_id"]
    data.update(id=team["id"], owner_version=team["owner_version"])
    assert (await teams.save(alice, data))["owner_version"] == 2
    with pytest.raises(Conflict):
        await teams.save(alice, data)
    assert (await storage.definitions.get_definition_view(leader_id, "private"))[
        "owner_version"
    ] == 2
    assert (await teams.list(alice))[0]["id"] == team["id"]
    with pytest.raises(Conflict):
        await teams.save(
            alice,
            {
                **data,
                "id": None,
                "owner_version": 1,
                "orchestrator_config": {
                    "agent_name": "rolled-back",
                    "agent_display_name": "Rolled back",
                },
            },
        )
    agents_after_conflict, _ = await storage.definitions.list_definitions_and_reviews("agent")
    assert {row["agent_name"] for row in agents_after_conflict} == {"leader", "writer"}
    with pytest.raises(Forbidden):
        await teams.save(bob, {**data, "owner_version": 2})
    with pytest.raises(Forbidden):
        await teams.save(alice, {**data, "id": None, "members": data["members"] * 2})
    await teams.delete(alice, team["id"])
    assert (await storage.get_definition(team["id"]))["deleted_at"] is not None
    assert (await storage.get_definition(leader_id))["deleted_at"] is not None
    assert (await storage.get_definition(member["id"]))["deleted_at"] is None


@pytest.mark.asyncio
async def test_definition_views_are_engine_only_reads_not_use_authorization(storage):
    _, admin, alice, bob = await identities(storage)
    agents, shares = AgentManager(storage), ShareService(storage)
    value = expert()
    row = await agents.add_definition(alice, value)
    views = DefinitionStorage(storage.engine)
    assert (await views.get_definition_view(row["id"], "private"))["owner_id"] == "alice"
    with pytest.raises(Forbidden):
        await AgentManager(storage).get_definition_for_use(bob, row["id"], "private")
    with pytest.raises(Forbidden):
        await views.get_definition_view(row["id"], "system")
    with pytest.raises(Forbidden):
        await views.get_definition_view(row["id"], "shared")
    review = await shares.submit(alice, "agent", row["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "approved")
    value.payload["system_prompt"] = "unreviewed"
    await agents.update_definition(alice, (value).id, agent_changes(value))
    shared = await views.get_definition_view(row["id"], "shared", review_id=review["review_id"])
    assert shared["payload"]["system_prompt"] == "旧版"
    assert shared["shared_review_id"] == review["review_id"]
    assert shared["resource_version"] == "1"
    shared["payload"]["system_prompt"] = "mutated copy"
    assert (await views.get_definition_view(row["id"], "shared", version="1"))["payload"][
        "system_prompt"
    ] == "旧版"
    await agents.delete_definition(alice, row["id"])
    with pytest.raises(Forbidden):
        await views.get_definition_view(row["id"], "shared")
    with pytest.raises(NotFound):
        await views.get_definition_view("missing", "private")


@pytest.mark.asyncio
async def test_permissions_and_snapshots_do_not_call_definition_service(
    storage, monkeypatch
):
    _, admin, alice, _ = await identities(storage)
    agents = AgentManager(storage)
    row = await agents.add_definition(admin, expert("sys", "system"))
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

    monkeypatch.setattr(AgentManager, "get_definition", unavailable)
    monkeypatch.setattr(AgentManager, "get_definition_for_use", unavailable)
    snapshot = await storage.definitions.get_definition_view(row["id"], "system")
    assert snapshot["agent_name"] == "writer"
    assert (await permissions.explain(alice, "agent", row["id"], "system")).is_allowed
    await permissions.require_execution(alice, snapshot)


@pytest.mark.asyncio
async def test_definition_storage_reads_catalog_reviews_and_caller_transaction(storage):
    repository_type = entry("egis_agentkit.storage._definition", "DefinitionStorage")
    _, admin, alice, _ = await identities(storage)
    repository = repository_type(storage.engine)
    agents, shares = AgentManager(storage), ShareService(storage)
    value = expert()
    row = await agents.add_definition(alice, value)
    assert (await repository.get_definition(row["id"]))["owner_version"] == 1
    assert await repository.get_approved_review(row["id"]) is None
    async with storage.transaction() as conn:
        assert await repository.get_locked_definition("missing", conn=conn) is None
        assert (await repository.get_definition(row["id"], conn=conn))["id"] == row[
            "id"
        ]
    review = await shares.submit(alice, "agent", row["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "approved")
    assert (await repository.get_approved_review(row["id"]))["review_id"] == review[
        "review_id"
    ]
    assert await repository.get_approved_review(row["id"], resource_type="skill") is None
    rows, states = await repository.list_definitions_and_reviews()
    assert [r["id"] for r in rows] == [row["id"]]
    assert states == {(row["id"], "1"): "approved"}
    assert await repository.list_definitions_and_reviews("team") == ([], {})
    with pytest.raises(NotFound):
        await repository.get_definition("missing")


@pytest.mark.asyncio
async def test_locked_definition_retains_row_lock_and_concurrent_save_conflicts(
    storage,
):
    repository_type = entry("egis_agentkit.storage._definition", "DefinitionStorage")
    _, _, alice, _ = await identities(storage)
    agents = AgentManager(storage)
    row = await agents.add_definition(alice, expert())
    repository = repository_type(storage.engine)
    async with storage.transaction() as first:
        await repository.get_locked_definition(row["id"], conn=first)
        with pytest.raises(DBAPIError) as error:
            async with storage.transaction() as second:
                await second.execute(text("SET LOCAL lock_timeout = '100ms'"))
                await repository.get_locked_definition(row["id"], conn=second)
        assert error.value.orig.sqlstate == "55P03"
    changed = AgentDefinition.model_validate(
        {key: row[key] for key in AgentDefinition.model_fields}
    )
    changed.payload["system_prompt"] = "new"
    results = await asyncio.gather(
        agents.update_definition(alice, (changed).id, agent_changes(changed)), agents.update_definition(alice, (changed).id, agent_changes(changed)), return_exceptions=True
    )
    assert sum(isinstance(result, Conflict) for result in results) == 1
    assert [
        result["owner_version"] for result in results if isinstance(result, dict)
    ] == [2]


@pytest.mark.asyncio
async def test_agent_query_pages_visible_rows_and_filters_definition_type(storage):
    _, admin, alice, bob = await identities(storage)
    experts, query = AgentManager(storage), AgentManager(storage)
    older = await experts.add_definition(alice, expert(name="older"))
    newer = await experts.add_definition(alice, expert(name="newer"))
    await experts.add_definition(bob, expert("bob", name="hidden"))
    personal = await PersonalManager(storage).get_or_add_definition(alice)
    remote = await AgentManager(storage).add_definition(
        admin,
        AgentDefinition(
            owner_id=alice.user_id,
            agent_type="remote",
            agent_name="finance",
            agent_display_name="财务专家",
            payload={
                "remote_config": remote_config().model_dump(mode="json"),
            },
        ),
    )

    page = await query.page_definitions(alice, types=("expert",), limit=1)
    assert [row["id"] for row in page["items"]] == [newer["id"]]
    assert page["next_cursor"]
    final = await query.page_definitions(
        alice, types=("expert",), limit=1, cursor=page["next_cursor"]
    )
    assert [row["id"] for row in final["items"]] == [older["id"]]
    assert final["next_cursor"] is None
    personal_page = await query.page_definitions(alice, types=("personal",))
    assert [row["id"] for row in personal_page["items"]] == [personal["id"]]
    remote_page = await query.page_definitions(alice, types=("remote",))
    assert [row["id"] for row in remote_page["items"]] == [remote["id"]]
    default_page = await query.page_definitions(alice)
    assert [row["id"] for row in default_page["items"]] == [
        remote["id"],
        newer["id"],
        older["id"],
    ]
    with pytest.raises(ValueError):
        await query.page_definitions(alice, types=("personal",), cursor=page["next_cursor"])

    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", remote["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "approved")
    native = AgentDefinition.model_validate(
        {
            **{key: remote[key] for key in AgentDefinition.model_fields},
            "agent_type": "expert",
            "payload": {},
        }
    )
    with pytest.raises(Forbidden):
        await experts.update_definition(alice, (native).id, agent_changes(native))
    from pydantic import ValidationError

    from egis_agentkit.storage.model.agent._update import AgentUpdate

    with pytest.raises(ValidationError):
        AgentUpdate(**agent_changes(native).model_dump(), agent_type="expert")
    assert (await query.page_definitions(bob, types=("expert",)))["items"][0]["id"] != remote["id"]
    shared_remote = (await query.page_definitions(bob, types=("remote",)))["items"]
    assert [row["id"] for row in shared_remote] == [remote["id"]]
    assert shared_remote[0]["resource_view_type"] == "shared"


@pytest.mark.asyncio
async def test_agent_catalog_uses_configured_default_page_and_cap(storage, tmp_path):
    from test_app_http import app_snapshot

    _, _, alice, _ = await identities(storage)
    await AgentManager(storage).add_definition(alice, expert(name="older"))
    latest = await AgentManager(storage).add_definition(alice, expert(name="latest"))
    provider = SimpleNamespace(
        snapshot=app_snapshot(
            tmp_path,
            agent={
                "catalog": {
                    "default_page_size": 1,
                    "max_page_size": 2,
                }
            },
        )
    )
    query = AgentManager(storage, config_provider=provider)
    page = await query.page_definitions(alice)
    assert [row["id"] for row in page["items"]] == [latest["id"]]
    assert page["next_cursor"]
    with pytest.raises(ValueError, match="2"):
        await query.page_definitions(alice, limit=3)


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


def test_definition_contract_requires_each_kind_to_implement_creation():
    from egis_agentkit.core.agent._definition_base import AgentDefinitionBase

    assert AgentDefinitionBase.__abstractmethods__ == {"build_definition"}
    for implementation in (
        PersonalDefinition,
        ExpertDefinition,
        OrchestratorDefinition,
        RemoteDefinition,
    ):
        assert implementation.__bases__ == (AgentDefinitionBase,)
        assert "build_definition" in implementation.__dict__


@pytest.mark.asyncio
async def test_team_manages_orchestrator_identity_and_versions(storage):
    _, _, alice, _ = await identities(storage)
    member = await AgentManager(storage).add_definition(alice, expert())
    teams = TeamDefinition(storage)
    data = {
        "team_name": "delivery",
        "team_display_name": "交付团队",
        "orchestrator_config": {"agent_name": "leader", "agent_display_name": "主理人"},
        "members": [{"definition_id": member["id"], "resource_view_type": "private"}],
    }
    first = await teams.save(alice, data)
    next_version = await teams.save(
        alice,
        {
            **data,
            "id": first["id"],
            "owner_version": first["owner_version"],
            "orchestrator_config": {
                **data["orchestrator_config"],
                "payload": {"system_prompt": "按任务分配专家"},
            },
        },
    )
    first_ref, next_ref = (
        first["payload"]["orchestrator"],
        next_version["payload"]["orchestrator"],
    )
    assert next_ref["definition_id"] == first_ref["definition_id"]
    leader = await storage.agents.get_definition(next_ref["definition_id"])
    assert leader["agent_type"] == "orchestrator"
    assert leader["owner_version"] == 2
    assert leader["payload"]["system_prompt"] == "按任务分配专家"
