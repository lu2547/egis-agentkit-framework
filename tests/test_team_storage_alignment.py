"""Team 与 Agent 使用相同的汇总、不可变明细、版本和逻辑删除语义。"""

import copy

import pytest
from sqlalchemy import text
from test_domain import COMPANY, expert, identities

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.storage._schema import upgrade_schema


async def create_team(storage):
    _, admin, owner, recipient = await identities(storage)
    expert_record = await AgentManager(storage).add_definition(owner, expert())
    data = {
        "team_name": "delivery",
        "team_display_name": "交付专家团",
        "payload": {"description": "首版"},
        "orchestrator_config": {
            "agent_name": "leader",
            "agent_display_name": "主理人",
            "payload": {"system_prompt": "协调交付"},
        },
        "members": [
            {"definition_id": expert_record["id"], "resource_view_type": "private"}
        ],
    }
    service = TeamDefinition(storage)
    record = await service.save(owner, data)
    return admin, owner, recipient, service, data, record


async def test_team_versions_sharing_and_deletion_match_agent_semantics(storage):
    admin, owner, recipient, teams, data, first = await create_team(storage)
    assert first["owner_version"] == first["version"] == 1
    assert "config_version" not in first and "is_enabled" not in first
    review = await ShareService(storage).submit(
        owner, "team", first["id"], target=COMPANY
    )
    await ShareService(storage).decide(admin, review["review_id"], True, "approved")
    changed = copy.deepcopy(data)
    changed.update(id=first["id"], owner_version=1)
    changed["payload"]["description"] = "第二版"
    second = await teams.save(owner, changed)
    assert second["owner_version"] == second["version"] == 2
    assert (
        second["payload"]["orchestrator"]["definition_id"]
        == first["payload"]["orchestrator"]["definition_id"]
    )
    shared = await teams.read(recipient, first["id"], "shared")
    assert shared["version"] == 1 and shared["payload"]["description"] == "首版"
    with pytest.raises(Conflict):
        await teams.save(owner, changed)
    await teams.delete(owner, first["id"])
    deleted = await storage.definitions.get_definition(first["id"])
    assert deleted["deleted_at"] is not None
    assert deleted["owner_version"] == 2
    assert (
        await storage.agents.get_definition(
            first["payload"]["orchestrator"]["definition_id"]
        )
    )["deleted_at"] is not None
    assert await teams.list(owner) == []
    with pytest.raises(Forbidden):
        await teams.read(recipient, first["id"], "shared")
    async with storage.engine.connect() as connection:
        versions = (
            await connection.execute(
                text(
                    "SELECT version, payload->>'description' FROM ak_team_definition_detail WHERE definition_id=:id ORDER BY version"
                ),
                {"id": first["id"]},
            )
        ).all()
        assert [tuple(row) for row in versions] == [(1, "首版"), (2, "第二版")]


async def test_team_upgrade_preserves_versions_sharing_and_deleted_state(storage):
    admin, owner, recipient, teams, data, first = await create_team(storage)
    assert first.get("owner_version") == 1
    review = await ShareService(storage).submit(
        owner, "team", first["id"], target=COMPANY
    )
    await ShareService(storage).decide(admin, review["review_id"], True, "approved")
    await teams.save(
        owner,
        {
            **data,
            "id": first["id"],
            "owner_version": 1,
            "payload": {"description": "第二版"},
        },
    )
    removed_data = copy.deepcopy(data)
    removed_data["team_name"] = "removed"
    removed_data["orchestrator_config"]["agent_name"] = "removed_leader"
    removed = await teams.save(owner, removed_data)
    await teams.delete(owner, removed["id"])
    async with storage.engine.begin() as connection:
        database = await connection.scalar(text("SELECT current_database()"))
        for statement in (
            "ALTER TABLE ak_team_definition_summary RENAME COLUMN owner_version TO config_version",
            "ALTER TABLE ak_team_definition_summary ADD COLUMN is_enabled boolean NOT NULL DEFAULT true",
            "UPDATE ak_team_definition_summary SET is_enabled=(deleted_at IS NULL)",
            "ALTER TABLE ak_team_definition_summary DROP COLUMN deleted_at",
            "ALTER TABLE ak_team_definition_detail RENAME COLUMN version TO config_version",
            "ALTER TABLE ak_team_definition_summary RENAME TO ak_team_definitions",
            "ALTER TABLE ak_team_definition_detail RENAME TO ak_team_definition_versions",
        ):
            await connection.execute(text(statement))
    preview = await upgrade_schema(storage.engine)
    assert preview.tables["Team 存储结构"] == "pending"
    upgraded = await upgrade_schema(storage.engine, database=database, apply=True)
    assert upgraded.tables["Team 存储结构"] == "upgraded"
    assert (await teams.read(owner, first["id"]))["owner_version"] == 2
    assert (await teams.read(recipient, first["id"], "shared"))["version"] == 1
    assert (await storage.definitions.get_definition(removed["id"]))[
        "deleted_at"
    ] is not None
    assert (await upgrade_schema(storage.engine, database=database, apply=True)).tables[
        "Team 存储结构"
    ] == "current"
