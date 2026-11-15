"""真实 PG 中升级 Agent 表名和版本字段，保留定义、共享与授权。"""

from sqlalchemy import text
from test_domain import COMPANY, agent_changes, expert, identities

from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.storage._schema import upgrade_schema


async def test_agent_upgrade_preserves_versions_access_and_deleted_state(storage):
    _, admin, alice, bob = await identities(storage)
    manager = AgentManager(storage)
    value = expert()
    original = await manager.add_definition(alice, value)
    review = await ShareService(storage).submit(
        alice, "agent", value.id, target=COMPANY
    )
    await ShareService(storage).decide(admin, review["review_id"], True, "approved")
    value.payload["system_prompt"] = "第二版"
    await manager.update_definition(alice, (value).id, agent_changes(value))
    deleted = await manager.add_definition(alice, expert(name="deleted"))
    await manager.delete_definition(alice, deleted["id"])
    async with storage.engine.begin() as connection:
        database = await connection.scalar(text("SELECT current_database()"))
        await connection.execute(
            text(
                "ALTER TABLE ak_agent_definition_summary RENAME COLUMN owner_version TO config_version"
            )
        )
        await connection.execute(
            text(
                "ALTER TABLE ak_agent_definition_summary ADD COLUMN is_enabled boolean NOT NULL DEFAULT true"
            )
        )
        await connection.execute(
            text(
                "UPDATE ak_agent_definition_summary SET is_enabled = (deleted_at IS NULL)"
            )
        )
        await connection.execute(
            text("ALTER TABLE ak_agent_definition_summary DROP COLUMN deleted_at")
        )
        await connection.execute(
            text(
                "ALTER TABLE ak_agent_definition_detail RENAME COLUMN version TO config_version"
            )
        )
        for current, previous in (
            ("ak_agent_definition_summary", "ak_agent_definitions"),
            ("ak_agent_definition_detail", "ak_agent_definition_versions"),
            ("ak_resource_access", "ak_resource_grants"),
        ):
            await connection.execute(
                text(f"ALTER TABLE {current} RENAME TO {previous}")
            )
    preview = await upgrade_schema(storage.engine)
    assert preview.tables["Agent 存储结构"] == "pending"
    result = await upgrade_schema(storage.engine, database=database, apply=True)
    assert result.tables["Agent 存储结构"] == "upgraded"
    assert (await manager.get_definition(alice, original["id"], "private"))["owner_version"] == 2
    assert (await manager.get_definition(bob, original["id"], "shared"))["payload"][
        "system_prompt"
    ] == "旧版"
    assert (await storage.agents.get_definition(deleted["id"]))[
        "deleted_at"
    ] is not None
    assert (await upgrade_schema(storage.engine, database=database, apply=True)).tables[
        "Agent 存储结构"
    ] == "current"
