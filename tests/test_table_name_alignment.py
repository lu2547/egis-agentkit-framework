"""个人助理资源和反馈表使用统一命名，显式升级保留数据及关联对象。"""

import pytest
from sqlalchemy import inspect, text
from test_domain import identities

from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage import tables as t
from egis_agentkit.storage._schema import upgrade_schema

RENAMES = {
    "ak_personal_agent_resources": "ak_agent_personal_resources",
    "ak_session_reply_feedback": "ak_feedback",
}
INDEXES = {
    "ix_ak_personal_resources_user_type": "ix_ak_agent_personal_resources_user_type",
    "uq_ak_reply_feedback_user_reply": "uq_ak_feedback_user_reply",
    "ix_ak_reply_feedback_created": "ix_ak_feedback_created",
}


def test_models_use_agent_personal_resources_and_feedback_names():
    assert t.agent_personal_resources.name == "ak_agent_personal_resources"
    assert t.feedback.name == "ak_feedback"


@pytest.mark.asyncio
async def test_table_rename_preserves_rows_keys_indexes_and_storage_queries(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.personal.add_resource(alice, "skill", "report")
    await resources.personal.update_resource_enabled(alice, "skill", "report", False)
    values = dict(
        user_id="alice",
        username="Alice",
        project_id="project",
        project_name="Project",
        session_id="session",
        session_name="Session",
        reply_id="reply",
        agent_id="agent",
        definition_id="definition",
        agent_display_name="Expert",
        comment="请核对来源",
        question_text="问题",
        reply_text="回答",
    )
    feedback = await storage.session_feedback.save_reply_feedback(values)
    async with storage.engine.begin() as conn:
        before = {
            new: (await conn.execute(text(f'SELECT * FROM "{new}"'))).mappings().all()
            for new in RENAMES.values()
        }
        for old, new in RENAMES.items():
            await conn.execute(text(f'ALTER TABLE "{new}" RENAME TO "{old}"'))
            await conn.execute(
                text(
                    f'ALTER TABLE "{old}" RENAME CONSTRAINT "{new}_pkey" TO "{old}_pkey"'
                )
            )
        for old, new in INDEXES.items():
            await conn.execute(text(f'ALTER INDEX "{new}" RENAME TO "{old}"'))
    with pytest.raises(ValueError, match="部署 SQL 脚本"):
        await storage.init_schema()
    preview = await upgrade_schema(storage.engine)
    assert all(preview.tables[old] == "pending" for old in RENAMES)
    async with storage.engine.connect() as conn:
        for old in RENAMES:
            assert (
                await conn.scalar(text("SELECT to_regclass(:name)"), {"name": old})
                is not None
            )
    applied = await upgrade_schema(
        storage.engine, database=preview.database, apply=True
    )
    assert all(applied.tables[old] == "renamed" for old in RENAMES)
    await storage.init_schema()
    async with storage.engine.connect() as conn:
        after = {
            new: (await conn.execute(text(f'SELECT * FROM "{new}"'))).mappings().all()
            for new in RENAMES.values()
        }
        assert after == before
        for old, new in RENAMES.items():
            assert (
                await conn.scalar(text("SELECT to_regclass(:name)"), {"name": old})
                is None
            )
            schema = await conn.scalar(text("SELECT current_schema()"))
            info = await conn.run_sync(
                lambda sync: inspect(sync).get_pk_constraint(new, schema=schema)
            )
            assert info["name"] == new + "_pkey"
        names = {
            item["name"]
            for table in RENAMES.values()
            for item in await conn.run_sync(
                lambda sync: inspect(sync).get_indexes(table, schema=schema)
            )
        }
        assert set(INDEXES.values()) <= names
        assert not set(INDEXES) & names
    selected = await storage.personal_resources.list_resources("alice", "skill")
    assert selected["report"]["is_enabled"] is False
    saved = await storage.session_feedback.save_reply_feedback(
        {**values, "comment": "补充说明"}
    )
    assert saved["id"] == feedback["id"]
    page = await storage.session_feedback.page_reply_feedback(limit=10, cursor=None)
    assert page["items"][0]["comment"] == "补充说明"
    repeated = await upgrade_schema(
        storage.engine, database=preview.database, apply=True
    )
    assert all(repeated.tables[new] == "current" for new in RENAMES.values())


@pytest.mark.asyncio
@pytest.mark.parametrize("old,new", RENAMES.items())
async def test_duplicate_old_and_new_tables_stop_upgrade_without_renaming(
    storage, old, new
):
    async with storage.engine.begin() as conn:
        await conn.execute(text(f'CREATE TABLE "{old}" (LIKE "{new}" INCLUDING ALL)'))
    preview = await upgrade_schema(storage.engine)
    assert preview.tables[old] == "inconsistent"
    with pytest.raises(ValueError, match="冲突"):
        await upgrade_schema(storage.engine, database=preview.database, apply=True)
    async with storage.engine.connect() as conn:
        for name in (old, new):
            assert (
                await conn.scalar(text("SELECT to_regclass(:name)"), {"name": name})
                is not None
            )


@pytest.mark.asyncio
async def test_old_personal_table_reports_owner_conflict_during_preview(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.personal.add_resource(alice, "skill", "report")
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("UPDATE ak_agent_personal_resources SET user_id='wrong-owner'")
        )
        await conn.execute(
            text(
                "ALTER TABLE ak_agent_personal_resources RENAME TO ak_personal_agent_resources"
            )
        )
    preview = await upgrade_schema(storage.engine)
    assert preview.tables["个人资源开关"] == "inconsistent"
    with pytest.raises(ValueError, match="冲突"):
        await upgrade_schema(storage.engine, database=preview.database, apply=True)
    async with storage.engine.connect() as conn:
        assert (
            await conn.scalar(text("SELECT to_regclass('ak_personal_agent_resources')"))
            is not None
        )
        assert (
            await conn.scalar(text("SELECT to_regclass('ak_agent_personal_resources')"))
            is None
        )


@pytest.mark.asyncio
async def test_targeted_table_rename_leaves_unselected_table_untouched(storage):
    from egis_agentkit.storage.migration._table_names import migrate_table_names

    async with storage.engine.begin() as conn:
        schema = await conn.scalar(text("SELECT current_schema()"))
        for old, new in RENAMES.items():
            await conn.execute(text(f'ALTER TABLE "{new}" RENAME TO "{old}"'))
        states = await migrate_table_names(
            conn, schema, apply=True, targets={"ak_feedback"}
        )
        assert states == {"ak_session_reply_feedback": "renamed"}
        assert (
            await conn.scalar(text("SELECT to_regclass('ak_personal_agent_resources')"))
            is not None
        )
        assert (
            await conn.scalar(text("SELECT to_regclass('ak_agent_personal_resources')"))
            is None
        )
        assert await conn.scalar(text("SELECT to_regclass('ak_feedback')")) is not None
