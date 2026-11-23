"""审批过程与生效权限使用明确表名，旧库升级保留审批及派生授权关系。"""

import pytest
from sqlalchemy import inspect, text
from test_domain import COMPANY, expert, identities

from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.storage import tables as t
from egis_agentkit.storage._schema import upgrade_schema
from egis_agentkit.storage.migration._table_names import OBJECT_RENAMES


def test_approval_and_permission_table_names():
    assert t.approvals.name == "ak_approve"
    assert t.resource_permission.name == "ak_resource_permission"


@pytest.mark.asyncio
@pytest.mark.parametrize("previous", ["ak_resource_access", "ak_resource_grants"])
async def test_permission_names_upgrade_preserves_approved_access(storage, previous):
    _, admin, alice, bob = await identities(storage)
    saved = await AgentManager(storage).add_definition(alice, expert())
    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", saved["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "通过")
    async with storage.engine.begin() as conn:
        before = {
            name: (await conn.execute(text(f'SELECT * FROM "{name}" ORDER BY 1')))
            .mappings()
            .all()
            for name in ("ak_resource_permission", "ak_approve")
        }
        # 模拟真实旧库的附属对象名，而不仅仅是旧表名。
        reverse_names = {new: old for old, new in OBJECT_RENAMES.items()}
        schema = await conn.scalar(text("SELECT current_schema()"))
        for current, old in (
            ("ak_resource_permission", previous),
            ("ak_approve", "ak_share_reviews"),
        ):
            constraints = (
                (
                    await conn.execute(
                        text(
                            "SELECT conname FROM pg_constraint WHERE conrelid=to_regclass(:name)"
                        ),
                        {"name": current},
                    )
                )
                .scalars()
                .all()
            )
            for name in constraints:
                previous_name = reverse_names.get(
                    name,
                    old + name[len(current) :]
                    if name.startswith(current + "_")
                    else name,
                )
                if previous_name != name:
                    await conn.execute(
                        text(
                            f'ALTER TABLE "{current}" RENAME CONSTRAINT "{name}" TO "{previous_name}"'
                        )
                    )
            indexes = await conn.run_sync(
                lambda sync: inspect(sync).get_indexes(current, schema=schema)
            )
            for index in indexes:
                name = index["name"]
                previous_name = reverse_names.get(
                    name, name.replace("ix_" + current + "_", "ix_" + old + "_")
                )
                if previous_name != name:
                    await conn.execute(
                        text(f'ALTER INDEX "{name}" RENAME TO "{previous_name}"')
                    )
        await conn.execute(
            text(f'ALTER TABLE ak_resource_permission RENAME TO "{previous}"')
        )
        await conn.execute(text("ALTER TABLE ak_approve RENAME TO ak_share_reviews"))
    with pytest.raises(ValueError, match="部署 SQL 脚本"):
        await storage.init_schema()
    preview = await upgrade_schema(storage.engine)
    assert preview.tables[previous] == "pending"
    assert preview.tables["ak_share_reviews"] == "pending"
    assert preview.tables["授权和共享版本"] == "current"
    await upgrade_schema(storage.engine, database=preview.database, apply=True)
    await storage.init_schema()
    async with storage.engine.connect() as conn:
        for name, rows in before.items():
            assert (
                await conn.execute(text(f'SELECT * FROM "{name}" ORDER BY 1'))
            ).mappings().all() == rows
    async with storage.engine.connect() as conn:
        for table in (t.resource_permission, t.approvals):
            names = {
                index["name"]
                for index in await conn.run_sync(
                    lambda sync: inspect(sync).get_indexes(table.name, schema=schema)
                )
            }
            assert {index.name for index in table.indexes} <= names
        foreign_keys = await conn.run_sync(
            lambda sync: inspect(sync).get_foreign_keys(
                "ak_resource_permission", schema=schema
            )
        )
        assert all(
            key["referred_table"] == "ak_resource_permission" for key in foreign_keys
        )
    repeated = await upgrade_schema(
        storage.engine, database=preview.database, apply=True
    )
    assert repeated.tables["ak_resource_permission"] == "current"
    assert repeated.tables["ak_approve"] == "current"
    assert (await AgentManager(storage).get_definition(bob, saved["id"], "shared"))[
        "id"
    ] == saved["id"]


@pytest.mark.asyncio
async def test_two_legacy_permission_tables_are_a_preview_conflict(storage):
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("ALTER TABLE ak_resource_permission RENAME TO ak_resource_access")
        )
        await conn.execute(
            text(
                "CREATE TABLE ak_resource_grants (LIKE ak_resource_access INCLUDING ALL)"
            )
        )
    preview = await upgrade_schema(storage.engine)
    assert "inconsistent" in preview.tables.values()
    with pytest.raises(ValueError, match="冲突|同时存在"):
        await upgrade_schema(storage.engine, database=preview.database, apply=True)


@pytest.mark.asyncio
async def test_mixed_historical_indexes_are_consolidated_without_data_changes(storage):
    from egis_agentkit.storage.migration._table_names import migrate_table_names

    await identities(storage)
    async with storage.engine.begin() as conn:
        schema = await conn.scalar(text("SELECT current_schema()"))
        await conn.execute(
            text("ALTER TABLE ak_resource_permission RENAME TO ak_resource_access")
        )
        for column in ("parent_grant_id", "root_grant_id"):
            await conn.execute(
                text(
                    f"ALTER INDEX ix_ak_resource_permission_{column} RENAME TO ix_ak_resource_access_{column}"
                )
            )
            await conn.execute(
                text(
                    f"CREATE INDEX ix_ak_resource_grants_{column} ON ak_resource_access ({column})"
                )
            )
        assert (await migrate_table_names(conn, schema))[
            "ak_resource_access"
        ] == "pending"
        await migrate_table_names(conn, schema, apply=True)
        names = {
            item["name"]
            for item in await conn.run_sync(
                lambda sync: inspect(sync).get_indexes(
                    "ak_resource_permission", schema=schema
                )
            )
        }
        assert "ix_ak_resource_permission_parent_grant_id" in names
        assert "ix_ak_resource_permission_root_grant_id" in names
        assert not any(
            name.startswith(("ix_ak_resource_access_", "ix_ak_resource_grants_"))
            for name in names
        )
        assert (await migrate_table_names(conn, schema))[
            "ak_resource_permission"
        ] == "current"


@pytest.mark.asyncio
async def test_conflicting_historical_index_definitions_are_not_dropped(storage):
    from egis_agentkit.storage.migration._table_names import migrate_table_names

    async with storage.engine.begin() as conn:
        schema = await conn.scalar(text("SELECT current_schema()"))
        await conn.execute(
            text("ALTER TABLE ak_resource_permission RENAME TO ak_resource_access")
        )
        # 名称相同不代表索引语义相同：不同列必须阻止迁移，不能静默删除。
        await conn.execute(
            text(
                "CREATE INDEX ix_ak_resource_grants_parent_grant_id ON ak_resource_access (root_grant_id)"
            )
        )
        preview = await migrate_table_names(conn, schema)
        assert preview["ak_resource_access"] == "inconsistent"
    with pytest.raises(ValueError, match="索引.*冲突"):
        async with storage.engine.begin() as conn:
            await migrate_table_names(conn, schema, apply=True)
    async with storage.engine.connect() as conn:
        names = {
            item["name"]
            for item in await conn.run_sync(
                lambda sync: inspect(sync).get_indexes(
                    "ak_resource_access", schema=schema
                )
            )
        }
        assert "ix_ak_resource_grants_parent_grant_id" in names
        assert "ix_ak_resource_permission_parent_grant_id" in names
