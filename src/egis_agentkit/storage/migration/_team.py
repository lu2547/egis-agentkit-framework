"""显式升级 Team 汇总和明细，保留版本、主理人及共享引用。"""

from sqlalchemy import inspect, text

RENAMES = {
    "ak_team_definitions": "ak_team_definition_summary",
    "ak_team_definition_versions": "ak_team_definition_detail",
}


async def team_tables(connection, schema):
    """定位升级前后的物理表；两份同类表并存时停止，避免丢失数据。"""
    names = set(
        await connection.run_sync(
            lambda sync: inspect(sync).get_table_names(schema=schema)
        )
    )
    for old, current in RENAMES.items():
        if old in names and current in names:
            raise ValueError(f"同时存在 {old} 与 {current}，请先核对数据")
    return {
        current: old if old in names else current for old, current in RENAMES.items()
    }


async def _rename_constraints(connection, schema, table, previous):
    """表改名后同步约束名称，主键对应的索引由 PG 一起改名。"""

    def names(sync):
        inspector = inspect(sync)
        return [
            inspector.get_pk_constraint(table, schema=schema),
            *inspector.get_foreign_keys(table, schema=schema),
            *inspector.get_check_constraints(table, schema=schema),
            *inspector.get_unique_constraints(table, schema=schema),
        ]

    quote = connection.dialect.identifier_preparer.quote_identifier
    qualified = f"{quote(schema)}.{quote(table)}"
    for constraint in await connection.run_sync(names):
        name = constraint.get("name")
        if name and name.startswith(previous + "_"):
            current = table + name[len(previous) :]
            await connection.execute(
                text(
                    f"ALTER TABLE {qualified} RENAME CONSTRAINT {quote(name)} TO {quote(current)}"
                )
            )


async def migrate_team_storage(connection, schema, *, apply=False):
    """预览或原子升级 Team 的表名、版本字段及逻辑删除状态。"""
    from ..model._tables import team_definition_summary

    actual = await team_tables(connection, schema)
    quote = connection.dialect.identifier_preparer.quote_identifier

    def qualified(name):
        return f"{quote(schema)}.{quote(name)}"

    async def columns(name):
        return {
            column["name"]
            for column in await connection.run_sync(
                lambda sync: inspect(sync).get_columns(name, schema=schema)
            )
        }

    summary_columns = await columns(actual["ak_team_definition_summary"])
    detail_columns = await columns(actual["ak_team_definition_detail"])
    pending = any(old != current for current, old in actual.items()) or (
        "owner_version" not in summary_columns
        or "deleted_at" not in summary_columns
        or "is_enabled" in summary_columns
        or "version" not in detail_columns
    )
    if not apply:
        return "pending" if pending else "current"

    # 1. 原地改名，外键和会话中的定义 ID 保持不变。
    for current, old in actual.items():
        if old != current:
            await connection.execute(
                text(f"ALTER TABLE {qualified(old)} RENAME TO {quote(current)}")
            )
            await _rename_constraints(connection, schema, current, old)
    summary = qualified("ak_team_definition_summary")
    detail = qualified("ak_team_definition_detail")
    # 2. 转换版本和生命周期表达，所有历史明细保留。
    if "owner_version" not in summary_columns:
        await connection.execute(
            text(f"ALTER TABLE {summary} RENAME COLUMN config_version TO owner_version")
        )
    if "version" not in detail_columns:
        await connection.execute(
            text(f"ALTER TABLE {detail} RENAME COLUMN config_version TO version")
        )
    if "deleted_at" not in summary_columns:
        await connection.execute(
            text(f"ALTER TABLE {summary} ADD COLUMN deleted_at timestamptz")
        )
    if "is_enabled" in summary_columns:
        await connection.execute(
            text(
                f"UPDATE {summary} SET deleted_at=coalesce(updated_at,created_at,now()) WHERE NOT is_enabled AND deleted_at IS NULL"
            )
        )
        await connection.execute(text(f"ALTER TABLE {summary} DROP COLUMN is_enabled"))
    # 3. 补齐活动目录索引，重复执行不创建重复索引。
    for index in team_definition_summary.indexes:
        await connection.run_sync(
            lambda sync, index=index: index.create(sync, checkfirst=True)
        )
    return "upgraded" if pending else "current"
