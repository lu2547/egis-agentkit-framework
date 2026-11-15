"""将 Agent 定义和资源权限迁移到已确认的表名及版本语义。"""

from sqlalchemy import inspect, text

RENAMES = {
    "ak_agent_definitions": "ak_agent_definition_summary",
    "ak_agent_definition_versions": "ak_agent_definition_detail",
    "ak_resource_grants": "ak_resource_access",
}


async def agent_tables(connection, schema):
    """定位升级前后的物理表，拒绝同时存在两份业务表。"""
    names = set(
        await connection.run_sync(
            lambda conn: inspect(conn).get_table_names(schema=schema)
        )
    )
    for old, current in RENAMES.items():
        if old in names and current in names:
            raise ValueError(f"同时存在 {old} 与 {current}，请先核对数据")
    return {
        current: old if old in names else current for old, current in RENAMES.items()
    }


async def migrate_agent_storage(connection, schema, *, apply=False):
    """预览只检查结构；执行在调用方事务中重命名并转换删除状态。"""
    from ..model._tables import agent_definition_summary, resource_access

    actual = await agent_tables(connection, schema)
    quote = connection.dialect.identifier_preparer.quote_identifier

    def qualified(name):
        return f"{quote(schema)}.{quote(name)}"

    async def columns(name):
        return {
            item["name"]
            for item in await connection.run_sync(
                lambda conn: inspect(conn).get_columns(name, schema=schema)
            )
        }

    summary_columns = await columns(actual["ak_agent_definition_summary"])
    detail_columns = await columns(actual["ak_agent_definition_detail"])
    pending = any(old != new for new, old in actual.items()) or (
        "owner_version" not in summary_columns
        or "deleted_at" not in summary_columns
        or "is_enabled" in summary_columns
        or "version" not in detail_columns
    )
    if not apply:
        return "pending" if pending else "current"
    for current, old in actual.items():
        if old != current:
            await connection.execute(
                text(f"ALTER TABLE {qualified(old)} RENAME TO {quote(current)}")
            )
    summary = qualified("ak_agent_definition_summary")
    detail = qualified("ak_agent_definition_detail")
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
                f"UPDATE {summary} SET deleted_at = coalesce(updated_at, created_at, now()) WHERE NOT is_enabled AND deleted_at IS NULL"
            )
        )
        await connection.execute(text(f"ALTER TABLE {summary} DROP COLUMN is_enabled"))
    # 建立当前查询所需索引；已有索引保留，重复执行不重建。
    for table in (agent_definition_summary, resource_access):
        for index in table.indexes:
            await connection.run_sync(
                lambda conn, index=index: index.create(conn, checkfirst=True)
            )
    return "upgraded" if pending else "current"
