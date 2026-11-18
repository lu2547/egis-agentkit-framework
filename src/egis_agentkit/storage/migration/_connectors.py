"""停用待管理员核对的历史 MCP，保留存量配置及原有主键。"""

from sqlalchemy import inspect, text

from ..model._resource import MCP_MANAGED_FLAG, MCP_STORAGE_VERSION

CONNECTOR_ASSET_TABLE = "ak_resource_assets"
AUDIT_ACTOR = "mcp-catalog-upgrade"


async def migrate_connectors(connection, schema, *, apply=False):
    """预览或停用旧配置，避免旧的系统地址在升级后被误当成最新版。"""
    exists = await connection.run_sync(
        lambda conn: inspect(conn).has_table(CONNECTOR_ASSET_TABLE, schema=schema)
    )
    if not exists:
        return "absent"

    quote = connection.dialect.identifier_preparer.quote_identifier
    table = f"{quote(schema)}.{quote(CONNECTOR_ASSET_TABLE)}"
    # 旧配置也可能恰好占用固定主键，只有管理员按新流程保存后才标记为受管。
    criteria = """
        asset.resource_type = 'mcp'
        AND asset.is_enabled
        AND (
            asset.owner_type <> 'system' OR asset.owner_id <> 'system'
            OR asset.resource_version <> :version
            OR asset.payload -> CAST(:managed_key AS text) IS DISTINCT FROM CAST('true' AS jsonb)
        )
    """
    params = {"version": MCP_STORAGE_VERSION, "managed_key": MCP_MANAGED_FLAG}
    if not apply:
        pending = await connection.scalar(
            text(f"SELECT EXISTS (SELECT 1 FROM {table} AS asset WHERE {criteria})"),
            params,
        )
        return "pending" if pending else "current"

    result = await connection.execute(
        text(f"""
            UPDATE {table} AS asset
            SET is_enabled = false, updated_by = :actor, updated_at = CURRENT_TIMESTAMP
            WHERE {criteria}
        """),
        {**params, "actor": AUDIT_ACTOR},
    )
    return "upgraded" if result.rowcount else "current"
