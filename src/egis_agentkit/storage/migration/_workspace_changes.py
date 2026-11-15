"""显式增加文件变更的会话、回复归属与最终汇总索引；不猜测已有记录的来源。"""

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncConnection

from egis_agentkit.storage.model import _tables as tables


async def migrate_workspace_changes(
    connection: AsyncConnection, schema: str, *, apply: bool = False
) -> str:
    """预览或补齐变更表归属字段，人工操作与已有未归属记录保持 NULL。"""
    from ._project import project_tables

    name = (await project_tables(connection, schema))[tables.file_changes.name]
    names = await connection.run_sync(
        lambda conn: inspect(conn).get_table_names(schema=schema)
    )
    if name not in names:
        return "absent"
    columns = {
        c["name"]
        for c in await connection.run_sync(
            lambda conn: inspect(conn).get_columns(name, schema=schema)
        )
    }
    indexes = {
        i["name"]
        for i in await connection.run_sync(
            lambda conn: inspect(conn).get_indexes(name, schema=schema)
        )
    }
    missing = {"session_id", "reply_id"} - columns
    expected_indexes = {index.name: index for index in tables.file_changes.indexes}
    missing_indexes = expected_indexes.keys() - indexes
    if not missing and not missing_indexes:
        return "current"
    if not apply:
        return "pending"
    quote = connection.dialect.identifier_preparer.quote_identifier
    qualified = f"{quote(schema)}.{quote(name)}"
    for column in sorted(missing):
        await connection.execute(
            text(f"ALTER TABLE {qualified} ADD COLUMN {quote(column)} varchar(128)")
        )
    for index_name in sorted(missing_indexes):
        index_columns = ", ".join(
            quote(column.name) for column in expected_indexes[index_name].columns
        )
        await connection.execute(
            text(f"CREATE INDEX {quote(index_name)} ON {qualified} ({index_columns})")
        )
    return "upgraded"
