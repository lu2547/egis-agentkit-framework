"""Query indexes: explicit provisioning and online creation for populated databases."""

import re
from dataclasses import dataclass

from sqlalchemy import text


@dataclass(frozen=True)
class QueryIndex:
    name: str
    table: str
    columns: str
    predicate: str | None = None
    include: tuple[str, ...] = ()

    def ddl(self, connection, schema, *, concurrently=False):
        quote = connection.dialect.identifier_preparer.quote_identifier
        return (
            f"CREATE INDEX {'CONCURRENTLY ' if concurrently else ''}IF NOT EXISTS "
            f"{quote(self.name)} ON {quote(schema)}.{quote(self.table)} ({self.columns})"
            + (
                f" INCLUDE ({', '.join(quote(column) for column in self.include)})"
                if self.include
                else ""
            )
            + (f" WHERE {self.predicate}" if self.predicate else "")
        )


QUERY_INDEXES = (
    QueryIndex(
        "ix_sessions_main_user_updated",
        "sessions",
        "user_id, updated_at DESC",
        "source='user' AND project_id IS NOT NULL",
        ("id",),
    ),
    QueryIndex(
        "ix_ak_project_changes_page",
        "ak_project_file_changes",
        "project_id, change_id DESC",
    ),
    QueryIndex(
        "ix_ak_permission_active_resource_principal",
        "ak_resource_permission",
        "resource_type, resource_id, principal_type, principal_id",
        "revoked_at IS NULL",
    ),
)
RETIRED_QUERY_INDEXES = {
    "ix_sessions_updated_at": ("sessions", "updated_at"),
    "ix_ak_project_file_changes_project_id": ("ak_project_file_changes", "project_id"),
}


async def create_query_indexes(connection, schema):
    """Only called by explicit schema provisioning, never by application startup."""
    for index in QUERY_INDEXES:
        await connection.execute(text(index.ddl(connection, schema)))
    # The partial covering index replaces the unused native global time index;
    # the file page index also covers the old project-only lookup prefix.
    quote = connection.dialect.identifier_preparer.quote_identifier
    for name in RETIRED_QUERY_INDEXES:
        if await _retired_index_exists(connection, schema, name):
            await connection.execute(text(f"DROP INDEX {quote(schema)}.{quote(name)}"))


@dataclass(frozen=True)
class QueryIndexReport:
    database: str
    schema: str
    tables: dict[str, str]


def _normalized(value):
    return re.sub(r'[\s"()]|::text', "", value or "").lower()


async def _retired_index_exists(connection, schema, name):
    table, column = RETIRED_QUERY_INDEXES[name]
    row = (
        (
            await connection.execute(
                text("""
        SELECT tab.relname AS table_name, am.amname AS method, i.indisunique, i.indnkeyatts,
               i.indnatts, pg_get_indexdef(i.indexrelid,1,true) AS first_column,
               pg_get_expr(i.indpred,i.indrelid) AS predicate
        FROM pg_index i JOIN pg_class idx ON idx.oid=i.indexrelid
        JOIN pg_am am ON am.oid=idx.relam
        JOIN pg_namespace ns ON ns.oid=idx.relnamespace
        JOIN pg_class tab ON tab.oid=i.indrelid
        WHERE ns.nspname=:schema AND idx.relname=:name
    """),
                {"schema": schema, "name": name},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return False
    if (
        row["table_name"] != table
        or row["method"] != "btree"
        or row["indisunique"]
        or row["predicate"] is not None
        or row["indnkeyatts"] != 1
        or row["indnatts"] != 1
        or row["first_column"].strip('"') != column
    ):
        raise ValueError(f"旧索引 {name} 的定义与预期不一致，未移除")
    return True


async def _index_state(connection, schema, index):
    row = (
        (
            await connection.execute(
                text("""
        SELECT i.indisvalid, tab.relname AS table_name, am.amname AS method,
               pg_get_indexdef(i.indexrelid) AS definition,
               pg_get_expr(i.indpred, i.indrelid) AS predicate
        FROM pg_index i
        JOIN pg_class idx ON idx.oid=i.indexrelid
        JOIN pg_am am ON am.oid=idx.relam
        JOIN pg_namespace ns ON ns.oid=idx.relnamespace
        JOIN pg_class tab ON tab.oid=i.indrelid
        WHERE ns.nspname=:schema AND idx.relname=:name
    """),
                {"schema": schema, "name": index.name},
            )
        )
        .mappings()
        .first()
    )
    if row is None:
        return False
    if not row["indisvalid"]:
        raise ValueError(f"索引 {index.name} 尚未有效，先确认并发建索引是否仍在运行")
    if (
        row["table_name"] != index.table
        or row["method"] != "btree"
        or re.sub(r'[\s"]', "", "(" + index.columns + ")").lower()
        not in re.sub(r'[\s"]', "", row["definition"]).lower()
        or (
            index.include
            and _normalized("INCLUDE (" + ", ".join(index.include) + ")")
            not in _normalized(row["definition"])
        )
        or _normalized(row["predicate"]) != _normalized(index.predicate)
    ):
        raise ValueError(f"现有索引 {index.name} 的定义与预期不一致")
    return True


async def optimize_query_indexes(engine, *, database=None, apply=False):
    """Preview by default; existing large tables use CONCURRENTLY outside a transaction."""
    async with engine.connect() as connection:
        connection = await connection.execution_options(isolation_level="AUTOCOMMIT")
        actual, schema = (
            await connection.execute(
                text("SELECT current_database(), current_schema()")
            )
        ).one()
        if not schema:
            raise ValueError("连接未选择有效的 schema")
        if (apply or database is not None) and database != actual:
            raise ValueError(f"目标数据库不匹配，当前连接为 {actual}")
        states = {}
        locked = False
        old_timeout = None
        try:
            if apply:
                locked = await connection.scalar(
                    text("SELECT pg_try_advisory_lock(727140203)")
                )
                if not locked:
                    raise ValueError("另一个查询索引维护任务正在运行")
                old_timeout = await connection.scalar(text("SHOW lock_timeout"))
                await connection.execute(text("SET lock_timeout='5s'"))
            for index in QUERY_INDEXES:
                if await _index_state(connection, schema, index):
                    states[index.name] = "present"
                elif not apply:
                    states[index.name] = "create concurrently"
                else:
                    await connection.execute(
                        text(index.ddl(connection, schema, concurrently=True))
                    )
                    if not await _index_state(connection, schema, index):
                        raise ValueError(f"索引 {index.name} 未成功创建")
                    states[index.name] = "created concurrently"
            for name in RETIRED_QUERY_INDEXES:
                if not await _retired_index_exists(connection, schema, name):
                    continue
                if not apply:
                    states[name] = "drop redundant concurrently"
                else:
                    quote = connection.dialect.identifier_preparer.quote_identifier
                    await connection.execute(
                        text(
                            f"DROP INDEX CONCURRENTLY IF EXISTS {quote(schema)}.{quote(name)}"
                        )
                    )
                    states[name] = "dropped redundant concurrently"
            return QueryIndexReport(actual, schema, states)
        finally:
            if old_timeout is not None:
                await connection.execute(
                    text("SELECT set_config('lock_timeout', :value, false)"),
                    {"value": old_timeout},
                )
            if locked:
                await connection.execute(text("SELECT pg_advisory_unlock(727140203)"))
