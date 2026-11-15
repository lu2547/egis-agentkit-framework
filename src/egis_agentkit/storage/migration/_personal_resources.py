"""显式升级个人资源开关，迁移后不再依赖按版本保存的原生安装记录。"""

from datetime import timezone
from typing import Literal

from sqlalchemy import inspect, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection

from egis_agentkit.storage import tables as t

from ._table_names import RENAMES


async def _upgrade_user_column(
    connection: AsyncConnection,
    schema: str,
    summary: str,
    *,
    apply: bool = False,
    physical_table: str | None = None,
) -> Literal["pending", "current", "upgraded", "inconsistent"]:
    """补齐资源选择的用户归属与查询索引，错误归属禁止自动覆盖。"""
    # 1. 核对已有列、索引，以及个人助理与用户归属是否一致。
    table = t.agent_personal_resources
    physical_table = physical_table or table.name
    columns = await connection.run_sync(
        lambda conn: inspect(conn).get_columns(physical_table, schema=schema)
    )
    user_column = next(
        (column for column in columns if column["name"] == "user_id"), None
    )
    indexes = await connection.run_sync(
        lambda conn: inspect(conn).get_indexes(physical_table, schema=schema)
    )
    missing_indexes = {index.name for index in table.indexes} - {
        index["name"] for index in indexes
    }
    needs_column = user_column is None or user_column["nullable"]
    quote = connection.dialect.identifier_preparer.quote_identifier
    resource_table = f"{quote(schema)}.{quote(physical_table)}"
    summary_table = f"{quote(schema)}.{quote(summary)}"
    wrong_user = (
        " OR (r.user_id IS NOT NULL AND r.user_id <> a.owner_id)"
        if user_column is not None
        else ""
    )
    invalid = await connection.scalar(
        text(f"""
        SELECT EXISTS (
            SELECT 1 FROM {resource_table} r
            LEFT JOIN {summary_table} a ON a.id = r.agent_id
            WHERE a.id IS NULL OR a.agent_type <> 'personal'
               OR a.owner_type <> 'user' {wrong_user}
        )
    """)
    )
    if invalid:
        if apply:
            raise ValueError("个人资源记录与个人助理归属不一致，不能回填 user_id")
        return "inconsistent"
    # 2. 预览只返回状态，显式执行时才补齐用户归属和索引。
    pending = needs_column or bool(missing_indexes)
    if not apply:
        return "pending" if pending else "current"
    if needs_column:
        await connection.execute(
            text(
                f"ALTER TABLE {resource_table} ADD COLUMN IF NOT EXISTS user_id varchar(128)"
            )
        )
        await connection.execute(
            text(f"""
            UPDATE {resource_table} r SET user_id = a.owner_id
            FROM {summary_table} a WHERE r.agent_id = a.id AND r.user_id IS NULL
        """)
        )
        await connection.execute(
            text(f"ALTER TABLE {resource_table} ALTER COLUMN user_id SET NOT NULL")
        )
    for index in table.indexes:
        await connection.run_sync(
            lambda conn, index=index: index.create(conn, checkfirst=True)
        )
    return "upgraded" if pending else "current"


async def migrate_personal_resources(
    connection: AsyncConnection, schema: str, *, apply: bool = False
) -> Literal["pending", "current", "upgraded", "inconsistent"]:
    """预览并迁移平台本地 Hub 的安装选择，其他 Hub 的原生记录不处理。"""
    names = set(
        await connection.run_sync(
            lambda conn: inspect(conn).get_table_names(schema=schema)
        )
    )
    quote = connection.dialect.identifier_preparer.quote_identifier

    def qualified(name: str) -> str:
        return f"{quote(schema)}.{quote(name)}"

    legacy = []
    summary = (
        "ak_agent_definition_summary"
        if "ak_agent_definition_summary" in names
        else "ak_agent_definitions"
    )
    if summary in names:
        for kind, native in (("skill", "skills"), ("mcp", "mcps")):
            if native not in names:
                continue
            rows = (
                (
                    await connection.execute(
                        text(f"""
                SELECT n.id, n.user_id, n.created_at, n.updated_at, n.payload, a.id AS agent_id
                FROM {qualified(native)} n
                JOIN {qualified(summary)} a ON a.owner_id=n.user_id
                    AND a.owner_type='user' AND a.agent_type='personal'
                WHERE n.payload->>'hub_id'='egis-local' AND n.payload->>'card_id' IS NOT NULL
                ORDER BY n.updated_at, n.id
            """)
                    )
                )
                .mappings()
                .all()
            )
            legacy.extend((kind, native, dict(row)) for row in rows)
    target = t.agent_personal_resources.name
    actual = (
        target
        if target in names
        else next(
            (old for old, new in RENAMES.items() if new == target and old in names),
            None,
        )
    )
    # 预览时也读取尚未改名的物理表，不能跳过已有记录的用户归属检查。
    user_state = (
        await _upgrade_user_column(connection, schema, summary, physical_table=actual)
        if actual is not None
        else "pending"
    )
    if user_state == "inconsistent":
        if apply:
            raise ValueError("个人资源记录与个人助理归属不一致")
        return user_state
    pending = user_state == "pending" or bool(legacy)
    if not apply:
        return "pending" if pending else "current"
    if actual is not None and actual != target:
        raise ValueError("个人资源表名需要先通过 upgrade-db 改名")
    await connection.run_sync(
        lambda conn: t.agent_personal_resources.create(conn, checkfirst=True)
    )
    await _upgrade_user_column(connection, schema, summary, apply=True)
    selected = {}
    for kind, native, row in legacy:
        selected[(row["agent_id"], kind, row["payload"]["card_id"])] = row
    for (agent_id, kind, identifier), row in selected.items():
        statement = (
            insert(t.agent_personal_resources)
            .values(
                user_id=row["user_id"],
                agent_id=agent_id,
                resource_type=kind,
                resource_id=identifier,
                is_enabled=bool(row["payload"].get("enabled", True)),
                created_by=row["user_id"],
                updated_by=row["user_id"],
                created_at=row["created_at"].replace(tzinfo=timezone.utc),
                updated_at=row["updated_at"].replace(tzinfo=timezone.utc),
            )
            .on_conflict_do_nothing()
        )
        await connection.execute(statement)
    # 只清理已迁移的本地 Hub 行；运行缓存仍由资源版本自己的机制管理。
    for _, native, row in legacy:
        await connection.execute(
            text(f"DELETE FROM {qualified(native)} WHERE id=:id"), {"id": row["id"]}
        )
    return "upgraded" if pending else "current"
