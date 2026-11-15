"""显式升级时修复导入数据后落后的 PostgreSQL 自增序列。

核心能力
- 只检查目标 schema 中由表字段拥有的正向序列。
- 预览不消耗序列；执行时锁表，重新核对最大主键。
- 仅向前调整，不修改业务记录，不回退已领先的序列。
"""

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection


async def repair_sequences(
    connection: AsyncConnection, schema: str, *, apply: bool = False
) -> dict[str, str]:
    """返回每个序列字段的状态；调用方负责事务及目标数据库校验。"""
    # 1. 使用 PostgreSQL 的归属依赖定位字段，兼容 serial 和 identity。
    rows = (
        (
            await connection.execute(
                text("""
                SELECT n.nspname AS table_schema, t.relname AS table_name,
                       a.attname AS column_name, sn.nspname AS sequence_schema,
                       s.relname AS sequence_name, q.seqincrement
                FROM pg_class s
                JOIN pg_namespace sn ON sn.oid = s.relnamespace
                JOIN pg_sequence q ON q.seqrelid = s.oid
                JOIN pg_depend d ON d.classid = 'pg_class'::regclass
                    AND d.objid = s.oid AND d.refclassid = 'pg_class'::regclass
                    AND d.deptype IN ('a', 'i')
                JOIN pg_class t ON t.oid = d.refobjid
                JOIN pg_namespace n ON n.oid = t.relnamespace
                JOIN pg_attribute a ON a.attrelid = t.oid AND a.attnum = d.refobjsubid
                WHERE n.nspname = :schema AND s.relkind = 'S' AND q.seqincrement > 0
                ORDER BY t.relname, a.attname
            """),
                {"schema": schema},
            )
        )
        .mappings()
        .all()
    )
    quote = connection.dialect.identifier_preparer.quote_identifier
    states = {}
    for row in rows:
        table = f"{quote(row['table_schema'])}.{quote(row['table_name'])}"
        sequence = f"{quote(row['sequence_schema'])}.{quote(row['sequence_name'])}"
        column = quote(row["column_name"])
        # 2. 执行时阻止并发插入；锁后再读取，避免用旧的最大值覆盖。
        if apply:
            await connection.execute(
                text(f"LOCK TABLE {table} IN SHARE ROW EXCLUSIVE MODE")
            )
        maximum = await connection.scalar(text(f"SELECT max({column}) FROM {table}"))
        last, called = (
            await connection.execute(
                text(f"SELECT last_value, is_called FROM {sequence}")
            )
        ).one()
        next_value = last + row["seqincrement"] if called else last
        pending = maximum is not None and maximum >= next_value
        key = f"{row['table_name']}.{row['column_name']}"
        states[key] = "pending" if pending else "current"
        # 3. 只把落后序列推进至已存在主键之后，空表和领先序列保持原样。
        if pending and apply:
            await connection.execute(
                text("SELECT setval(CAST(:sequence AS regclass), :maximum, true)"),
                {"sequence": sequence, "maximum": maximum},
            )
            states[key] = "repaired"
    return states
