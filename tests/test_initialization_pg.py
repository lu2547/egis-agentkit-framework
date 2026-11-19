"""在独立 schema 执行首次 SQL，核对结构和失败回滚后的数据保留。"""

from pathlib import Path
from uuid import uuid4

import asyncpg

from egis_agentkit.storage._message_bus import PostgresPrimitives

PG_SQL = Path(__file__).resolve().parents[1] / "scripts/init/pg.sql"
CLUSTER_TABLES = {
    "agents", "credentials", "messages", "schedules", "sessions", "teams",
    "ak_users", "ak_groups", "ak_agent_definition_summary",
    "ak_agent_definition_detail", "ak_agent_personal_resources", "ak_agent_invite_candidates",
    "ak_agent_remote_runs", "ak_team_definition_summary",
    "ak_team_definition_detail", "ak_resource_assets", "ak_resource_permission",
    "ak_approve", "ak_skill_imports", "ak_skill_summary", "ak_skill_detail",
    "ak_virtual_users", "ak_mcp_credentials", "ak_projects",
    "ak_project_file_changes", "ak_project_file_fingerprints",
    "ak_project_reply_artifacts", "ak_feedback", "ak_bus_keys", "ak_bus_entries",
    "ak_bus_registry", "ak_bus_leases", "ak_bus_broadcast_payloads",
}


async def schema_shape(connection, schema):
    """Compare column rules, constraints and indexes, excluding schema identity."""
    rows = await connection.fetch(
        """
        SELECT 'column' AS kind, c.relname AS table_name, a.attname AS name,
               format_type(a.atttypid, a.atttypmod) || '|' || a.attnotnull::text
               || '|' || a.attgenerated::text || '|'
               || coalesce(pg_get_expr(d.adbin, d.adrelid), '') AS definition
        FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        JOIN pg_attribute a ON a.attrelid = c.oid
        LEFT JOIN pg_attrdef d ON d.adrelid = c.oid AND d.adnum = a.attnum
        WHERE n.nspname = $1 AND c.relkind = 'r'
          AND a.attnum > 0 AND NOT a.attisdropped
        UNION ALL
        SELECT 'constraint', c.relname, p.conname,
               pg_get_constraintdef(p.oid)
        FROM pg_constraint p JOIN pg_class c ON c.oid = p.conrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
        UNION ALL
        SELECT 'index', t.relname, i.relname, pg_get_indexdef(i.oid)
        FROM pg_index x JOIN pg_class i ON i.oid = x.indexrelid
        JOIN pg_class t ON t.oid = x.indrelid
        JOIN pg_namespace n ON n.oid = t.relnamespace
        WHERE n.nspname = $1
        UNION ALL
        SELECT 'sequence', c.relname, c.relname,
               format_type(s.seqtypid, NULL) || '|' || s.seqstart || '|'
               || s.seqincrement || '|' || s.seqmin || '|' || s.seqmax || '|'
               || s.seqcache || '|' || s.seqcycle
        FROM pg_sequence s JOIN pg_class c ON c.oid = s.seqrelid
        JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE n.nspname = $1
        """,
        schema,
    )
    return sorted(
        (row["kind"], row["table_name"], row["name"],
         row["definition"].replace(f"{schema}.", ""))
        for row in rows
    )


async def test_pg_sql_initializes_cluster_schema_and_rejects_rerun(storage):
    assert PG_SQL.is_file(), "缺少首次建表脚本 scripts/init/pg.sql"
    script = PG_SQL.read_text()
    # 1. 测试夹具通过当前模型建表，作为独立参照；SQL 在另一空 schema 执行。
    await PostgresPrimitives(storage.engine).init_schema()
    schema = "ak_sql_test_" + uuid4().hex
    async with storage.engine.connect() as connection:
        raw = await connection.get_raw_connection()
        pg = raw.driver_connection
        original = await pg.fetchval("SELECT current_schema()")
        await pg.execute(f'CREATE SCHEMA "{schema}"')
        try:
            await pg.execute(f'SET search_path TO "{schema}"')
            await pg.execute(script)
            assert not pg.is_in_transaction(), "首次 SQL 必须提交完整事务"
            tables = await pg.fetch(
                "SELECT tablename FROM pg_tables WHERE schemaname = $1", schema
            )
            assert {row["tablename"] for row in tables} == CLUSTER_TABLES
            assert await schema_shape(pg, schema) == await schema_shape(pg, original)
            await pg.execute(
                """
                INSERT INTO sessions
                    (id, user_id, agent_id, source, created_at, updated_at, payload)
                VALUES ('first-session', 'first-user', 'first-agent', 'user',
                        now(), now(),
                        '{"state":{"middle_context":{"egis_agentkit":
                            {"project_id":"first-project"}}}}')
                """
            )
            assert await pg.fetchval(
                "SELECT project_id FROM sessions WHERE id = 'first-session'"
            ) == "first-project"
            # 2. 重复首次安装必须失败，回滚后原有会话和项目归属仍在。
            try:
                await pg.execute(script)
            except asyncpg.DuplicateTableError:
                assert pg.is_in_transaction(), "失败的建表操作必须仍在显式事务中"
                await pg.execute("ROLLBACK")
            else:
                raise AssertionError("首次 SQL 不应覆盖已使用的 schema")
            assert await pg.fetchval("SELECT count(*) FROM sessions") == 1
            assert await pg.fetchval(
                "SELECT project_id FROM sessions WHERE id = 'first-session'"
            ) == "first-project"
        finally:
            if pg.is_in_transaction():
                await pg.execute("ROLLBACK")
            await pg.execute(f'SET search_path TO "{original}"')
            await pg.execute(f'DROP SCHEMA "{schema}" CASCADE')
