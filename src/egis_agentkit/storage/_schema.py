"""管理工作台使用的 PostgreSQL 表。

核心能力
- 用原生 AgentScope 模型建表，只选择工作台依赖的表。
- 预览并执行旧库升级，清理已核对的冗余关系和空退役表。
"""

from dataclasses import dataclass

from agentscope.app.storage._sql._tables import _Base as NativeBase
from sqlalchemy import inspect, text

from ._message_bus import rename_bus_tables
from .migration._agent import RENAMES as AGENT_RENAMES
from .migration._agent import agent_tables, migrate_agent_storage
from .migration._personal_resources import migrate_personal_resources
from .migration._project import (
    TABLE_SOURCES,
    migrate_project_tables,
    migrate_session_workspaces,
)
from .migration._sequences import repair_sequences
from .migration._skill_archives import LEGACY_SKILL_ARCHIVES, migrate_skill_archives
from .migration._skill_imports import migrate_skill_import_files
from .migration._table_names import RENAMES as TABLE_RENAMES
from .migration._table_names import migrate_table_names
from .migration._team import RENAMES as TEAM_RENAMES
from .migration._team import migrate_team_storage, team_tables
from .migration._workspace_changes import migrate_workspace_changes
from .model._tables import metadata

SCHEMA_LOCK = 727140201
NATIVE_TABLES = (
    "agents",
    "credentials",
    "messages",
    "schedules",
    "sessions",
    "teams",
)
OBSOLETE_TABLES = (
    "mcps",
    "skills",
    "ak_definitions",
    "ak_tool_installations",
    "channels",
    "knowledge_documents",
    "knowledge_bases",
    "sop_runs",
    "sops",
)
LEGACY_PROJECT_SESSIONS = "ak_project_sessions"
PROJECT_BINDING = "'{state,middle_context,egis_agentkit,project_id}'"
SESSION_BINDING = "'{state,middle_context,egis_agentkit}'"
AGENT_TYPE_CONSTRAINT = "ak_agent_definitions_agent_type_check"


@dataclass(frozen=True)
class SchemaReport:
    """记录已核对的目标库、schema 和每张候选表的状态。"""

    database: str
    schema: str
    tables: dict[str, str]


async def init_schema(engine):
    """在同一事务内初始化原生运行表和 AgentKit 业务表。"""
    native = NativeBase.metadata
    required = [native.tables[name] for name in NATIVE_TABLES]
    async with engine.begin() as connection:
        await connection.execute(
            text("SELECT pg_advisory_xact_lock(:key)"), {"key": SCHEMA_LOCK}
        )
        schema = await connection.scalar(text("SELECT current_schema()"))
        if schema is None:
            raise ValueError("连接未选择有效的 schema")
        existing = await _existing_tables(connection, schema)
        if LEGACY_SKILL_ARCHIVES in existing:
            raise ValueError("技能归档需要先通过 upgrade-db 迁移到 NAS")
        if "ak_skill_imports" in existing:
            columns = await _table_columns(connection, schema, "ak_skill_imports")
            if "archive" in columns:
                raise ValueError("技能导入文件需要先通过 upgrade-db 迁移到 NAS")
        old_project_tables = {
            name for sources in TABLE_SOURCES.values() for name in sources
        }
        if old_project_tables & existing:
            raise ValueError("项目文件表名需要先执行 upgrade-db 预览和升级")
        if (
            "sessions" in existing
            and await migrate_session_workspaces(connection, schema) == "pending"
        ):
            raise ValueError("会话工作区身份需要先执行 upgrade-db 预览和升级")
        if set(AGENT_RENAMES) & existing:
            raise ValueError("旧版 Agent 数据库请先通过 upgrade-db 预览和升级")
        if set(TEAM_RENAMES) & existing:
            raise ValueError("旧版 Team 数据库请先通过 upgrade-db 预览和升级")
        if (
            "ak_team_definition_summary" in existing
            and await migrate_team_storage(connection, schema) == "pending"
        ):
            raise ValueError("Team 存储结构需要先执行 upgrade-db")
        if (
            set(TABLE_RENAMES) & existing
            or LEGACY_PROJECT_SESSIONS in existing
            and (
                "sessions" not in existing
                or not await _generated_project_id(connection, schema)
            )
        ):
            raise ValueError("旧版数据库请先通过 upgrade-db 预览和升级")
        if await rename_bus_tables(connection, apply=False):
            raise ValueError("旧版消息总线请先通过 upgrade-db 预览和升级")
        if "ak_agent_definitions" in existing:
            columns = await _table_columns(connection, schema, "ak_agent_definitions")
            if "shared_version" not in columns:
                raise ValueError("旧版定义表请先通过 upgrade-db 预览和升级")
        if "ak_project_file_changes" in existing:
            if await migrate_workspace_changes(connection, schema) == "pending":
                raise ValueError(
                    "工作区变更表缺少会话归属字段或索引，请先执行 upgrade-db"
                )
        await connection.run_sync(native.create_all, tables=required)
        await connection.run_sync(metadata.create_all)
        await _create_session_project_index(connection, schema)


async def _existing_tables(connection, schema):
    return set(
        await connection.run_sync(
            lambda conn: inspect(conn).get_table_names(schema=schema)
        )
    )


async def _table_columns(connection, schema, table):
    return {
        item["name"]: item
        for item in await connection.run_sync(
            lambda conn: inspect(conn).get_columns(table, schema=schema)
        )
    }


def _qualified(connection, schema, name):
    quote = connection.dialect.identifier_preparer.quote_identifier
    return f"{quote(schema)}.{quote(name)}"


async def _generated_project_id(connection, schema):
    cols = await _table_columns(connection, schema, "sessions")
    return bool(cols.get("project_id", {}).get("computed"))


async def _create_session_project_index(connection, schema):
    sessions = _qualified(connection, schema, "sessions")
    columns = await _table_columns(connection, schema, "sessions")
    if "project_id" in columns and not columns["project_id"].get("computed"):
        raise ValueError("sessions.project_id 已存在，但并非从会话绑定生成")
    await connection.execute(
        text(f"""
            ALTER TABLE {sessions} ADD COLUMN IF NOT EXISTS project_id text
            GENERATED ALWAYS AS (payload #>> {PROJECT_BINDING}) STORED
        """)
    )
    await connection.execute(
        text(f"""
            CREATE INDEX IF NOT EXISTS ix_sessions_project_user
            ON {sessions} (project_id, user_id) WHERE project_id IS NOT NULL
        """)
    )


async def upgrade_schema(
    engine,
    *,
    database=None,
    apply=False,
    skill_archives=None,
    skill_archive_limits=None,
):
    """预览旧库改造。核对目标库后原子执行并保留旧项目关联供清理。"""
    from ._access_schema import check_access_upgrade, upgrade_access_tables

    async with engine.begin() as connection:
        actual_database, schema = await _target(connection, database, apply)
        existing = await _existing_tables(connection, schema)
        layout = await agent_tables(connection, schema)
        team_layout = await team_tables(connection, schema)
        core = {
            layout["ak_agent_definition_summary"],
            layout["ak_agent_definition_detail"],
            team_layout["ak_team_definition_detail"],
            team_layout["ak_team_definition_summary"],
            "ak_projects",
            "sessions",
        }
        if not core.issubset(existing):
            raise ValueError("升级仅适用于已初始化版本表的独立 AgentKit schema")
        legacy = LEGACY_PROJECT_SESSIONS in existing
        if apply:
            locked = {
                layout["ak_agent_definition_summary"],
                layout["ak_agent_definition_detail"],
                team_layout["ak_team_definition_summary"],
                team_layout["ak_team_definition_detail"],
                "sessions",
                "ak_projects",
            }
            if legacy:
                locked.add(LEGACY_PROJECT_SESSIONS)
            tables = ", ".join(
                _qualified(connection, schema, name) for name in sorted(locked)
            )
            await connection.execute(
                text(f"LOCK TABLE {tables} IN SHARE ROW EXCLUSIVE MODE")
            )

        states = await migrate_table_names(connection, schema)
        if "inconsistent" in states.values():
            if apply:
                raise ValueError("升级数据冲突：表或附属对象无法安全合并")
            return SchemaReport(actual_database, schema, states)
        states.update(await migrate_project_tables(connection, schema))
        states["Session Workspace 身份"] = await migrate_session_workspaces(
            connection, schema
        )
        states.update(
            {
                name: "pending"
                for name in await rename_bus_tables(connection, apply=False)
            }
        )
        states["Agent 存储结构"] = await migrate_agent_storage(connection, schema)
        states["Team 存储结构"] = await migrate_team_storage(connection, schema)
        states["个人资源开关"] = await migrate_personal_resources(connection, schema)
        states["工作区变更归属"] = await migrate_workspace_changes(connection, schema)
        states["Agent 类型"] = await _agent_upgrade_state(connection, schema)
        states["Session 绑定"] = await _session_upgrade_state(connection, schema)
        states["授权和共享版本"] = await check_access_upgrade(connection, schema)
        states["技能文件存储"] = await migrate_skill_archives(
            connection,
            schema,
            archive_store=skill_archives,
            limits=skill_archive_limits,
        )
        states["技能导入文件"] = await migrate_skill_import_files(connection, schema)
        states[LEGACY_PROJECT_SESSIONS] = (
            await _check_legacy_project_links(
                connection,
                schema,
                _qualified(connection, schema, LEGACY_PROJECT_SESSIONS),
                existing,
                apply=False,
                check_payload=True,
            )
            if legacy
            else "absent"
        )
        if apply:
            if states["授权和共享版本"] == "inconsistent":
                await upgrade_access_tables(connection, schema)
            inconsistent = [
                name for name, state in states.items() if state == "inconsistent"
            ]
            if inconsistent:
                raise ValueError(f"升级数据冲突，事务已回滚：{', '.join(inconsistent)}")
            states.update(await migrate_table_names(connection, schema, apply=True))
            for name in await rename_bus_tables(connection):
                states[name] = "renamed"
            if states["Agent 类型"] == "pending":
                await _upgrade_agent_types(connection, schema)
                states["Agent 类型"] = "upgraded"
            if states["Session 绑定"] == "pending":
                await _upgrade_session_bindings(connection, schema)
                states["Session 绑定"] = "upgraded"
            if states["授权和共享版本"] == "pending":
                await upgrade_access_tables(connection, schema)
                states["授权和共享版本"] = "upgraded"
            states["Agent 存储结构"] = await migrate_agent_storage(
                connection, schema, apply=True
            )
            states["Team 存储结构"] = await migrate_team_storage(
                connection, schema, apply=True
            )
            states["个人资源开关"] = await migrate_personal_resources(
                connection, schema, apply=True
            )
        if apply:
            states.update(await migrate_project_tables(connection, schema, apply=True))
            states["Session Workspace 身份"] = await migrate_session_workspaces(
                connection, schema, apply=True
            )
            states["工作区变更归属"] = await migrate_workspace_changes(
                connection, schema, apply=True
            )
        if apply:
            states["技能文件存储"] = await migrate_skill_archives(
                connection,
                schema,
                archive_store=skill_archives,
                limits=skill_archive_limits,
                apply=True,
            )
            states["技能导入文件"] = await migrate_skill_import_files(
                connection, schema, archive_store=skill_archives, apply=True
            )
        states.update(
            {
                f"自增序列:{name}": state
                for name, state in (
                    await repair_sequences(connection, schema, apply=apply)
                ).items()
            }
        )
        return SchemaReport(actual_database, schema, states)


async def _target(connection, database, apply):
    actual_database, schema = (
        await connection.execute(text("SELECT current_database(), current_schema()"))
    ).one()
    if schema is None:
        raise ValueError("连接未选择有效的 schema")
    if (apply or database is not None) and database != actual_database:
        raise ValueError(f"目标数据库不匹配，当前连接为 {actual_database}")
    if apply:
        await connection.execute(text("SET LOCAL lock_timeout = '5s'"))
        await connection.execute(
            text("SELECT pg_advisory_xact_lock(:key)"), {"key": SCHEMA_LOCK}
        )
    return actual_database, schema


async def _agent_upgrade_state(connection, schema):
    layout = await agent_tables(connection, schema)
    definitions = _qualified(connection, schema, layout["ak_agent_definition_summary"])
    versions = _qualified(connection, schema, layout["ak_agent_definition_detail"])
    mixed = await connection.scalar(
        text(f"""
        SELECT EXISTS (
            SELECT 1 FROM {definitions} AS d JOIN {versions} AS v ON v.definition_id = d.id
            GROUP BY d.id, d.agent_type
            HAVING bool_or(v.payload->>'remote_config' IS NULL)
                   AND (d.agent_type = 'remote'
                        OR bool_or(v.payload->>'remote_config' IS NOT NULL)
                        OR bool_or(v.payload->>'execution_type' = 'remote'))
                OR bool_or(v.payload->>'remote_config' IS NOT NULL)
                   AND d.agent_type NOT IN ('expert', 'remote')
        )
    """)
    )
    if mixed:
        return "inconsistent"
    checks = await connection.run_sync(
        lambda conn: inspect(conn).get_check_constraints(
            layout["ak_agent_definition_summary"], schema=schema
        )
    )
    accepts_remote = any(
        "agent_type" in item["sqltext"] and "'remote'" in item["sqltext"]
        for item in checks
    )
    needs_rewrite = await connection.scalar(
        text(f"""
        SELECT EXISTS (
            SELECT 1 FROM {versions} AS v JOIN {definitions} AS d ON d.id = v.definition_id
            WHERE v.payload ? 'execution_type'
               OR (v.payload->>'remote_config' IS NOT NULL AND d.agent_type = 'expert')
        )
    """)
    )
    return "pending" if not accepts_remote or needs_rewrite else "current"


async def _upgrade_agent_types(connection, schema):
    layout = await agent_tables(connection, schema)
    definitions = _qualified(connection, schema, layout["ak_agent_definition_summary"])
    versions = _qualified(connection, schema, layout["ak_agent_definition_detail"])
    checks = await connection.run_sync(
        lambda conn: inspect(conn).get_check_constraints(
            layout["ak_agent_definition_summary"], schema=schema
        )
    )
    quote = connection.dialect.identifier_preparer.quote_identifier
    for check in checks:
        if "agent_type" in check["sqltext"]:
            await connection.execute(
                text(
                    f"ALTER TABLE {definitions} DROP CONSTRAINT {quote(check['name'])}"
                )
            )
    await connection.execute(
        text(f"""
        UPDATE {definitions} AS d SET agent_type = 'remote'
        WHERE d.agent_type = 'expert' AND EXISTS (
            SELECT 1 FROM {versions} AS v WHERE v.definition_id = d.id
              AND v.payload->>'remote_config' IS NOT NULL
        )
    """)
    )
    await connection.execute(
        text(f"""
        UPDATE {versions} SET payload = payload - 'execution_type'
        WHERE payload ? 'execution_type'
    """)
    )
    await connection.execute(
        text(f"""
        ALTER TABLE {definitions} ADD CONSTRAINT {AGENT_TYPE_CONSTRAINT}
        CHECK (agent_type IN ('personal', 'expert', 'remote', 'orchestrator'))
    """)
    )


async def _session_upgrade_state(connection, schema):
    sessions = _qualified(connection, schema, "sessions")
    columns = await _table_columns(connection, schema, "sessions")
    project_id = columns.get("project_id")
    if project_id is not None and not project_id.get("computed"):
        return "inconsistent"
    binding = f"(payload::jsonb #> {SESSION_BINDING})"
    invalid_team_binding = await connection.scalar(
        text(f"""
        SELECT EXISTS (
            SELECT 1 FROM {sessions}
            WHERE ({binding} ? 'agent_node_key' AND {binding}->>'definition_id' IS NULL)
               OR ({binding}->>'root_resource_type' = 'team'
                   AND {binding}->>'agent_definition_id' IS NULL
                   AND coalesce({binding}->>'agent_node_key', '') !~ '^agent:[^:]+:[^:]+$')
        )
    """)
    )
    if invalid_team_binding:
        return "inconsistent"
    requires_rewrite = await connection.scalar(
        text(f"""
        SELECT EXISTS (
            SELECT 1 FROM {sessions} WHERE {binding} ? 'agent_node_key'
               OR ({binding}->>'definition_id' IS NOT NULL AND {binding}->>'agent_definition_id' IS NULL)
        )
    """)
    )
    return "pending" if project_id is None or requires_rewrite else "current"


async def _upgrade_session_bindings(connection, schema):
    sessions = _qualified(connection, schema, "sessions")
    binding = f"(payload::jsonb #> {SESSION_BINDING})"
    await connection.execute(
        text(f"""
        UPDATE {sessions} SET payload = jsonb_set(
            payload::jsonb, {SESSION_BINDING}, ({binding} - 'agent_node_key')
            || jsonb_build_object('agent_definition_id', coalesce(
                {binding}->>'agent_definition_id',
                CASE WHEN {binding}->>'root_resource_type' = 'team'
                     THEN split_part({binding}->>'agent_node_key', ':', 3)
                     ELSE {binding}->>'definition_id' END
            ))
        )::json
        WHERE {binding}->>'definition_id' IS NOT NULL
          AND ({binding}->>'agent_definition_id' IS NULL OR {binding} ? 'agent_node_key')
    """)
    )
    await _create_session_project_index(connection, schema)


async def cleanup_schema(engine, *, database=None, apply=False):
    """核对旧项目关联及退役表，应用时锁定数据并保留依赖检查。"""
    async with engine.begin() as connection:
        actual_database, schema = await _target(connection, database, apply)
        existing = await _existing_tables(connection, schema)
        if apply and not {
            "ak_agent_definition_detail",
            "ak_team_definition_detail",
        }.issubset(existing):
            raise ValueError("清理仅适用于已初始化版本表的独立 AgentKit schema")
        candidates = (*OBSOLETE_TABLES, LEGACY_PROJECT_SESSIONS)
        names = [name for name in candidates if name in existing]
        qualified = {name: _qualified(connection, schema, name) for name in names}

        # 锁定后检查，避免校验和删除之间有新数据写入。
        if apply and qualified:
            await connection.execute(
                text(
                    f"LOCK TABLE {', '.join(qualified.values())} IN ACCESS EXCLUSIVE MODE"
                )
            )
        states = dict.fromkeys(candidates, "absent")
        for name, table in qualified.items():
            if name == LEGACY_PROJECT_SESSIONS:
                states[name] = await _check_legacy_project_links(
                    connection, schema, table, existing, apply=apply
                )
                continue
            has_rows = await connection.scalar(
                text(f"SELECT EXISTS (SELECT 1 FROM {table})")
            )
            states[name] = "contains-data" if has_rows else "empty"
        if apply and qualified:
            if (
                LEGACY_PROJECT_SESSIONS in names
                and states[LEGACY_PROJECT_SESSIONS] != "verified"
            ):
                raise ValueError(f"{LEGACY_PROJECT_SESSIONS} 核对未通过，清理已回滚")
            populated = [name for name in names if states[name] == "contains-data"]
            if populated:
                raise ValueError(f"候选表含有数据，清理已回滚：{', '.join(populated)}")
            await connection.execute(
                text(f"DROP TABLE {', '.join(qualified.values())} RESTRICT")
            )
            states.update(dict.fromkeys(names, "dropped"))
        return SchemaReport(actual_database, schema, states)


async def _check_legacy_project_links(
    connection,
    schema,
    table,
    existing,
    *,
    apply,
    check_payload=False,
):
    """验证旧关联均可由归属同一用户的原生会话和项目恢复。"""
    if not {"sessions", "ak_projects"}.issubset(existing):
        return "requires-upgrade"
    if not check_payload and not await _generated_project_id(connection, schema):
        return "requires-upgrade"
    sessions, projects = (
        _qualified(connection, schema, name) for name in ("sessions", "ak_projects")
    )
    if apply:
        await connection.execute(
            text(f"LOCK TABLE {sessions}, {projects} IN SHARE MODE")
        )
    project_id = (
        f"session.payload #>> {PROJECT_BINDING}"
        if check_payload
        else "session.project_id"
    )
    inconsistent = await connection.scalar(
        text(f"""
            SELECT EXISTS (
                SELECT 1 FROM {table} AS legacy
                LEFT JOIN {sessions} AS session ON session.id = legacy.session_id
                LEFT JOIN {projects} AS project ON project.id = legacy.project_id
                WHERE ({project_id}) IS DISTINCT FROM legacy.project_id
                   OR project.id IS NULL
                   OR session.user_id IS DISTINCT FROM project.owner_id
            )
        """)
    )
    return "inconsistent" if inconsistent else "verified"
