"""迁移既有技能发布记录及共享指针，保留源文件定位和稳定授权。"""

from sqlalchemy import inspect, text

from ..model._tables import (
    assets,
    metadata,
    resource_permission,
    skill_detail,
    skill_summary,
)

SKILL_TABLES = (skill_summary, skill_detail)
POINTER_COLUMNS = {"publication_revision", "is_shared_current"}


async def migrate_skills(connection, schema, *, apply=False, access_pending=False):
    """在上层事务内预览或移动 Skill 行，遇到不明确的历史顺序时保留原数据。"""
    names = await connection.run_sync(
        lambda conn: set(inspect(conn).get_table_names(schema=schema))
    )
    quote = connection.dialect.identifier_preparer.quote_identifier
    legacy, summary, detail = (
        f"{quote(schema)}.{quote(table.name)}" for table in (assets, *SKILL_TABLES)
    )
    present = [table for table in SKILL_TABLES if table.name in names]
    if apply:
        lock_names = [
            qualified
            for table, qualified in zip(
                (assets, *SKILL_TABLES), (legacy, summary, detail)
            )
            if table.name in names
        ]
        if lock_names:
            await connection.execute(
                text(f"LOCK TABLE {', '.join(lock_names)} IN SHARE ROW EXCLUSIVE MODE")
            )
    has_legacy_rows = assets.name in names and await connection.scalar(
        text(f"SELECT EXISTS (SELECT 1 FROM {legacy} WHERE resource_type = 'skill')")
    )
    if has_legacy_rows:
        state = await _legacy_state(
            connection,
            schema,
            legacy,
            summary if skill_summary in present else None,
            grant_table=resource_permission.name
            if resource_permission.name in names
            else None,
            access_pending=access_pending,
        )
        if state == "inconsistent":
            if apply:
                raise ValueError(
                    "技能版本指针无法安全迁移，请在部署 SQL 脚本中核对历史版本"
                )
            return state
    if not has_legacy_rows and len(present) == len(SKILL_TABLES):
        return "current"
    if not apply:
        return "pending"

    if len(present) != len(SKILL_TABLES):
        await connection.run_sync(metadata.create_all, tables=SKILL_TABLES)
    if has_legacy_rows:
        await _move_versions(connection, legacy, summary, detail)
    return "upgraded"


async def _columns(connection, schema, name):
    return await connection.run_sync(
        lambda conn: {
            column["name"] for column in inspect(conn).get_columns(name, schema=schema)
        }
    )


async def _legacy_state(
    connection, schema, legacy, summary, *, grant_table, access_pending
):
    conflict = await connection.scalar(
        text(f"""
            SELECT EXISTS (
                SELECT resource_id FROM {legacy} WHERE resource_type = 'skill'
                GROUP BY resource_id HAVING count(DISTINCT (owner_type, owner_id)) > 1
            )
        """)
    )
    if summary is not None and not conflict:
        conflict = await connection.scalar(
            text(f"""
                SELECT EXISTS (
                    SELECT 1 FROM {legacy} AS old JOIN {summary} AS head
                      ON old.resource_id = head.skill_id
                    WHERE old.resource_type = 'skill'
                )
            """)
        )
    if conflict:
        return "inconsistent"
    if access_pending:
        return "pending"

    if grant_table and "resource_version" in await _columns(
        connection, schema, grant_table
    ):
        return "inconsistent"
    if not POINTER_COLUMNS <= await _columns(connection, schema, assets.name):
        return "inconsistent"
    conflict = await connection.scalar(
        text(f"""
            SELECT EXISTS (
                SELECT resource_id FROM {legacy} WHERE resource_type = 'skill'
                GROUP BY resource_id
                HAVING count(DISTINCT publication_revision) <> count(*)
                    OR min(publication_revision) <= 0
                    OR count(*) FILTER (WHERE is_shared_current) > 1
            )
        """)
    )
    return "inconsistent" if conflict else "pending"


async def _move_versions(connection, legacy, summary, detail):
    await connection.execute(
        text(f"""
            WITH versions AS (
                SELECT resource_id, publication_revision, owner_type, owner_id,
                       created_by, created_at,
                       max(publication_revision) OVER skill AS owner_version,
                       max(publication_revision) FILTER (WHERE is_shared_current)
                           OVER skill AS shared_version,
                       max(updated_at) OVER skill AS summary_updated_at,
                       first_value(updated_by) OVER (
                           PARTITION BY resource_id
                           ORDER BY updated_at DESC, publication_revision DESC
                       ) AS summary_updated_by
                FROM {legacy} WHERE resource_type = 'skill'
                WINDOW skill AS (PARTITION BY resource_id)
            )
            INSERT INTO {summary} (
                skill_id, owner_type, owner_id, source_user_id,
                owner_version, shared_version, created_by, updated_by, created_at, updated_at
            )
            SELECT DISTINCT ON (resource_id)
                   resource_id, owner_type, owner_id, created_by,
                   owner_version, shared_version, created_by, summary_updated_by,
                   created_at, summary_updated_at
            FROM versions ORDER BY resource_id, publication_revision
        """)
    )
    await connection.execute(
        text(f"""
            INSERT INTO {detail} (
                skill_id, version, resource_version, checksum_sha256, entrypoint,
                is_enabled, display_name, description, category, avatar, avatar_checksum,
                files, import_job_id, payload, created_by, updated_by, created_at, updated_at
            )
            SELECT resource_id, publication_revision, resource_version, checksum_sha256,
                   entrypoint, is_enabled, typed.display_name, typed.description,
                   typed.category, typed.avatar, typed.avatar_checksum, typed.files,
                   typed.import_job_id,
                   CASE WHEN jsonb_typeof(payload) = 'object' THEN
                       payload - array_remove(ARRAY[
                           CASE WHEN typed.display_name IS NOT NULL THEN 'display_name' END,
                           CASE WHEN typed.description IS NOT NULL THEN 'description' END,
                           CASE WHEN typed.category IS NOT NULL THEN 'category' END,
                           CASE WHEN typed.avatar IS NOT NULL THEN 'avatar' END,
                           CASE WHEN typed.avatar_checksum IS NOT NULL THEN 'avatar_checksum' END,
                           CASE WHEN typed.files IS NOT NULL THEN 'files' END,
                           CASE WHEN typed.import_job_id IS NOT NULL THEN 'import_job_id' END
                       ], NULL)
                   ELSE payload END,
                   created_by, updated_by, created_at, updated_at
            FROM {legacy}
            CROSS JOIN LATERAL (
                SELECT
                    CASE WHEN jsonb_typeof(payload->'display_name') = 'string'
                         THEN payload->>'display_name' END AS display_name,
                    CASE WHEN jsonb_typeof(payload->'description') = 'string'
                         THEN payload->>'description' END AS description,
                    CASE WHEN jsonb_typeof(payload->'category') = 'string'
                         THEN payload->>'category' END AS category,
                    CASE WHEN jsonb_typeof(payload->'avatar') = 'string'
                         THEN payload->>'avatar' END AS avatar,
                    CASE WHEN jsonb_typeof(payload->'avatar_checksum') = 'string'
                              AND length(payload->>'avatar_checksum') <= 64
                         THEN payload->>'avatar_checksum' END AS avatar_checksum,
                    CASE WHEN jsonb_typeof(payload->'files') = 'array'
                         THEN payload->'files' END AS files,
                    CASE WHEN jsonb_typeof(payload->'import_job_id') = 'string'
                              AND length(payload->>'import_job_id') <= 128
                         THEN payload->>'import_job_id' END AS import_job_id
            ) AS typed
            WHERE resource_type = 'skill'
        """)
    )
    await connection.execute(
        text(f"DELETE FROM {legacy} WHERE resource_type = 'skill'")
    )
