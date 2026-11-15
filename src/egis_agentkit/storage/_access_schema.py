"""将旧开发库的版本授权迁移为稳定授权与唯一共享版本。"""

from sqlalchemy import inspect, text

from ._access import GRANT_KEY

ACCESS_TABLES = {
    "grants": "ak_resource_access",
    "reviews": "ak_share_reviews",
    "assets": "ak_resource_assets",
    "agent": "ak_agent_definition_summary",
    "agent_versions": "ak_agent_definition_detail",
    "team": "ak_team_definition_summary",
    "team_versions": "ak_team_definition_detail",
}
CURRENT_COLUMNS = {
    "reviews": {"review_type"},
    "assets": {"is_shared_current", "publication_revision"},
    "agent": {"shared_version"},
    "team": {"shared_version"},
}
DIRECT_GRANTS = "revoked_at IS NULL AND grant_source_type IN ('share', 'manual')"


async def _qualified_tables(connection, schema):
    from .migration._agent import agent_tables
    from .migration._team import team_tables

    actual = await agent_tables(connection, schema)
    actual.update(await team_tables(connection, schema))
    quote = connection.dialect.identifier_preparer.quote_identifier
    return {
        key: f"{quote(schema)}.{quote(actual.get(name, name))}"
        for key, name in ACCESS_TABLES.items()
    }


async def _access_state(connection, schema):
    from .migration._agent import agent_tables
    from .migration._team import team_tables

    actual = await agent_tables(connection, schema)
    actual.update(await team_tables(connection, schema))

    def columns(sync):
        inspector = inspect(sync)
        names = set(inspector.get_table_names(schema=schema))
        return {
            key: {
                column["name"]
                for column in inspector.get_columns(
                    actual.get(name, name), schema=schema
                )
            }
            for key, name in ACCESS_TABLES.items()
            if actual.get(name, name) in names
        }

    existing = await connection.run_sync(columns)
    if not existing:
        return "absent"
    if existing.keys() != ACCESS_TABLES.keys():
        return "inconsistent"
    if "resource_version" in existing["grants"]:
        return "pending"
    if all(required <= existing[key] for key, required in CURRENT_COLUMNS.items()):
        return "current"
    return "inconsistent"


async def _legacy_conflict(connection, table):
    grants = table["grants"]
    conflicting = (
        await connection.execute(
            text(f"""
            SELECT resource_type, resource_id
            FROM {grants}
            WHERE {DIRECT_GRANTS}
            GROUP BY resource_type, resource_id
            HAVING count(DISTINCT resource_version) > 1
            ORDER BY resource_type, resource_id
            LIMIT 1
        """)
        )
    ).first()
    if conflicting:
        return f"{conflicting.resource_type}/{conflicting.resource_id} 存在多个有效授权版本"

    key = ", ".join(column for column in GRANT_KEY if column != "origin_key")
    duplicate = (
        await connection.execute(
            text(f"""
            SELECT resource_type, resource_id
            FROM {grants}
            WHERE {DIRECT_GRANTS}
            GROUP BY {key}
            HAVING count(*) > 1
            ORDER BY resource_type, resource_id
            LIMIT 1
        """)
        )
    ).first()
    if duplicate:
        return f"{duplicate.resource_type}/{duplicate.resource_id} 的直接授权来源重复"

    for kind in ("agent", "team", "skill", "mcp"):
        if kind in ("agent", "team"):
            current_definition = f"ak_{kind}_definition_summary" in table[kind]
            enabled = (
                "head.deleted_at IS NULL" if current_definition else "head.is_enabled"
            )
            version = "version" if current_definition else "config_version"
            owner = "owner_version" if current_definition else "config_version"
            valid = f"""
                SELECT 1 FROM {table[kind]} AS head
                JOIN {table[f"{kind}_versions"]} AS edition
                  ON edition.definition_id = head.id
                WHERE head.id = grant_row.resource_id
                  AND {enabled}
                  AND edition.{version}::text = grant_row.resource_version
                  AND (grant_row.resource_view_type <> 'system'
                       OR edition.{version} = head.{owner})
            """
        else:
            valid = f"""
                SELECT 1 FROM {table["assets"]} AS edition
                WHERE edition.resource_type = grant_row.resource_type
                  AND edition.resource_id = grant_row.resource_id
                  AND edition.resource_version = grant_row.resource_version
                  AND edition.is_enabled
                  AND (grant_row.resource_view_type <> 'system' OR NOT EXISTS (
                      SELECT 1 FROM {table["assets"]} AS newer
                      WHERE newer.resource_type = edition.resource_type
                        AND newer.resource_id = edition.resource_id
                        AND (newer.created_at, newer.resource_version)
                            > (edition.created_at, edition.resource_version)
                  ))
            """
        invalid = await connection.scalar(
            text(f"""
                SELECT resource_id FROM {grants} AS grant_row
                WHERE {DIRECT_GRANTS} AND resource_type = :kind
                  AND NOT EXISTS ({valid})
                LIMIT 1
            """),
            {"kind": kind},
        )
        if invalid is not None:
            return f"{kind}/{invalid} 的有效授权版本缺失、已停用或需要重新核对系统版本"
    return None


async def check_access_upgrade(connection, schema):
    """预览稳定授权升级，检查是否存在无法自动决议的版本。"""
    state = await _access_state(connection, schema)
    if state == "pending" and await _legacy_conflict(
        connection, await _qualified_tables(connection, schema)
    ):
        return "inconsistent"
    return state


async def upgrade_access_tables(connection, schema):
    """在调用方的升级事务内保留根授权，并切换资源共享指针。"""
    state = await _access_state(connection, schema)
    if state == "inconsistent":
        raise ValueError("授权表结构不完整，无法自动升级")
    if state != "pending":
        return state
    table = await _qualified_tables(connection, schema)
    await connection.execute(
        text(f"LOCK TABLE {', '.join(table.values())} IN ACCESS EXCLUSIVE MODE")
    )
    conflict = await _legacy_conflict(connection, table)
    if conflict:
        raise ValueError(conflict)

    grants, assets, reviews = (table[key] for key in ("grants", "assets", "reviews"))
    for kind in ("agent", "team"):
        await connection.execute(
            text(f"""
            ALTER TABLE {table[kind]} ADD COLUMN IF NOT EXISTS shared_version bigint
        """)
        )
        await connection.execute(
            text(f"""
            UPDATE {table[kind]} AS head
            SET shared_version = grant_row.resource_version::bigint
            FROM {grants} AS grant_row
            WHERE grant_row.resource_type = '{kind}' AND grant_row.resource_id = head.id
              AND grant_row.resource_view_type = 'shared' AND {DIRECT_GRANTS}
        """)
        )
    await connection.execute(
        text(f"""
        ALTER TABLE {assets}
          ADD COLUMN IF NOT EXISTS is_shared_current boolean NOT NULL DEFAULT false,
          ADD COLUMN IF NOT EXISTS publication_revision bigint
              CHECK (publication_revision > 0)
    """)
    )
    await connection.execute(
        text(f"""
        WITH ranked AS (
            SELECT resource_type, resource_id, resource_version,
                   row_number() OVER (
                       PARTITION BY resource_type, resource_id
                       ORDER BY created_at, resource_version
                   ) AS revision
            FROM {assets}
            WHERE resource_type IN ('skill','mcp')
        )
        UPDATE {assets} AS asset SET publication_revision = ranked.revision
        FROM ranked WHERE (asset.resource_type, asset.resource_id, asset.resource_version)
            = (ranked.resource_type, ranked.resource_id, ranked.resource_version)
          AND asset.publication_revision IS NULL
    """)
    )
    await connection.execute(
        text(f"""
        UPDATE {assets} AS asset SET is_shared_current = true
        FROM {grants} AS grant_row
        WHERE (asset.resource_type, asset.resource_id, asset.resource_version)
            = (grant_row.resource_type, grant_row.resource_id, grant_row.resource_version)
          AND grant_row.resource_view_type = 'shared' AND {DIRECT_GRANTS}
    """)
    )
    await connection.execute(
        text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ak_asset_publication_revision
            ON {assets} (resource_type, resource_id, publication_revision)
    """)
    )
    await connection.execute(
        text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ak_asset_shared_current
            ON {assets} (resource_type, resource_id) WHERE is_shared_current
    """)
    )

    await connection.execute(
        text(f"""
        ALTER TABLE {reviews}
            ADD COLUMN IF NOT EXISTS review_type varchar(16) NOT NULL DEFAULT 'share',
            ALTER COLUMN share_scope_type DROP NOT NULL,
            ALTER COLUMN principal_id DROP NOT NULL,
            DROP CONSTRAINT IF EXISTS ck_ak_share_target
    """)
    )
    await connection.execute(
        text(f"""
        ALTER TABLE {reviews} ADD CONSTRAINT ck_ak_share_target CHECK (
            (review_type = 'share' AND share_scope_type IS NOT NULL
             AND share_scope_type IN ('user','group') AND principal_id IS NOT NULL
             AND principal_id NOT IN ('', '*')) OR
            (review_type = 'update' AND share_scope_type IS NULL AND principal_id IS NULL)
        )
    """)
    )
    await connection.execute(
        text(f"""
        UPDATE {reviews} AS review SET is_current = false
        WHERE review.is_current AND NOT EXISTS (
            SELECT 1 FROM {grants}
            WHERE source_review_id = review.review_id AND {DIRECT_GRANTS}
        )
    """)
    )
    await connection.execute(
        text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ak_pending_share_update
            ON {reviews} (resource_type, resource_id)
            WHERE review_type = 'update' AND review_status_type = 'pending'
    """)
    )

    quote = connection.dialect.identifier_preparer.quote_identifier
    old_index = f"{quote(schema)}.uq_ak_active_grant_origin"
    await connection.execute(text(f"DROP INDEX IF EXISTS {old_index}"))
    await connection.execute(
        text(f"""
        UPDATE {grants} SET origin_key = 'share' WHERE grant_source_type = 'share'
    """)
    )
    key = ", ".join(GRANT_KEY)
    await connection.execute(
        text(f"""
        WITH ranked AS (
            SELECT id, row_number() OVER (
                PARTITION BY {key} ORDER BY granted_at DESC, id
            ) AS position
            FROM {grants} WHERE revoked_at IS NULL
              AND grant_source_type IN ('owner', 'dependency')
        )
        UPDATE {grants} AS grant_row
        SET revoked_at = now(), revoked_by = 'schema-upgrade'
        FROM ranked WHERE grant_row.id = ranked.id AND ranked.position > 1
    """)
    )
    await connection.execute(text(f"ALTER TABLE {grants} DROP COLUMN resource_version"))
    await connection.execute(
        text(f"""
        CREATE UNIQUE INDEX uq_ak_active_grant_origin ON {grants} ({key})
        WHERE revoked_at IS NULL
    """)
    )
    return "upgraded"
