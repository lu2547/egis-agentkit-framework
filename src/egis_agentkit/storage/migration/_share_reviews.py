"""Explicit additive upgrade for withdrawal approvals and submitted snapshots."""

from sqlalchemy import inspect, text

from ._table_names import resolve_table_names


async def migrate_share_reviews(connection, schema, *, apply=False):
    names = await resolve_table_names(connection, schema)
    table = names.get("ak_approve", "ak_approve")
    def layout(conn):
        inspector = inspect(conn)
        if table not in inspector.get_table_names(schema=schema):
            return None
        return (
            {row["name"] for row in inspector.get_columns(table, schema=schema)},
            {row["name"] for row in inspector.get_indexes(table, schema=schema)},
            {row["name"]: row["sqltext"] for row in inspector.get_check_constraints(table, schema=schema)},
        )
    current = await connection.run_sync(layout)
    if current is None:
        return "absent"
    columns, indexes, constraints = current
    pending = not (
        {"original_review_id", "content_snapshot"} <= columns
        and "uq_ak_approve_pending_revoke" in indexes
        and "ck_ak_approve_original" in constraints
        and "revoke" in constraints.get("ck_ak_approve_target", "")
    )
    if not pending:
        return "current"
    if not apply:
        return "pending"
    quote = connection.dialect.identifier_preparer.quote_identifier
    target = f"{quote(schema)}.{quote(table)}"
    await connection.execute(text(f"""
        ALTER TABLE {target}
            ADD COLUMN IF NOT EXISTS original_review_id varchar(64) REFERENCES {target}(review_id),
            ADD COLUMN IF NOT EXISTS content_snapshot jsonb,
            DROP CONSTRAINT IF EXISTS ck_ak_approve_target,
            DROP CONSTRAINT IF EXISTS ck_ak_share_target,
            DROP CONSTRAINT IF EXISTS ck_ak_approve_original
    """))
    await connection.execute(text(f"""
        ALTER TABLE {target}
          ADD CONSTRAINT ck_ak_approve_target CHECK (
            (review_type IN ('share','revoke') AND share_scope_type IS NOT NULL
             AND share_scope_type IN ('user','group') AND principal_id IS NOT NULL
             AND principal_id NOT IN ('','*')) OR
            (review_type='update' AND share_scope_type IS NULL AND principal_id IS NULL)),
          ADD CONSTRAINT ck_ak_approve_original CHECK (
            (review_type='revoke') = (original_review_id IS NOT NULL))
    """))
    await connection.execute(text(f"""
        CREATE UNIQUE INDEX IF NOT EXISTS uq_ak_approve_pending_revoke
          ON {target}(original_review_id)
          WHERE review_type='revoke' AND review_status_type='pending'
    """))
    return "upgraded"
