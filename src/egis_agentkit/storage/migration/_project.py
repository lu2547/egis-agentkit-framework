"""显式升级项目文件表名和会话工作区身份，保留所有业务记录。

核心能力
- 定位单一物理表并拒绝多份同类数据，原子重命名表、索引和序列。
- 按用户回填 SessionConfig.workspace_id，不改变项目、cwd 或会话状态。
- 预览不写入，重复执行返回 current；不清理指纹、流水或产物。
"""

from sqlalchemy import inspect, text

from egis_agentkit.core.workspace._identity import workspace_id_for

TABLE_SOURCES = {
    "ak_project_file_fingerprints": (
        "ak_file_fingerprints",
        "ak_workspace_file_fingerprints",
    ),
    "ak_project_file_changes": ("ak_file_changes", "ak_workspace_file_changes"),
    "ak_project_reply_artifacts": ("ak_reply_artifacts",),
}
INDEX_RENAMES = {
    "ix_ak_workspace_change_reply": "ix_ak_project_change_reply",
    "ix_ak_workspace_change_path": "ix_ak_project_change_path",
}
PROJECT_BINDING = "'{state,middle_context,egis_agentkit,project_id}'"


def qualified(connection, schema, name):
    """只对程序给定的标识符引用，值参数仍通过绑定变量传递。"""
    quote = connection.dialect.identifier_preparer.quote_identifier
    return f"{quote(schema)}.{quote(name)}"


async def project_tables(connection, schema):
    """返回目标表对应的实际表名；同时存在多份时停止升级。"""
    existing = set(
        await connection.run_sync(
            lambda conn: inspect(conn).get_table_names(schema=schema)
        )
    )
    result = {}
    for target, sources in TABLE_SOURCES.items():
        found = existing.intersection((target, *sources))
        if len(found) > 1:
            raise ValueError(
                f"项目文件表同时存在多份，请先核对：{', '.join(sorted(found))}"
            )
        result[target] = next(iter(found), target)
    return result


async def migrate_project_tables(connection, schema, *, apply=False):
    """在调用方事务内重命名项目表及其附属对象，不复制或删除数据。"""
    actual = await project_tables(connection, schema)
    existing = set(
        await connection.run_sync(
            lambda conn: inspect(conn).get_table_names(schema=schema)
        )
    )
    quote = connection.dialect.identifier_preparer.quote_identifier
    states = {}
    for target, source in actual.items():
        if source not in existing:
            continue
        states[source] = "current" if target == source else "pending"
        if not apply:
            continue
        # 1. 重命名表，所有外键与业务记录保持原对象。
        if target != source:
            await connection.execute(
                text(
                    f"ALTER TABLE {qualified(connection, schema, source)} RENAME TO {quote(target)}"
                )
            )
            states[source] = "renamed"
        # 2. 约束及普通索引名称同步，避免下一次初始化重复创建索引。
        for old_prefix in TABLE_SOURCES[target]:
            constraints = await connection.execute(
                text("""
                SELECT conname FROM pg_constraint
                WHERE conrelid=to_regclass(:table)
            """),
                {"table": qualified(connection, schema, target)},
            )
            for name in constraints.scalars():
                if name.startswith(old_prefix + "_"):
                    new = target + name[len(old_prefix) :]
                    await connection.execute(
                        text(
                            f"ALTER TABLE {qualified(connection, schema, target)} RENAME CONSTRAINT {quote(name)} TO {quote(new)}"
                        )
                    )
            indexes = await connection.run_sync(
                lambda conn: inspect(conn).get_indexes(target, schema=schema)
            )
            for item in indexes:
                old = item["name"]
                new = INDEX_RENAMES.get(old)
                if new is None and old.startswith("ix_" + old_prefix + "_"):
                    new = "ix_" + target + old[len("ix_" + old_prefix) :]
                if new:
                    await _rename_index(connection, schema, old, new)
        # 3. 自增序列只改名，不改变当前值或归属关系。
        if target == "ak_project_file_changes":
            sequence = await connection.scalar(
                text("SELECT pg_get_serial_sequence(:table, 'change_id')"),
                {"table": qualified(connection, schema, target)},
            )
            if sequence:
                current_name = await connection.scalar(
                    text(
                        "SELECT relname FROM pg_class WHERE oid=to_regclass(:sequence)"
                    ),
                    {"sequence": sequence},
                )
                desired_name = target + "_change_id_seq"
                if current_name != desired_name:
                    await connection.execute(
                        text(
                            f"ALTER SEQUENCE {qualified(connection, schema, current_name)} RENAME TO {quote(desired_name)}"
                        )
                    )
    return states


async def _rename_index(connection, schema, old, new):
    """统一索引名称；只有完整定义一致的重复普通索引才允许合并。"""

    async def signature(name):
        row = (
            await connection.execute(
                text("""
            SELECT i.indrelid, i.indisunique, i.indisexclusion,
                   i.indisvalid, i.indisready, i.indnkeyatts,
                   i.indkey::text, i.indclass::text, i.indcollation::text,
                   i.indoption::text, c.relam,
                   pg_get_expr(i.indexprs, i.indrelid) AS expressions,
                   pg_get_expr(i.indpred, i.indrelid) AS predicate
            FROM pg_index i JOIN pg_class c ON c.oid=i.indexrelid
            WHERE i.indexrelid=to_regclass(:index)
        """),
                {"index": qualified(connection, schema, name)},
            )
        ).first()
        return tuple(row) if row is not None else None

    # 1. 存量库可能同时留下两轮表名生成的普通索引，先核对目标。
    current = await signature(new)
    if current is not None:
        if current != await signature(old):
            raise ValueError(f"索引 {old} 与 {new} 定义不一致，停止升级")
        # 2. 已存在完全一致的新索引，移除重复旧索引，不触碰表和记录。
        await connection.execute(
            text(f"DROP INDEX {qualified(connection, schema, old)}")
        )
        return
    # 3. 没有目标时直接改名，不重建索引。
    quote = connection.dialect.identifier_preparer.quote_identifier
    await connection.execute(
        text(f"ALTER INDEX {qualified(connection, schema, old)} RENAME TO {quote(new)}")
    )


async def migrate_session_workspaces(connection, schema, *, apply=False):
    """把有项目绑定的原生会话指向用户 Workspace，保留项目及其他配置。"""
    table = qualified(connection, schema, "sessions")
    existing = await connection.scalar(
        text("SELECT to_regclass(:table)"), {"table": table}
    )
    if existing is None:
        return "absent"
    # 1. 仅处理企业项目会话；不同项目的同一用户使用同一逻辑身份。
    rows = await connection.execute(
        text(f"""
        SELECT DISTINCT user_id, payload #>> '{{config,workspace_id}}' AS workspace_id
        FROM {table} WHERE payload #>> {PROJECT_BINDING} IS NOT NULL
    """)
    )
    pending_users = {
        row.user_id for row in rows if row.workspace_id != workspace_id_for(row.user_id)
    }
    if not pending_users:
        return "current"
    if not apply:
        return "pending"
    # 2. 只更新配置里的 Workspace ID，project_id 仍由会话状态生成。
    for user_id in sorted(pending_users):
        await connection.execute(
            text(f"""
            UPDATE {table} SET payload=jsonb_set(
                payload::jsonb, '{{config,workspace_id}}', to_jsonb(CAST(:workspace AS text))
            )::json
            WHERE user_id=:user_id AND payload #>> {PROJECT_BINDING} IS NOT NULL
        """),
            {"user_id": user_id, "workspace": workspace_id_for(user_id)},
        )
    return "upgraded"
