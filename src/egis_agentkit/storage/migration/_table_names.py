"""显式升级业务表及关联对象名称，保留记录、主外键和索引语义。

核心能力
- 预览只读取目录，发现新旧两份表时标记冲突。
- 执行在调用方事务中改名表、约束及索引，不复制或删除业务数据。
- 重复执行返回 current；运行时模型和查询不保留旧表名兼容入口。
"""

from sqlalchemy import inspect, text

RENAMES = {
    "ak_remote_agent_runs": "ak_agent_remote_runs",
    "ak_personal_agent_resources": "ak_agent_personal_resources",
    "ak_session_reply_feedback": "ak_feedback",
}
INDEX_RENAMES = {
    "ix_ak_personal_resources_user_type": "ix_ak_agent_personal_resources_user_type",
    "uq_ak_reply_feedback_user_reply": "uq_ak_feedback_user_reply",
    "ix_ak_reply_feedback_created": "ix_ak_feedback_created",
}


def _qualified(connection, schema, name):
    """表和目录返回的对象名使用标识符引用，查询值继续绑定参数。"""
    quote = connection.dialect.identifier_preparer.quote_identifier
    return f"{quote(schema)}.{quote(name)}"


def _object_name(name, old, new):
    """转换默认约束/索引前缀及已经明确登记的自定义索引名。"""
    if name in INDEX_RENAMES:
        return INDEX_RENAMES[name]
    for prefix in ("", "ix_", "uq_"):
        previous = prefix + old + "_"
        if name.startswith(previous):
            return prefix + new + name[len(prefix + old) :]
    return name


async def _object_renames(connection, schema, table, old, new):
    """枚举待改名对象；约束附带的索引由 PostgreSQL 随约束一起改名。"""
    constraints = await connection.execute(
        text("SELECT conname FROM pg_constraint WHERE conrelid=to_regclass(:table)"),
        {"table": _qualified(connection, schema, table)},
    )
    result = []
    for name in constraints.scalars():
        desired = _object_name(name, old, new)
        if desired != name:
            result.append(("constraint", name, desired))
    indexes = await connection.run_sync(
        lambda sync: inspect(sync).get_indexes(table, schema=schema)
    )
    for index in indexes:
        if index.get("duplicates_constraint"):
            continue
        name = index["name"]
        desired = _object_name(name, old, new)
        if desired != name:
            result.append(("index", name, desired))
    return result


async def migrate_table_names(connection, schema, *, apply=False, targets=None):
    """预览或执行受管表改名，可指定目标表集合；冲突由调用方事务整体回滚。"""
    if targets is not None and targets - set(RENAMES.values()):
        raise ValueError("包含未登记的表名升级目标")
    names = set(
        await connection.run_sync(
            lambda sync: inspect(sync).get_table_names(schema=schema)
        )
    )
    states = {}
    quote = connection.dialect.identifier_preparer.quote_identifier
    for old, new in RENAMES.items():
        if targets is not None and new not in targets:
            continue
        # 1. 先识别物理表，禁止把两份业务数据静默合并或覆盖。
        if old in names and new in names:
            if apply:
                raise ValueError(f"表名升级冲突：同时存在 {old} 与 {new}")
            states[old] = "inconsistent"
            continue
        actual = old if old in names else new if new in names else None
        if actual is None:
            continue
        objects = await _object_renames(connection, schema, actual, old, new)
        states[actual] = "pending" if actual == old or objects else "current"
        if not apply:
            continue
        # 2. 同一个物理表原地改名，记录和外键关联保持原对象。
        if actual == old:
            await connection.execute(
                text(
                    f"ALTER TABLE {_qualified(connection, schema, old)} RENAME TO {quote(new)}"
                )
            )
            states[actual] = "renamed"
        # 3. 同步约束及索引名，避免后续初始化出现旧名或重复索引。
        for kind, name, desired in objects:
            if kind == "constraint":
                statement = f"ALTER TABLE {_qualified(connection, schema, new)} RENAME CONSTRAINT {quote(name)} TO {quote(desired)}"
            else:
                statement = f"ALTER INDEX {_qualified(connection, schema, name)} RENAME TO {quote(desired)}"
            await connection.execute(text(statement))
        if actual != old and objects:
            states[actual] = "renamed"
    return states
