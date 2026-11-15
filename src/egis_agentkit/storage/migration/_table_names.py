"""显式升级业务表及关联对象名称，保留记录、主外键和索引语义。

核心能力
- 预览只读取目录，发现新旧两份表时标记冲突。
- 执行在调用方事务中改名表、约束及索引，不复制或删除业务数据。
- 重复执行返回 current；运行时模型和查询不保留旧表名兼容入口。
- 历史名称汇合时只合并定义完全相同的普通索引，不覆盖约束或不同索引。
"""

from sqlalchemy import inspect, text

RENAMES = {
    "ak_remote_agent_runs": "ak_agent_remote_runs",
    "ak_personal_agent_resources": "ak_agent_personal_resources",
    "ak_session_reply_feedback": "ak_feedback",
    "ak_share_reviews": "ak_approve",
    "ak_resource_access": "ak_resource_permission",
    "ak_resource_grants": "ak_resource_permission",
}
OBJECT_RENAMES = {
    "ix_ak_personal_resources_user_type": "ix_ak_agent_personal_resources_user_type",
    "uq_ak_reply_feedback_user_reply": "uq_ak_feedback_user_reply",
    "ix_ak_reply_feedback_created": "ix_ak_feedback_created",
    "uq_ak_active_grant_origin": "uq_ak_resource_permission_origin",
    "ix_ak_access_resource_principal": "ix_ak_resource_permission_resource_principal",
    "ck_ak_grant_view": "ck_ak_resource_permission_view",
    "ck_ak_grant_source": "ck_ak_resource_permission_source",
    "ck_ak_grant_dependency": "ck_ak_resource_permission_dependency",
    "ck_ak_share_target": "ck_ak_approve_target",
    "ix_ak_share_resource_version": "ix_ak_approve_resource_version",
    "uq_ak_pending_share_update": "uq_ak_approve_pending_update",
}


def _qualified(connection, schema, name):
    """表和目录返回的对象名使用标识符引用，查询值继续绑定参数。"""
    quote = connection.dialect.identifier_preparer.quote_identifier
    return f"{quote(schema)}.{quote(name)}"


def _object_name(name, old, new):
    """转换默认约束/索引前缀及已经明确登记的自定义索引名。"""
    if name in OBJECT_RENAMES:
        return OBJECT_RENAMES[name]
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


def _table_sources():
    """把同一目标的历史名称集中，统一识别多份物理表冲突。"""
    result = {}
    for old, target in RENAMES.items():
        result.setdefault(target, []).append(old)
    return result


async def resolve_table_names(connection, schema):
    """供升级预检查读取真实表，运行时查询不使用历史别名。"""
    names = set(
        await connection.run_sync(
            lambda sync: inspect(sync).get_table_names(schema=schema)
        )
    )
    result = {}
    for target, sources in _table_sources().items():
        found = names.intersection([target, *sources])
        if len(found) > 1:
            raise ValueError("同时存在多份表：" + ", ".join(sorted(found)))
        result[target] = next(iter(found), target)
    return result


async def _index_details(connection, schema, name):
    """读取完整索引结构及约束归属，比较时只排除各索引自身的 OID。"""
    return (
        (
            await connection.execute(
                text("""
                SELECT jsonb_build_object(
                    'definition', to_jsonb(i) - 'indexrelid',
                    'method', c.relam, 'options', c.reloptions,
                    'tablespace', c.reltablespace
                ) AS signature,
                EXISTS (SELECT 1 FROM pg_constraint p WHERE p.conindid=c.oid)
                    AS constraint_backed
                FROM pg_class c LEFT JOIN pg_index i ON i.indexrelid=c.oid
                WHERE c.oid=to_regclass(:name)
            """),
                {"name": _qualified(connection, schema, name)},
            )
        )
        .mappings()
        .first()
    )


async def _resolve_index_renames(connection, schema, objects):
    """预检现有和计划中的目标名；完全相同的普通索引才允许移除冗余副本。"""
    destinations = {}
    operations = []
    for kind, name, desired in objects:
        if kind != "index":
            operations.append((kind, name, desired))
            continue
        source = await _index_details(connection, schema, name)
        if desired not in destinations:
            destinations[desired] = await _index_details(connection, schema, desired)
        target = destinations[desired]
        if target is None:
            destinations[desired] = source
            operations.append((kind, name, desired))
        elif (
            source is not None
            and source["signature"] == target["signature"]
            and not source["constraint_backed"]
            and not target["constraint_backed"]
        ):
            operations.append(("drop_index", name, desired))
        else:
            raise ValueError(f"索引升级冲突：{name} 与 {desired} 定义不同或属于约束")
    return operations


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
    for target, sources in _table_sources().items():
        if targets is not None and target not in targets:
            continue
        # 1. 同时核对所有历史名称，禁止把两份业务数据静默合并。
        found = names.intersection([target, *sources])
        if len(found) > 1:
            if apply:
                raise ValueError("表名升级冲突：同时存在 " + ", ".join(sorted(found)))
            states.update({name: "inconsistent" for name in found if name != target})
            continue
        if not found:
            continue
        actual = next(iter(found))
        objects = []
        for old in sources:
            for item in await _object_renames(connection, schema, actual, old, target):
                if item not in objects:
                    objects.append(item)
        try:
            objects = await _resolve_index_renames(connection, schema, objects)
        except ValueError:
            if apply:
                raise
            states[actual] = "inconsistent"
            continue
        states[actual] = "pending" if actual != target or objects else "current"
        if not apply:
            continue
        # 2. 原地改名，所有外键和业务记录保持原对象。
        if actual != target:
            await connection.execute(
                text(
                    f"ALTER TABLE {_qualified(connection, schema, actual)} RENAME TO {quote(target)}"
                )
            )
            states[actual] = "renamed"
        # 3. 约束及其索引由 PostgreSQL 同步；普通索引单独改名。
        for kind, name, desired in objects:
            if kind == "constraint":
                statement = f"ALTER TABLE {_qualified(connection, schema, target)} RENAME CONSTRAINT {quote(name)} TO {quote(desired)}"
            elif kind == "drop_index":
                statement = (
                    f"DROP INDEX {_qualified(connection, schema, name)} RESTRICT"
                )
            else:
                statement = f"ALTER INDEX {_qualified(connection, schema, name)} RENAME TO {quote(desired)}"
            await connection.execute(text(statement))
        if actual == target and objects:
            states[actual] = "renamed"
    return states
