"""重置开发库的技能目录、授权和导入记录。"""

from pathlib import Path
from shutil import rmtree
from tempfile import mkdtemp

from sqlalchemy import text

from ._schema import SchemaReport, _existing_tables, _qualified, _target
from .migration._skills import SKILL_TABLES
from .model._tables import (
    metadata,
    skill_detail,
    skill_imports,
    skill_summary,
    virtual_users,
)

RESOURCE_TABLES = (
    "ak_agent_personal_resources",
    "ak_resource_permission",
    "ak_approve",
    "ak_resource_assets",
)
DEFINITION_TABLES = ("ak_agent_definition_detail", "ak_team_definition_detail")
SKILL_ONLY_TABLES = ("ak_skill_imports", "ak_skill_archives", "skills")
SKILL_VERSION_TABLES = tuple(table.name for table in SKILL_TABLES)
SESSION_SKILL_STATE = "'{state,middle_context,egis_agentkit,activated_skills}'"
SKILL_BINDINGS = "jsonb_typeof(payload->'bindings') = 'array'"
HAS_SKILL_BINDING = f"{SKILL_BINDINGS} AND payload->'bindings' @> '[{{\"resource_type\":\"skill\"}}]'::jsonb"


def _reset_paths(workspace_root, upload_root):
    """取得受管路径，并验证父目录没有通过符号链接逃逸。"""
    root = Path(workspace_root).absolute()
    uploads = Path(upload_root).absolute()
    if root == Path("/") or root.resolve() != root:
        raise ValueError("技能清理需要实际的工作区根目录")
    if not uploads.is_relative_to(root) or uploads == root:
        raise ValueError("技能上传目录必须在工作区内")
    if uploads.relative_to(root).parts[0] in {"common", "user", "projects"}:
        raise ValueError("技能上传目录不能与工作区内容重叠")
    candidates = {
        uploads,
        root / "skill-archives",
        root / "common" / "skills",
        *root.glob("user/*/skills"),
        *root.glob("common/*/config-*/skills"),
    }
    paths = []
    for path in sorted(candidates):
        if path.resolve() != path:
            raise ValueError(f"技能目录含符号链接：{path}")
        if path.exists():
            if not path.is_dir():
                raise ValueError(f"技能路径不是目录：{path}")
            if not any(path.is_relative_to(parent) for parent in paths):
                paths.append(path)
    return root, paths


async def _reset_rows(connection, schema, names, *, apply):
    filters = {
        **dict.fromkeys(RESOURCE_TABLES, "resource_type = 'skill'"),
        **dict.fromkeys(SKILL_ONLY_TABLES, "TRUE"),
        **dict.fromkeys(SKILL_VERSION_TABLES, "TRUE"),
        **dict.fromkeys(DEFINITION_TABLES, HAS_SKILL_BINDING),
        "sessions": f"payload::jsonb #> {SESSION_SKILL_STATE} IS NOT NULL",
    }
    counts = {}
    for name, where in filters.items():
        if name in names:
            table = _qualified(connection, schema, name)
            counts[name] = await connection.scalar(
                text(f"SELECT count(*) FROM {table} WHERE {where}")
            )
    if not apply:
        return counts

    for name in counts:
        table = _qualified(connection, schema, name)
        if name == skill_detail.name and skill_summary.name in counts:
            # 汇总删除时由外键一并清理历史版本。
            continue
        if name in SKILL_ONLY_TABLES:
            await connection.execute(text(f"DROP TABLE {table} RESTRICT"))
        elif name in DEFINITION_TABLES:
            await connection.execute(
                text(f"""
                UPDATE {table} SET payload = jsonb_set(
                    payload, '{{bindings}}',
                    COALESCE((
                        SELECT jsonb_agg(binding ORDER BY position)
                        FROM jsonb_array_elements(payload->'bindings')
                          WITH ORDINALITY AS entries(binding, position)
                        WHERE binding->>'resource_type' IS DISTINCT FROM 'skill'
                    ), '[]'::jsonb)
                ) WHERE {HAS_SKILL_BINDING}
            """)
            )
        elif name == "sessions":
            await connection.execute(
                text(f"""
                UPDATE {table} SET payload = (payload::jsonb #- {SESSION_SKILL_STATE})::json
                WHERE {filters[name]}
            """)
            )
        else:
            await connection.execute(text(f"DELETE FROM {table} WHERE {filters[name]}"))
    await connection.run_sync(
        metadata.create_all, tables=[skill_imports, virtual_users, *SKILL_TABLES]
    )
    return counts


async def reset_skills(
    engine, workspace_root, upload_root, *, database=None, apply=False
):
    """核对数据库，暂移技能目录，再在同一事务内清理数据并创建导入表。"""
    root, paths = _reset_paths(workspace_root, upload_root)
    moved = []
    quarantine = None
    try:
        async with engine.begin() as connection:
            actual, schema = await _target(connection, database, apply)
            existing = await _existing_tables(connection, schema)
            if not {"ak_resource_assets", "ak_agent_definition_detail"}.issubset(
                existing
            ):
                raise ValueError("目标不是已初始化的 AgentKit 数据库")
            if apply:
                locked = existing.intersection(
                    (
                        *RESOURCE_TABLES,
                        *SKILL_ONLY_TABLES,
                        *SKILL_VERSION_TABLES,
                        *DEFINITION_TABLES,
                        "sessions",
                    )
                )
                tables = ", ".join(
                    _qualified(connection, schema, name) for name in sorted(locked)
                )
                await connection.execute(
                    text(f"LOCK TABLE {tables} IN ACCESS EXCLUSIVE MODE")
                )
                if paths:
                    quarantine = Path(mkdtemp(prefix=".skill-reset-", dir=root))
                    for path in paths:
                        destination = quarantine / path.relative_to(root)
                        destination.parent.mkdir(parents=True, exist_ok=True)
                        path.rename(destination)
                        moved.append((path, destination))
            counts = await _reset_rows(connection, schema, existing, apply=apply)
    except BaseException:
        for path, destination in reversed(moved):
            destination.rename(path)
        if quarantine:
            rmtree(quarantine)
        raise
    if quarantine:
        rmtree(quarantine)
    states = {name: f"{count} 条" for name, count in counts.items()}
    states.update({str(path): "已清理" if apply else "待清理" for path in paths})
    return SchemaReport(actual, schema, states)
