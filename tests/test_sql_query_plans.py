"""Exercise real scoped queries against large unrelated tables, with the planner enabled."""

import json
from contextlib import contextmanager

from sqlalchemy import event, text
from test_domain import identities

from egis_agentkit.service.project._project_manager import ProjectManager


def nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from nodes(child)


def report_plan(scenario, plan):
    print(
        "SQL_PLAN "
        + json.dumps(
            {
                "scenario": scenario,
                "execution_ms": plan["Execution Time"],
                "shared_hit_blocks": plan["Plan"]["Shared Hit Blocks"],
                "indexes": [
                    {
                        "index": node["Index Name"],
                        "rows": node["Actual Rows"],
                        "loops": node["Actual Loops"],
                        "rows_removed": node.get("Rows Removed by Filter", 0),
                    }
                    for node in nodes(plan["Plan"])
                    if "Index Name" in node
                ],
            }
        )
    )


@contextmanager
def capture_selects(storage):
    result = []

    def capture(_conn, _cursor, statement, _params, context, _many):
        if context.compiled is not None and getattr(
            context.compiled.statement, "is_select", False
        ):
            result.append(context.compiled.statement)

    event.listen(storage.engine.sync_engine, "before_cursor_execute", capture)
    try:
        yield result
    finally:
        event.remove(storage.engine.sync_engine, "before_cursor_execute", capture)


async def explain(storage, statement):
    compiled = statement.compile(
        dialect=storage.engine.dialect, compile_kwargs={"literal_binds": True}
    )
    async with storage.engine.connect() as conn:
        result = await conn.scalar(
            text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + str(compiled))
        )
    if isinstance(result, str):
        result = json.loads(result)
    return result[0]


async def test_large_project_history_stops_at_indexed_page(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    project = await ProjectManager(storage, tmp_path).create(alice, "Indexed")
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO ak_project_file_changes (project_id, area, path, change_type)
            SELECT CASE WHEN n <= 5000 THEN :pid ELSE 'other-' || (n % 100)::text END,
                   'work', n::text, 'modified'
            FROM generate_series(1, 50000) n
        """),
            {"pid": project["id"]},
        )
        await conn.execute(text("ANALYZE ak_project_file_changes"))
    with capture_selects(storage) as queries:
        rows, error = await storage.project_files.list_changes(project["id"], None, 20)
    assert len(rows) == 21
    assert error is None
    plan = await explain(storage, queries[0])
    report_plan("50k-project-changes-page", plan)
    large_sorts = [
        node
        for node in nodes(plan["Plan"])
        if node["Node Type"] == "Sort"
        and any(child["Actual Rows"] > 21 for child in node.get("Plans", []))
    ]
    assert not large_sorts, json.dumps(plan)
    assert any(
        node.get("Index Name") == "ix_ak_project_changes_page"
        for node in nodes(plan["Plan"])
    )
    assert not any(
        node["Node Type"] == "Seq Scan"
        and node.get("Relation Name") == "ak_project_file_changes"
        for node in nodes(plan["Plan"])
    )


async def test_large_session_history_uses_owner_time_index(storage):
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO sessions (id, user_id, agent_id, source, created_at, updated_at, payload)
            SELECT 'session-' || n::text,
                   CASE WHEN n <= 5000 THEN 'target' ELSE 'other-' || (n % 100)::text END,
                   'agent', 'user', now(), now() + n * interval '1 second',
                   '{"state":{"middle_context":{"egis_agentkit":{"project_id":"project"}}}}'::json
            FROM generate_series(1, 50000) n
        """)
        )
        await conn.execute(text("ANALYZE sessions"))
    from egis_agentkit.storage._session import SessionStorage

    plan = await explain(storage, SessionStorage.main_records_query("target", limit=20))
    report_plan("50k-sessions-page", plan)
    assert any(
        node.get("Index Name") == "ix_sessions_main_user_updated"
        for node in nodes(plan["Plan"])
    ), json.dumps(plan)
    assert not any(node["Node Type"] == "Seq Scan" for node in nodes(plan["Plan"]))
    assert not any(
        node["Node Type"] == "Sort"
        and any(child["Actual Rows"] > 20 for child in node.get("Plans", []))
        for node in nodes(plan["Plan"])
    )
    scan = next(
        node
        for node in nodes(plan["Plan"])
        if node.get("Index Name") == "ix_sessions_main_user_updated"
    )
    assert scan["Actual Rows"] == 20


async def test_mixed_main_sessions_keep_indexed_user_page_and_hide_workers(storage):
    from egis_agentkit.storage._session import SessionStorage

    async with storage.engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO sessions (id, user_id, agent_id, source, source_schedule_id, created_at, updated_at, payload)
            SELECT 'mixed-' || n::text,
                   CASE WHEN n <= 5000 THEN 'target' ELSE 'other-' || (n % 100)::text END,
                   'agent', CASE WHEN n % 100 = 0 THEN 'schedule' WHEN n % 3 = 0 THEN 'team' ELSE 'user' END,
                   CASE WHEN n % 100 = 0 THEN 'morning' END,
                   now(), now() + n * interval '1 second',
                   '{"state":{"middle_context":{"egis_agentkit":{"project_id":"project"}}}}'::json
            FROM generate_series(1, 50000) n
        """)
        )
        await conn.execute(text("ANALYZE sessions"))
        rows = (
            await conn.execute(SessionStorage.main_records_query("target", limit=20))
        ).mappings().all()
    assert len(rows) == 20
    assert rows[0]["id"] == "mixed-5000"
    assert {row["source"] for row in rows} == {"user", "schedule"}
    assert all(row["user_id"] == "target" for row in rows)
    assert [row["updated_at"] for row in rows] == sorted(
        (row["updated_at"] for row in rows), reverse=True
    )
    plan = await explain(storage, SessionStorage.main_records_query("target", limit=20))
    report_plan("50k-mixed-sessions-page", plan)
    assert any(
        node.get("Index Name") == "ix_sessions_main_user_updated"
        for node in nodes(plan["Plan"])
    )
    assert not any(node["Node Type"] == "Seq Scan" for node in nodes(plan["Plan"]))


async def test_schedule_completion_batch_uses_existing_history_index(storage):
    from test_schedule_storage import task

    await storage.schedules.create(task())
    await storage.schedules.create(task("unrelated"))
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO ak_schedule_runs
                (id, schedule_id, schedule_name, project_id, trigger_type,
                 status, created_at, finished_at, input_snapshot, session_id)
            SELECT 'run-' || lpad(n::text, 5, '0'),
                   CASE WHEN n <= 500 THEN 'morning' ELSE 'unrelated' END,
                   'Report', 'workspace', 'test', 'succeeded',
                   now() + n * interval '1 second', now() + n * interval '1 second',
                   '{"agent_id":"writer"}'::jsonb,
                   CASE WHEN n <= 500 THEN 'session' ELSE 'other-session' END
            FROM generate_series(1, 50000) n
        """)
        )
        await conn.execute(text("ANALYZE ak_schedule_runs"))
    with capture_selects(storage) as queries:
        assert await storage.schedules.latest_completed_run_ids(
            "alice", {"session": ("morning", "workspace")}
        ) == {"session": "run-00500"}
    assert len(queries) == 1
    plan = await explain(storage, queries[0])
    report_plan("50k-schedule-completion-batch", plan)
    assert any(
        node.get("Index Name") == "ix_ak_schedule_runs_history"
        for node in nodes(plan["Plan"])
    )
    assert not any(
        node["Node Type"] == "Seq Scan"
        and node.get("Relation Name") == "ak_schedule_runs"
        for node in nodes(plan["Plan"])
    )


async def test_online_index_preview_and_apply_do_not_rewrite_business_data(storage):
    from egis_agentkit.storage._query_indexes import optimize_query_indexes

    async with storage.engine.begin() as conn:
        database = await conn.scalar(text("SELECT current_database()"))
        await conn.execute(text("DROP INDEX IF EXISTS ix_sessions_main_user_updated"))
        await conn.execute(
            text("CREATE INDEX ix_sessions_updated_at ON sessions(updated_at)")
        )
        await conn.execute(
            text(
                "CREATE INDEX ix_ak_project_file_changes_project_id ON ak_project_file_changes(project_id)"
            )
        )
    preview = await optimize_query_indexes(storage.engine, database=database)
    assert preview.tables["ix_sessions_main_user_updated"] == "create concurrently"
    assert preview.tables["ix_sessions_updated_at"] == "drop redundant concurrently"
    async with storage.engine.connect() as conn:
        assert (
            await conn.scalar(
                text("SELECT to_regclass('ix_sessions_main_user_updated')")
            )
            is None
        )
    applied = await optimize_query_indexes(
        storage.engine, database=database, apply=True
    )
    assert applied.tables["ix_sessions_main_user_updated"] == "created concurrently"
    assert applied.tables["ix_sessions_updated_at"] == "dropped redundant concurrently"
    again = await optimize_query_indexes(storage.engine, database=database, apply=True)
    assert set(again.tables.values()) == {"present"}


async def test_online_index_retirement_rejects_name_collision(storage):
    import pytest

    from egis_agentkit.storage._query_indexes import optimize_query_indexes

    async with storage.engine.begin() as conn:
        database = await conn.scalar(text("SELECT current_database()"))
        await conn.execute(
            text("CREATE INDEX ix_sessions_updated_at ON sessions(updated_at, user_id)")
        )
    with pytest.raises(ValueError, match="旧索引.*不一致"):
        await optimize_query_indexes(storage.engine, database=database, apply=True)
    async with storage.engine.connect() as conn:
        assert (
            await conn.scalar(text("SELECT to_regclass('ix_sessions_updated_at')"))
            is not None
        )


async def test_large_dependency_grants_use_scoped_active_index(storage):
    _, _, alice, _ = await identities(storage)
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO ak_resource_permission
            (id, principal_type, principal_id, resource_type, resource_id,
             resource_view_type, resource_owner_type, resource_owner_id,
             grant_source_type, origin_key, granted_by, granted_at)
            VALUES ('root','group','company','agent','root-agent','system','system',
                    'platform','manual','root','test',now())
        """)
        )
        await conn.execute(
            text("""
            INSERT INTO ak_resource_permission
            (id, principal_type, principal_id, resource_type, resource_id,
             resource_view_type, resource_owner_type, resource_owner_id,
             grant_source_type, origin_key, root_grant_id, parent_grant_id,
             agent_id, granted_by, granted_at)
            SELECT n::text, 'group',
                   CASE WHEN n % 5000=0 THEN 'company' ELSE 'unrelated-' || (n % 100)::text END,
                   'skill', CASE WHEN n % 5000=0 THEN 'wanted' ELSE 'asset-' || n::text END,
                   'system','system','platform','dependency',n::text,
                   'root','root','agent','test',now()
            FROM generate_series(1,50000) n
        """)
        )
        await conn.execute(text("ANALYZE ak_resource_permission"))
    with capture_selects(storage) as queries:
        result = await storage.access.authorization_grants(
            alice.user_id, alice.group_ids, {("skill", "wanted")}
        )
    assert len(result) == 10
    assert all(row["is_for_user"] for row in result)
    plan = await explain(storage, queries[0])
    report_plan("50k-dependency-grants", plan)
    assert any(
        node.get("Index Name") == "ix_ak_permission_active_resource_principal"
        for node in nodes(plan["Plan"])
    ), json.dumps(plan)
    assert not any(
        node["Node Type"] == "Seq Scan"
        and node.get("Relation Name") == "ak_resource_permission"
        for node in nodes(plan["Plan"])
    )
