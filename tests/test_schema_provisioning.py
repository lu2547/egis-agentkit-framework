"""验证首版建表范围和清理保护。"""

import pytest
from sqlalchemy import inspect, text
from sqlalchemy.exc import DBAPIError
from test_domain import agent_changes

from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.storage import tables


@pytest.mark.asyncio
async def test_schema_creates_only_native_tables_required_by_workbench(storage):
    async with storage.engine.connect() as connection:
        names = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
        indexes = await connection.run_sync(
            lambda conn: inspect(conn).get_indexes("sessions")
        )
    assert set(names) == set(tables.metadata.tables) | {
        "agents",
        "credentials",
        "messages",
        "schedules",
        "sessions",
        "teams",
    }
    assert "ak_project_sessions" not in names
    assert {"ak_project_file_changes", "ak_project_file_fingerprints"} <= set(names)
    assert any(index["column_names"] == ["project_id", "user_id"] for index in indexes)


async def test_project_schema_upgrade_preserves_workspace_data_and_checks_legacy_links(
    storage, tmp_path
):
    from test_domain import expert, identities

    from egis_agentkit.service.session._session import SessionService
    from egis_agentkit.service.team._definition import TeamDefinition
    from egis_agentkit.storage._schema import cleanup_schema, upgrade_schema

    _, _, alice, _ = await identities(storage)
    definition = await AgentManager(storage).add_definition(alice, expert())
    sessions = SessionService(storage, tmp_path)
    session = await sessions.add_session(alice, definition["id"], "private")
    sid, project = session["session_id"], session["project_id"]
    team = await TeamDefinition(storage).save(
        alice,
        {
            "team_name": "writers",
            "team_display_name": "写作团",
            "orchestrator_config": {
                "agent_name": "coordinator",
                "agent_display_name": "主理人",
            },
            "members": [
                {"definition_id": definition["id"], "resource_view_type": "private"}
            ],
        },
    )
    team_session = await sessions.add_session(
        alice, team["id"], "private", project_id=project
    )
    leader = await sessions.get_session_record(alice, team_session["session_id"])
    native_team = await storage.get_team(alice.user_id, leader.team_id)
    member = native_team.data.members[0]
    worker = await storage.get_session(
        alice.user_id, member.agent_id, member.session_id
    )
    legacy_sessions = [
        (sid, definition["id"]),
        (
            leader.id,
            leader.state.middle_context["egis_agentkit"]["agent_definition_id"],
        ),
        (worker.id, definition["id"]),
    ]
    async with storage.engine.begin() as connection:
        database = await connection.scalar(text("SELECT current_database()"))
        await connection.execute(text("ALTER TABLE sessions DROP COLUMN project_id"))
        await connection.execute(
            text("""
            UPDATE sessions SET payload = jsonb_set(
                payload::jsonb, '{state,middle_context,egis_agentkit}',
                (payload::jsonb #> '{state,middle_context,egis_agentkit}') - 'agent_definition_id'
                    || jsonb_build_object('agent_node_key', CAST(:node AS text))
            ) WHERE id=:sid
        """),
            [
                {"node": f"agent:private:{agent_definition_id}", "sid": session_id}
                for session_id, agent_definition_id in legacy_sessions
            ],
        )
        await connection.execute(
            text("ALTER TABLE ak_project_file_changes RENAME TO ak_file_changes")
        )
        await connection.execute(
            text(
                "ALTER TABLE ak_project_file_fingerprints RENAME TO ak_file_fingerprints"
            )
        )
        await connection.execute(
            text(
                "INSERT INTO ak_file_changes (project_id, area, path, change_type) VALUES (:pid, 'uploads', 'old.txt', 'created')"
            ),
            {"pid": project},
        )
        await connection.execute(
            text(
                "INSERT INTO ak_file_fingerprints VALUES (:pid, 'uploads', 'old.txt', 'abc', 3)"
            ),
            {"pid": project},
        )
        await connection.execute(
            text(
                "CREATE TABLE ak_project_sessions (session_id varchar PRIMARY KEY, project_id varchar NOT NULL)"
            )
        )
        await connection.execute(
            text("INSERT INTO ak_project_sessions VALUES (:sid, 'other-project')"),
            {"sid": sid},
        )

    with pytest.raises(ValueError, match="upgrade-db"):
        await storage.init_schema()
    preview = await upgrade_schema(storage.engine)
    assert preview.tables["ak_file_changes"] == "pending"
    assert preview.tables["Session 绑定"] == "pending"
    assert preview.tables["ak_project_sessions"] == "inconsistent"
    with pytest.raises(ValueError, match="目标数据库"):
        await upgrade_schema(storage.engine, database="other-database", apply=True)
    with pytest.raises(ValueError, match="ak_project_sessions"):
        await upgrade_schema(storage.engine, database=database, apply=True)
    async with storage.engine.begin() as connection:
        assert await connection.scalar(text("SELECT to_regclass('ak_file_changes')"))
        assert (
            await connection.scalar(
                text("SELECT to_regclass('ak_project_file_changes')")
            )
            is None
        )
        await connection.execute(
            text("UPDATE ak_project_sessions SET project_id=:pid"), {"pid": project}
        )

    upgraded = await upgrade_schema(storage.engine, database=database, apply=True)
    assert upgraded.tables["Session 绑定"] == "upgraded"
    assert upgraded.tables["ak_file_changes"] == "renamed"
    repeated = await upgrade_schema(storage.engine, database=database, apply=True)
    assert repeated.tables["Session 绑定"] == "current"
    await storage.init_schema()
    async with storage.engine.connect() as connection:
        assert (
            await connection.scalar(
                text("SELECT project_id FROM sessions WHERE id=:sid"), {"sid": sid}
            )
            == project
        )
        for table in (tables.file_fingerprints, tables.file_changes):
            assert (
                await connection.scalar(table.select().with_only_columns(table.c.path))
                == "old.txt"
            )
        names = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
        assert {"ak_file_changes", "ak_file_fingerprints"}.isdisjoint(names)

    assert (await cleanup_schema(storage.engine)).tables[
        "ak_project_sessions"
    ] == "verified"
    applied = await cleanup_schema(storage.engine, database=database, apply=True)
    assert applied.tables["ak_project_sessions"] == "dropped"
    assert (await SessionService(storage, tmp_path).get_session(alice, sid))[
        "project_id"
    ] == project
    record = await storage.get_session(alice.user_id, session["agent_id"], sid)
    binding = record.state.middle_context["egis_agentkit"]
    assert binding["agent_definition_id"] == definition["id"]
    assert "agent_node_key" not in binding
    worker = await storage.get_session(
        alice.user_id, member.agent_id, member.session_id
    )
    binding = worker.state.middle_context["egis_agentkit"]
    assert binding["definition_id"] == team["id"]
    assert binding["agent_definition_id"] == definition["id"]
    assert worker.team_id == leader.team_id
    assert (await sessions.get_session(alice, leader.id))["project_id"] == project


async def test_upgrade_rejects_mixed_agent_types_and_preserves_remote_runs(storage):
    import json

    from test_domain import expert, identities
    from test_remote_agents import config

    from egis_agentkit.storage._schema import upgrade_schema

    _, _, alice, _ = await identities(storage)
    definitions = AgentManager(storage)
    draft = expert()
    original = await definitions.add_definition(alice, draft)
    draft.id, draft.owner_version = original["id"], original["owner_version"]
    await definitions.update_definition(alice, (draft).id, agent_changes(draft))
    remote = config().model_dump(mode="json")
    async with storage.engine.begin() as connection:
        database = await connection.scalar(text("SELECT current_database()"))
        await connection.execute(
            text("""
            ALTER TABLE ak_agent_definition_summary DROP CONSTRAINT ak_agent_definition_summary_agent_type_check,
            ADD CONSTRAINT ak_agent_definition_summary_agent_type_check
            CHECK (agent_type IN ('personal', 'expert', 'orchestrator'))
        """)
        )
        await connection.execute(
            text("""
            UPDATE ak_agent_definition_detail SET payload = payload || CAST(:remote AS jsonb)
            WHERE definition_id = :id AND version = 2
        """),
            {
                "id": draft.id,
                "remote": json.dumps(
                    {"execution_type": "remote", "remote_config": remote}
                ),
            },
        )
        await connection.execute(
            text("ALTER TABLE ak_agent_remote_runs RENAME TO ak_remote_agent_runs")
        )
        await connection.execute(
            text("""
            INSERT INTO ak_remote_agent_runs
                (run_id, conversation_id, connection_id, connection_config, request_digest, deadline)
            VALUES ('pending', 'session', 'finance', CAST(:remote AS jsonb), 'digest', now())
        """),
            {"remote": json.dumps(remote)},
        )

    preview = await upgrade_schema(storage.engine)
    assert preview.tables["Agent 类型"] == "inconsistent"
    with pytest.raises(ValueError, match="Agent 类型"):
        await upgrade_schema(storage.engine, database=database, apply=True)
    async with storage.engine.begin() as connection:
        assert (
            await connection.scalar(text("SELECT run_id FROM ak_remote_agent_runs"))
            == "pending"
        )
        await connection.execute(
            text("""
            UPDATE ak_agent_definition_detail AS first SET payload = second.payload
            FROM ak_agent_definition_detail AS second
            WHERE first.definition_id = :id AND first.version = 1
              AND second.definition_id = first.definition_id AND second.version = 2
        """),
            {"id": draft.id},
        )

    result = await upgrade_schema(storage.engine, database=database, apply=True)
    assert result.tables["Agent 类型"] == "upgraded"
    assert result.tables["ak_remote_agent_runs"] == "renamed"
    async with storage.engine.connect() as connection:
        assert (
            await connection.scalar(text("SELECT run_id FROM ak_agent_remote_runs"))
            == "pending"
        )
    for version in (1, 2):
        definition = await storage.definitions.get_definition(draft.id, version=version)
        assert definition["agent_type"] == "remote"
        assert definition["payload"]["remote_config"]["endpoint"] == remote["endpoint"]
        assert "execution_type" not in definition["payload"]


@pytest.mark.asyncio
async def test_cleanup_previews_and_rejects_nonempty_retired_tables(storage):
    from egis_agentkit.storage._schema import cleanup_schema

    async with storage.engine.begin() as connection:
        await connection.execute(
            text("CREATE TABLE ak_tool_installations (id integer)")
        )
        await connection.execute(text("CREATE TABLE channels (id integer)"))
        await connection.execute(text("INSERT INTO channels VALUES (1)"))
        database = await connection.scalar(text("SELECT current_database()"))

    preview = await cleanup_schema(storage.engine)
    assert preview.tables["channels"] == "contains-data"
    assert preview.tables["ak_tool_installations"] == "empty"
    with pytest.raises(ValueError, match="数据库"):
        await cleanup_schema(storage.engine, database="wrong-database", apply=True)
    with pytest.raises(ValueError, match="channels"):
        await cleanup_schema(storage.engine, database=database, apply=True)

    async with storage.engine.begin() as connection:
        assert await connection.scalar(
            text("SELECT to_regclass('ak_tool_installations')")
        )
        await connection.execute(text("DELETE FROM channels"))
        await connection.execute(
            text("CREATE VIEW channel_view AS SELECT id FROM channels")
        )

    with pytest.raises(DBAPIError) as dependency:
        await cleanup_schema(storage.engine, database=database, apply=True)
    assert dependency.value.orig.sqlstate == "2BP01"
    async with storage.engine.begin() as connection:
        assert await connection.scalar(
            text("SELECT to_regclass('ak_tool_installations')")
        )
        await connection.execute(text("DROP VIEW channel_view"))

    result = await cleanup_schema(storage.engine, database=database, apply=True)
    assert result.tables["channels"] == "dropped"
    assert result.tables["ak_tool_installations"] == "dropped"
    async with storage.engine.connect() as connection:
        names = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
    assert {"agents", "schedules", "ak_resource_assets"} <= set(names)
    assert {"channels", "ak_tool_installations"}.isdisjoint(names)

    result = await cleanup_schema(storage.engine, database=database, apply=True)
    assert result.tables["channels"] == "absent"


async def test_bus_table_rename_preserves_queue_rows(storage):
    from egis_agentkit.storage._message_bus import PostgresPrimitives, rename_bus_tables

    async with storage.engine.begin() as conn:
        await conn.execute(
            text("""
            CREATE TABLE egis_bus_keys (
                kind text NOT NULL, key text NOT NULL, seq bigint NOT NULL,
                expires_at timestamptz, PRIMARY KEY(kind,key)
            )
        """)
        )
        await conn.execute(
            text("INSERT INTO egis_bus_keys VALUES ('queue','old-key',7,NULL)")
        )
        await rename_bus_tables(conn)
        assert await conn.scalar(text("SELECT to_regclass('egis_bus_keys')")) is None
    bus = PostgresPrimitives(storage.engine)
    await bus.init_schema()
    async with storage.engine.connect() as conn:
        assert (
            await conn.scalar(text("SELECT seq FROM ak_bus_keys WHERE key='old-key'"))
            == 7
        )
