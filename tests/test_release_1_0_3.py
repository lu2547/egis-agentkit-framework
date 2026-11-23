"""Run the release SQL in disposable schemas and retain pre-release records."""

from pathlib import Path
from uuid import uuid4

import asyncpg
import pytest

from test_initialization_pg import schema_shape

ROOT = Path(__file__).resolve().parents[1]
RELEASE = ROOT / "scripts/releases/1.0.3"


async def test_release_upgrade_matches_fresh_schema_and_preserves_records(storage):
    schemas = ["ak_release_test_" + uuid4().hex for _ in range(2)]
    async with storage.engine.connect() as connection:
        raw = await connection.get_raw_connection()
        pg = raw.driver_connection
        original = await pg.fetchval("SELECT current_schema()")
        try:
            for schema in schemas:
                await pg.execute(f'CREATE SCHEMA "{schema}"')
                await pg.execute(f'SET search_path TO "{schema}"')
                await pg.execute((ROOT / "scripts/init/pg.sql").read_text())
            fresh, upgraded = schemas
            # Restore the affected tables to the 1.0.2 release baseline.
            # Query indexes are already present, as required by 1.0.2/01.
            await pg.execute("""
                DROP TABLE ak_schedule_runs;
                ALTER TABLE schedules DROP COLUMN project_id,
                    DROP COLUMN next_run_at, DROP COLUMN deleted_at;
                ALTER TABLE ak_agent_personal_resources
                    DROP CONSTRAINT ak_agent_personal_resources_resource_type_check,
                    ADD CONSTRAINT ak_agent_personal_resources_resource_type_check
                        CHECK (resource_type IN ('skill','mcp'));
                INSERT INTO schedules
                    (id, user_id, agent_id, created_at, updated_at, payload)
                VALUES ('native', 'alice', 'personal', now(), now(),
                        '{"data":{"name":"Existing native schedule"}}');
                INSERT INTO ak_agent_definition_summary
                    (id, owner_type, owner_id, agent_type, agent_name,
                     owner_version, created_by, updated_by)
                VALUES ('personal', 'user', 'alice', 'personal', 'Assistant',
                        1, 'alice', 'alice');
                INSERT INTO ak_agent_personal_resources
                    (user_id, agent_id, resource_type, resource_id,
                     is_enabled, created_by, updated_by)
                VALUES ('alice', 'personal', 'skill', 'existing-skill', true,
                        'alice', 'alice'),
                       ('alice', 'personal', 'mcp', 'existing-mcp', false,
                        'alice', 'alice');
            """)
            before = await pg.fetch("SELECT * FROM ak_agent_personal_resources ORDER BY resource_type")
            native = await pg.fetchrow("SELECT * FROM schedules WHERE id = 'native'")
            scripts = sorted(path for path in RELEASE.glob("*.sql") if "verify" not in path.name)
            for _ in range(2):
                for script in scripts:
                    await pg.execute(script.read_text())
                    assert not pg.is_in_transaction(), script.name
                assert await schema_shape(pg, upgraded) == await schema_shape(pg, fresh)
                assert await pg.fetch(
                    "SELECT * FROM ak_agent_personal_resources ORDER BY resource_type"
                ) == before
                row = dict(await pg.fetchrow("SELECT * FROM schedules WHERE id = 'native'"))
                for name in ("project_id", "next_run_at", "deleted_at"):
                    assert row.pop(name) is None
                assert row == dict(native)

            await pg.execute("""
                INSERT INTO ak_agent_personal_resources
                    (user_id, agent_id, resource_type, resource_id,
                     is_enabled, created_by, updated_by)
                VALUES ('alice', 'personal', 'tool', 'KnowledgeBaseSearch',
                        true, 'alice', 'alice');
                INSERT INTO ak_schedule_runs
                    (id, schedule_id, schedule_name, project_id, trigger_type,
                     scheduled_for, status, created_at, input_snapshot)
                VALUES ('automatic', 'native', 'Scheduled', 'project', 'scheduled',
                        '2026-11-23T01:00:00Z', 'queued', now(), '{}'),
                       ('test-1', 'native', 'Test', 'project', 'test',
                        NULL, 'queued', now(), '{}'),
                       ('test-2', 'native', 'Test', 'project', 'test',
                        NULL, 'queued', now(), '{}');
            """)
            invalid_runs = [
                ("backfill", "2026-11-23T01:00:00Z", "queued", asyncpg.UniqueViolationError),
                ("scheduled", None, "queued", asyncpg.CheckViolationError),
                ("test", "2026-11-23T02:00:00Z", "queued", asyncpg.CheckViolationError),
                ("other", "2026-11-23T02:00:00Z", "queued", asyncpg.CheckViolationError),
                ("test", None, "other", asyncpg.CheckViolationError),
            ]
            for trigger, scheduled_for, status, error in invalid_runs:
                with pytest.raises(error):
                    await pg.execute("""
                        INSERT INTO ak_schedule_runs
                            (id, schedule_id, schedule_name, project_id,
                             trigger_type, scheduled_for, status, created_at, input_snapshot)
                        VALUES ('invalid', 'native', 'Invalid', 'project',
                                $1, $2::text::timestamptz, $3, now(), '{}')
                    """, trigger, scheduled_for, status)
            with pytest.raises(asyncpg.ForeignKeyViolationError):
                await pg.execute("""
                    INSERT INTO ak_schedule_runs
                        (id, schedule_id, schedule_name, project_id, trigger_type,
                         status, created_at, input_snapshot)
                    VALUES ('orphan', 'missing', 'Invalid', 'project', 'test',
                            'queued', now(), '{}')
                """)
            rows = await pg.fetch((RELEASE / "03_verify_schema.sql").read_text())
            assert len(rows) == 38
            for row in rows:
                assert row["passed"], dict(row)
        finally:
            if pg.is_in_transaction():
                await pg.execute("ROLLBACK")
            await pg.execute(f'SET search_path TO "{original}"')
            for schema in schemas:
                await pg.execute(f'DROP SCHEMA IF EXISTS "{schema}" CASCADE')
