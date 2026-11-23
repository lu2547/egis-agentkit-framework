"""Exercise schedule storage against an isolated PostgreSQL schema."""

import asyncio
from datetime import datetime, timedelta, timezone
from pathlib import Path

from agentscope.app.storage import ScheduleRecord
from sqlalchemy import text

NOW = datetime(2026, 9, 21, 1, tzinfo=timezone.utc)
NEXT = NOW + timedelta(days=1)


def task(identifier="morning", **fields):
    return {
        "id": identifier,
        "user_id": "alice",
        "agent_id": "writer",
        "project_id": "workspace",
        "name": "Morning report",
        "description": "Summarize today's files",
        "model_key": "enterprise",
        "permission_mode": "dontAsk",
        "timezone": "Asia/Shanghai",
        "enabled": True,
        "stateful": True,
        "files": [{"area": "inputs", "path": "source.txt"}],
        "starts_on": "2026-09-01",
        "ends_on": None,
        "rule": {"type": "daily", "cron_expression": "0 9 * * *"},
        "next_run_at": NOW,
        "created_at": NOW,
        **fields,
    }


async def test_managed_rows_use_native_schedule_table_and_are_owner_scoped(storage):
    store = storage.schedules
    row = await store.create(task())
    assert row["created_at"] == NOW
    assert await store.get("bob", row["id"]) is None
    assert await store.update("bob", row["id"], {"name": "tampered"}) is None
    assert await store.list("bob") == []

    async with storage.engine.connect() as conn:
        assert (
            await conn.scalar(
                text(
                    "SELECT payload #>> '{data,name}' FROM schedules WHERE id = 'morning'"
                )
            )
            == "Morning report"
        )
    renamed = await store.update("alice", row["id"], {"name": "New name"})
    assert renamed["name"] == "New name"
    assert renamed["description"] == "Summarize today's files"
    assert renamed["next_run_at"] == NOW
    assert await store.list("alice") == [renamed]


async def test_managed_schedules_are_hidden_from_native_reads_and_deletes(storage):
    await storage.schedules.create(task())
    native = ScheduleRecord(
        id="native",
        user_id="alice",
        agent_id="writer",
        data={
            "name": "Native",
            "cron_expression": "0 9 * * *",
            "chat_model_config": {
                "type": "openai",
                "credential_id": "model-access",
                "model": "enterprise",
                "parameters": {},
            },
        },
    )
    await storage.upsert_schedule("alice", native)
    assert await storage.get_schedule("alice", "morning") is None
    assert [item.id for item in await storage.list_schedules("alice")] == ["native"]
    assert [item.id for item in await storage.list_all_schedules()] == ["native"]
    assert await storage.delete_schedule("alice", "morning") is False
    assert await storage.delete_schedule("alice", "native") is True
    assert await storage.schedules.get("alice", "morning") is not None


async def test_delete_is_all_or_none_and_keeps_run_history(storage):
    store = storage.schedules
    await store.create(task())
    await store.create(task("private", user_id="bob"))
    run = await store.enqueue_test("alice", "morning", NOW)
    assert await store.delete("alice", ["morning", "private"], NOW) == 0
    assert await store.get("alice", "morning") is not None
    assert await store.delete("alice", ["morning", "morning"], NOW) == 1
    assert await store.get("alice", "morning") is None
    assert await store.enqueue_test("alice", "morning", NOW) is None
    assert await store.due_ids(NOW) == ["private"]
    assert (await store.get_run("alice", run["id"]))[
        "schedule_name"
    ] == "Morning report"


async def test_overlapping_dispatch_advances_cursor_and_deduplicates_auto_time(storage):
    store = storage.schedules
    await store.create(task())
    assert await store.due_ids(NOW) == ["morning"]
    competing = await asyncio.gather(
        *(
            store.enqueue_due("morning", NOW, lambda row: (row["next_run_at"], NEXT))
            for _ in range(3)
        )
    )
    [run] = [row for row in competing if row is not None]
    assert run["trigger_type"] == "scheduled"
    assert run["scheduled_for"] == NOW
    assert (await store.get("alice", "morning"))["next_run_at"] == NEXT

    await store.update("alice", "morning", {"next_run_at": NOW})
    assert await store.enqueue_due("morning", NOW, lambda row: (NOW, NEXT)) is None
    assert (await store.get("alice", "morning"))["next_run_at"] == NEXT
    assert await store.list_runs("alice") == [run]


async def test_recovery_and_test_runs_have_distinct_trigger_semantics(storage):
    store = storage.schedules
    await store.create(task())
    later = NOW + timedelta(hours=2)
    recovery = await store.enqueue_due("morning", later, lambda row: (NOW, NEXT))
    manual = [await store.enqueue_test("alice", "morning", later) for _ in range(2)]
    assert recovery["trigger_type"] == "backfill"
    assert {run["id"] for run in manual + [recovery]} == {
        run["id"] for run in await store.list_runs("alice")
    }
    assert all(
        run["trigger_type"] == "test" and run["scheduled_for"] is None for run in manual
    )
    assert (await store.get("alice", "morning"))["next_run_at"] == NEXT


async def test_executor_claim_once_and_keeps_input_and_result_references(storage):
    store = storage.schedules
    await store.create(task())
    run = await store.enqueue_due("morning", NOW, lambda row: (NOW, NEXT))
    run_id = run["id"]
    await store.update(
        "alice",
        "morning",
        {
            "name": "Renamed",
            "project_id": "new-workspace",
            "description": "New prompt",
        },
    )
    [queued] = await store.list_queued()
    assert queued["id"] == run_id
    assert queued["agent_id"] == "writer"
    assert queued["input_snapshot"]["files"] == [
        {"area": "inputs", "path": "source.txt"}
    ]
    assert queued["input_snapshot"]["description"] == "Summarize today's files"
    assert queued["project_id"] == "workspace"
    assert queued["schedule_name"] == "Morning report"

    competing = await asyncio.gather(*(store.claim_run(run_id, NOW) for _ in range(3)))
    [running] = [row for row in competing if row is not None]
    assert running["user_id"] == "alice"
    assert running["status"] == "running"
    assert running["started_at"] == NOW
    assert await store.list_queued() == []
    assert not await store.delete_run("alice", run_id, NOW)
    await store.bind_run(run_id, "session-1", "reply-1")
    finished = NOW + timedelta(seconds=3)
    await store.finish_run(run_id, finished, status="succeeded", summary="Morning done")
    await store.finish_run(run_id, NEXT, status="failed", error_code="duplicate")
    [done] = await store.list_runs(
        "alice", schedule_id="morning", status="succeeded", q="MORNING"
    )
    assert done["session_id"] == "session-1"
    assert done["reply_id"] == "reply-1"
    assert done["summary"] == "Morning done"
    assert done["finished_at"] == finished
    assert done["error_code"] is None
    assert await store.list_runs("bob", q="morning") == []
    assert await store.get_run("bob", run_id) is None
    assert await store.archive("bob", run_id, True, finished) is None
    assert not await store.delete_run("bob", run_id, finished)

    assert (await store.archive("alice", run_id, True, finished))[
        "archived_at"
    ] == finished
    assert await store.list_runs("alice") == []
    assert (await store.list_runs("alice", archived=True))[0]["id"] == run_id
    assert (await store.archive("alice", run_id, False, finished))[
        "archived_at"
    ] is None
    assert await store.delete_run("alice", run_id, finished)
    assert await store.get_run("alice", run_id) is None
    assert (await store.last_stateful_run("morning", "workspace"))["id"] == run_id
    assert await store.last_stateful_run("morning", "new-workspace") is None


async def test_running_expiry_fails_old_claim_without_replaying(storage):
    store = storage.schedules
    await store.create(task())
    first = await store.enqueue_test("alice", "morning", NOW)
    second = await store.enqueue_test("alice", "morning", NOW)
    await store.claim_run(first["id"], NOW)
    expire_at = NOW + timedelta(hours=1)
    assert await store.expire_running(NOW + timedelta(minutes=1), expire_at) == 1
    expired = await store.get_run("alice", first["id"])
    assert expired["status"] == "failed"
    assert expired["error_code"] == "interrupted"
    assert expired["finished_at"] == expire_at
    assert [row["id"] for row in await store.list_queued()] == [second["id"]]


async def test_project_deleting_pauses_owned_schedules_and_preserves_history(storage):
    store = storage.schedules
    await store.create(task())
    await store.create(task("different-project", project_id="other"))
    await store.create(task("different-owner", user_id="bob"))
    run = await store.enqueue_test("alice", "morning", NOW)
    async with storage.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO ak_projects (id, owner_id, project_name, created_by, updated_by)"
                " VALUES ('workspace', 'alice', 'Workspace', 'alice', 'alice')"
            )
        )
    await storage.projects.mark_deleting("alice", "workspace")
    paused = await store.get("alice", "morning")
    assert paused["enabled"] is False
    assert paused["next_run_at"] is None
    assert await store.due_ids(NOW) == ["different-owner", "different-project"]
    assert await store.get_run("alice", run["id"]) is not None


async def test_schedule_release_preserves_legacy_rows_and_can_be_rerun(storage):
    script = (
        Path(__file__).resolve().parents[1] / "scripts/releases/1.0.3/01_schedules.sql"
    )
    async with storage.engine.connect() as conn:
        pg = (await conn.get_raw_connection()).driver_connection
        await pg.execute(
            """
            DROP TABLE ak_schedule_runs;
            ALTER TABLE schedules
                DROP COLUMN project_id, DROP COLUMN next_run_at, DROP COLUMN deleted_at;
            INSERT INTO schedules(id, user_id, agent_id, created_at, updated_at, payload)
            VALUES ('legacy', 'alice', 'writer', now(), now(),
                    '{"data":{"name":"Legacy"}}');
            """
        )
        await pg.execute(script.read_text())
        await pg.execute(script.read_text())
        assert (
            await pg.fetchval(
                "SELECT payload #>> '{data,name}' FROM schedules WHERE id = 'legacy'"
            )
            == "Legacy"
        )
    assert await storage.schedules.list("alice") == []
    assert (await storage.schedules.create(task()))["id"] == "morning"


async def test_one_schedule_does_not_run_twice_concurrently(storage):
    store = storage.schedules
    await store.create(task())
    await store.create(task("another"))
    first = await store.enqueue_test("alice", "morning", NOW)
    second = await store.enqueue_test("alice", "morning", NOW)
    independent = await store.enqueue_test("alice", "another", NOW)
    attempts = await asyncio.gather(
        store.claim_run(first["id"], NOW),
        store.claim_run(second["id"], NOW),
    )
    (claimed,) = [row for row in attempts if row is not None]
    queued_id = ({first["id"], second["id"]} - {claimed["id"]}).pop()
    assert [row["id"] for row in await store.list_queued()] == [independent["id"]]
    assert await store.claim_run(queued_id, NOW) is None
    assert await store.claim_run(independent["id"], NOW) is not None
    await store.finish_run(claimed["id"], NEXT, status="succeeded")
    assert (await store.claim_run(queued_id, NEXT))["status"] == "running"


async def test_completion_watermarks_match_owner_schedule_project_and_finished_time(
    storage,
):
    """A session id alone must not expose another schedule's completion."""
    from sqlalchemy import update

    from egis_agentkit.storage._schedule import schedule_runs

    store = storage.schedules
    await store.create(task())
    await store.create(task("another"))
    await store.create(task("foreign", user_id="bob"))
    sources = {"session": ("morning", "workspace")}
    assert await store.latest_completed_run_ids("alice", sources) == {}
    first = await store.enqueue_test("alice", "morning", NOW)
    await store.claim_run(first["id"], NOW)
    await store.bind_run(first["id"], "session")
    assert await store.latest_completed_run_ids("alice", sources) == {}
    await store.finish_run(first["id"], NEXT, status="succeeded")

    # A later-created execution which finished earlier cannot lower the watermark.
    second = await store.enqueue_test("alice", "morning", NEXT)
    await store.claim_run(second["id"], NEXT)
    await store.bind_run(second["id"], "session")
    await store.finish_run(second["id"], NOW, status="failed")
    assert await store.latest_completed_run_ids("alice", sources) == {
        "session": first["id"]
    }
    for owner, schedule_id in [("alice", "another"), ("bob", "foreign")]:
        run = await store.enqueue_test(owner, schedule_id, NEXT)
        await store.claim_run(run["id"], NEXT)
        await store.bind_run(run["id"], "session")
        await store.finish_run(
            run["id"], NEXT + timedelta(seconds=1), status="succeeded"
        )
    assert await store.latest_completed_run_ids(
        "alice", {"session": ("foreign", "workspace")}
    ) == {}
    assert await store.latest_completed_run_ids(
        "alice", {"session": ("morning", "other-project")}
    ) == {}
    assert await store.latest_completed_run_ids("bob", sources) == {}

    incomplete = await store.enqueue_test("alice", "morning", NEXT)
    await store.claim_run(incomplete["id"], NEXT)
    await store.bind_run(incomplete["id"], "session")
    async with storage.engine.begin() as conn:
        await conn.execute(
            update(schedule_runs)
            .where(schedule_runs.c.id == incomplete["id"])
            .values(status="succeeded")
        )
    assert await store.latest_completed_run_ids("alice", sources) == {
        "session": first["id"]
    }
