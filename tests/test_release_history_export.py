"""Exercise the offline history exporter with legacy PG rows and a temporary NAS."""

from datetime import datetime
from pathlib import Path
from runpy import run_path

import pytest
from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.storage._sql._tables import MessageRow, SessionRow
from agentscope.message import Msg
from sqlalchemy import func, insert, select, text

from egis_agentkit.storage._message_bus import PostgresPrimitives

SCRIPT = (
    Path(__file__).resolve().parents[1] / "scripts/releases/1.0.3/export_history.py"
)


@pytest.fixture
def export():
    assert SCRIPT.is_file()
    return run_path(str(SCRIPT))["export_history"]


@pytest.fixture
async def legacy(storage):
    early, late = datetime(2025, 1, 1), datetime(2026, 1, 1)
    messages = [
        Msg(
            id=key,
            name="用户",
            role="user",
            content=[{"type": "text", "text": body}],
            metadata={"reply_id": "r", "nested": {"x": [False, "甲"]}},
            structured_output={"value": 20},
        )
        for key, body in (("z", "早"), ("a", "晚一"), ("b", "晚二"))
    ]
    async with storage.engine.begin() as conn:
        await conn.run_sync(MessageRow.__table__.create)
        await conn.execute(
            insert(SessionRow).values(
                user_id="alice",
                agent_id="agent",
                id="sid",
                source="user",
                created_at=early,
                updated_at=late,
                payload={},
            )
        )
        await conn.execute(
            insert(MessageRow),
            [
                dict(
                    session_id="sid",
                    msg_id=m.id,
                    created_at=early if m.id == "z" else late,
                    payload=m.model_dump(mode="json"),
                )
                for m in reversed(messages)
            ],
        )
        await conn.execute(
            insert(MessageRow).values(
                session_id="orphan",
                msg_id="z",
                created_at=early,
                payload=messages[0].model_dump(mode="json"),
            )
        )
    return messages


async def test_dry_run_apply_and_repeat_preserve_exact_order_and_source(
    storage, tmp_path, legacy, export
):
    plan = await export(storage)
    assert (
        plan["source_sessions"],
        plan["source_messages"],
        plan["orphan_messages"],
    ) == (1, 3, 1)
    assert plan["pending_sessions"] == 1 and plan["copied_sessions"] == 0
    assert not (tmp_path / "user").exists()
    result = await export(storage, apply=True)
    assert result["copied_sessions"] == 1
    assert await storage.message_history().read("alice", "sid") == legacy
    journal = tmp_path / "user/alice/session/history/sid/messages.jsonl"
    initial = journal.read_bytes()
    again = await export(storage, apply=True)
    assert again["identical_sessions"] == 1 and again["copied_sessions"] == 0
    assert journal.read_bytes() == initial
    async with storage.engine.connect() as conn:
        assert await conn.scalar(select(func.count()).select_from(MessageRow)) == 4


async def test_export_without_old_table_is_empty(storage, export):
    result = await export(storage, apply=True)
    assert not any(result.values())


@pytest.mark.parametrize("reset", [False, True])
async def test_export_refuses_conflicting_new_history(storage, legacy, export, reset):
    target = [] if reset else legacy[1:]
    await storage.message_history().replace("alice", "sid", target)
    assert (await export(storage))["conflicting_sessions"] == 1
    with pytest.raises(RuntimeError, match="conflict"):
        await export(storage, apply=True)
    assert await storage.message_history().read("alice", "sid") == target


async def test_export_refuses_live_session_lease(storage, legacy, export):
    await PostgresPrimitives(storage.engine).init_schema()
    async with storage.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO ak_bus_leases VALUES (:key,'worker',clock_timestamp()+interval '1 minute')"
            ),
            {"key": MessageBusKeys.session_lock("sid")},
        )
    with pytest.raises(RuntimeError, match="active session"):
        await export(storage, apply=True)
    assert await storage.message_history().read("alice", "sid") == []
    async with storage.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE ak_bus_leases SET expires_at=clock_timestamp()-interval '1 second'"
            )
        )
    assert (await export(storage, apply=True))["copied_sessions"] == 1


async def test_export_does_not_treat_boolean_and_numeric_metadata_as_identical(
    storage, legacy, export
):
    changed = list(legacy)
    changed[0] = changed[0].model_copy(
        update={
            "metadata": {"reply_id": "r", "nested": {"x": [0, "甲"]}},
        }
    )
    await storage.message_history().replace("alice", "sid", changed)
    assert (await export(storage))["conflicting_sessions"] == 1
