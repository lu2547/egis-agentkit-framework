from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

from agentscope.app._bus_ops import publish_session_event
from agentscope.app.message_bus import MessageBusKeys
from agentscope.event import (
    ReplyEndEvent,
    ReplyStartEvent,
    ToolCallStartEvent,
    ToolResultEndEvent,
)
from redis_helpers import redis_config as redis_config

from egis_agentkit.core.session._context import session_execution_context
from egis_agentkit.service.session._message_bus_override import SessionMessageBus


async def test_worker_start_and_finish_times_survive_progress_updates_and_reload():
    async with SessionMessageBus(deployment_mode="standalone") as bus:
        progress = bus.collaborator_progress
        start = ReplyStartEvent(
            session_id="worker", reply_id="first", name="skills-assistant",
            created_at="2026-11-23T01:00:00Z",
        )
        finish = ReplyEndEvent(
            session_id="worker", reply_id="first",
            created_at="2026-11-23T01:01:00Z",
        )
        for event in (
            start,
            ToolCallStartEvent(
                reply_id="first", tool_call_id="write", tool_call_name="CreateSkill"
            ),
            finish,
        ):
            await progress.observe(
                "parent", "worker", "expert", "Skills 助手", event.model_dump(mode="json")
            )
        (saved,) = await progress.list("parent")
        assert saved["started_at"] == start.model_dump(mode="json")["created_at"]
        assert saved["finished_at"] == finish.model_dump(mode="json")["created_at"]
        assert saved["status"] == "completed"

        next_start = ReplyStartEvent(
            session_id="worker", reply_id="second", name="skills-assistant",
            created_at="2026-11-23T02:00:00Z",
        )
        await progress.observe(
            "parent", "worker", "expert", "Skills 助手", next_start.model_dump(mode="json")
        )
        (saved,) = await progress.list("parent")
        assert saved["started_at"] == next_start.model_dump(mode="json")["created_at"]
        assert "finished_at" not in saved


async def test_worker_progress_reaches_the_parent_after_its_reply_ends():
    async with SessionMessageBus(deployment_mode="standalone") as bus:
        await publish_session_event(
            bus,
            "parent",
            ReplyEndEvent(session_id="parent", reply_id="main").model_dump(mode="json"),
        )
        scope = SimpleNamespace(
            session_id="worker",
            agent_id="expert",
            binding={"collaboration_parent_session_id": "parent"},
            definition={"agent_display_name": "Skills 助手"},
        )
        token = session_execution_context.set(scope)
        try:
            for event in (
                ReplyStartEvent(
                    session_id="worker", reply_id="child", name="skills-assistant"
                ),
                ToolCallStartEvent(
                    reply_id="child", tool_call_id="write", tool_call_name="CreateSkill"
                ),
                ToolResultEndEvent(
                    reply_id="child", tool_call_id="write", state="success"
                ),
                ReplyEndEvent(session_id="worker", reply_id="child"),
            ):
                await publish_session_event(
                    bus, "worker", event.model_dump(mode="json")
                )
        finally:
            session_execution_context.reset(token)

        events = await bus.log_read(MessageBusKeys.session_events("parent"))
        assert events[0][1]["type"] == "REPLY_END"
        updates = [
            event["value"]
            for _, event in events
            if event.get("name") == "collaborator_progress"
        ]
        assert [update["status"] for update in updates] == [
            "running",
            "running",
            "running",
            "completed",
        ]
        assert updates[-1]["worker_agent_id"] == "expert"
        assert updates[-1]["activities"] == [
            {"id": "write", "name": "CreateSkill", "state": "success"}
        ]
        assert await bus.collaborator_progress.list("parent") == [updates[-1]]


async def test_postgres_progress_preserves_worker_state_when_first_dispatch_arrives_late(
    storage, monkeypatch, redis_config
):
    bus = SessionMessageBus(
        deployment_mode="cluster", engine=storage.engine, redis=redis_config
    )
    await bus.init_schema()
    await bus._start_replay()
    monkeypatch.setattr(bus, "_kafka", lambda: SimpleNamespace(publish=AsyncMock()))
    parent = "test-parent-" + uuid4().hex
    progress = bus.collaborator_progress
    member = {
        "worker_session_id": "worker",
        "native_agent_id": "expert",
        "display_name": "Skills 助手",
    }
    start = ReplyStartEvent(
        session_id="worker", reply_id="child", name="skills-assistant"
    )
    try:
        await progress.observe(
            parent, "worker", "expert", "Skills 助手", start.model_dump(mode="json")
        )
        await progress.dispatched(parent, member)
        (snapshot,) = await progress.list(parent)
        assert snapshot["status"] == "running"
        assert snapshot["revision"] == 1
        await progress.forget(parent, "worker")
    finally:
        await bus.log_trim(MessageBusKeys.session_events(parent))
        await bus.aclose()
