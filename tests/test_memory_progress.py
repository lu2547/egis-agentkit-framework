"""Exercise synchronous memory write-back progress without running ReMe or a model."""

import asyncio
import importlib.util
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace

import httpx
import pytest
import pytest_asyncio
from agentscope.app.message_bus import MessageBusKeys
from agentscope.event import ReplyEndEvent, ReplyStartEvent
from agentscope.message import AssistantMsg, TextBlock, UserMsg
from agentscope.middleware import ReMeMiddleware
from egis_agentkit_app.bootstrap import build_app
from identity_helpers import configure_test_issuer, signed_token
from test_app_http import Config, app_snapshot
from test_domain import identities

from egis_agentkit.client.nacos import NacosBootstrap, NacosClient
from egis_agentkit.service.session._message_bus_override import SessionMessageBus

EVENT = "session_execution_phase"
FIELD = "session_execution_phase:current"


async def saved_phase(bus, sid):
    raw = await bus.registry_get(MessageBusKeys.projection_namespace(sid), FIELD)
    return json.loads(raw) if raw is not None else None


async def phase_events(bus, sid):
    events = await bus.log_read(MessageBusKeys.session_events(sid))
    return [event["value"] for _, event in events if event.get("name") == EVENT]


async def test_new_session_lock_clears_crashed_memory_phase_before_slow_preparation():
    sid = "preparing-session"
    preparing, proceed = asyncio.Event(), asyncio.Event()

    @asynccontextmanager
    async def bind(session_id):
        assert session_id == sid
        preparing.set()
        await proceed.wait()
        yield

    async with SessionMessageBus(deployment_mode="standalone") as bus:
        bus.coordinator = SimpleNamespace(bind=bind)
        await bus.registry_set(
            MessageBusKeys.projection_namespace(sid),
            FIELD,
            json.dumps({"session_id": sid, "reply_id": "crashed", "phase": "memory"}),
        )

        async def execute():
            async with bus.acquire_lock(MessageBusKeys.session_lock(sid)):
                pass

        run = asyncio.create_task(execute())
        try:
            await asyncio.wait_for(preparing.wait(), 3)
            assert await bus.is_locked(MessageBusKeys.session_lock(sid))
            assert await saved_phase(bus, sid) is None
        finally:
            proceed.set()
            await asyncio.wait_for(run, 3)


@pytest_asyncio.fixture
async def memory_app(storage, tmp_path, monkeypatch):
    original = importlib.util.find_spec
    monkeypatch.setattr(
        importlib.util,
        "find_spec",
        lambda name: object() if name == "reme" else original(name),
    )
    configure_test_issuer(monkeypatch)
    monkeypatch.setenv("TEST_DOWNLOAD", "test-memory-download-key-longer-than-32-bytes")
    monkeypatch.setenv("TEST_MODEL_KEY", "test-only")
    _, _, alice, bob = await identities(storage)
    config = Config(
        app_snapshot(
            tmp_path,
            project={
                "upload": {"max_bytes": 20_971_520},
                "memory": {"is_enabled": True},
                "download": {"secret_env": "TEST_DOWNLOAD"},
            },
        )
    )
    async with NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos:
        app = await build_app(
            config, nacos, engine=storage.engine, manage_logging=False
        )
        kit = app.state.egis_kit
        async with kit._storage, kit.bus:
            project = await kit.services.projects.default(alice)
            personal = await kit.services.personal.get_or_add_definition(alice)
            task = await kit.services.sessions.add_session(
                alice, personal["id"], "private", project_id=project["id"]
            )
            memory = await kit._memory.for_project(alice, project["id"])
            try:
                yield app, memory, kit.bus, task["session_id"], alice, bob
            finally:
                await kit._memory.close()


async def turn(memory, sid, rid, events, *, before_answer=None):
    state = SimpleNamespace(session_id=sid, reply_id="previous-reply", context=[])
    agent = SimpleNamespace(name="personal", state=state)
    message = UserMsg(name="user", content=[TextBlock(text="整理项目进展")])

    async def reply(**kwargs):
        if before_answer is not None:
            await before_answer()
        state.reply_id = rid
        state.context.extend(
            [
                message,
                AssistantMsg(
                    id=rid, name="personal", content=[TextBlock(text="已整理")]
                ),
            ]
        )
        yield ReplyStartEvent(session_id=sid, reply_id=rid, name="personal")
        yield ReplyEndEvent(session_id=sid, reply_id=rid)

    async for event in memory.on_reply(agent, {"inputs": message}, reply):
        events.append(event)


async def test_slow_writeback_keeps_progress_after_reply_end_and_refresh_is_owner_scoped(
    memory_app, monkeypatch
):
    app, memory, bus, sid, alice, bob = memory_app
    entered, release = asyncio.Event(), asyncio.Event()

    async def job(self, name, **kwargs):
        if name == "auto_memory":
            entered.set()
            await release.wait()
        return SimpleNamespace(metadata={})

    monkeypatch.setattr(ReMeMiddleware, "_run_job", job)
    assert await bus.try_lock(MessageBusKeys.session_lock(sid))
    events = []
    run = asyncio.create_task(turn(memory, sid, "new-reply", events))
    try:
        await asyncio.wait_for(entered.wait(), 3)
        assert isinstance(events[-1], ReplyEndEvent)
        assert not run.done(), "memory must remain synchronously awaited after ReplyEnd"
        expected = {"session_id": sid, "reply_id": "new-reply", "phase": "memory"}
        assert await saved_phase(bus, sid) == expected
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            path = f"/manager/tasks/{sid}"
            owner = {"Authorization": "Bearer " + signed_token(alice.user_id)}
            for _ in range(2):
                response = await client.get(path, headers=owner)
                assert response.status_code == 200, response.text
                assert response.json().get("execution_phase") == expected
            denied = await client.get(
                path, headers={"Authorization": "Bearer " + signed_token(bob.user_id)}
            )
            assert denied.status_code == 403
            assert "memory" not in denied.text
        assert await phase_events(bus, sid) == [expected]
    finally:
        release.set()
        await asyncio.wait_for(run, 3)
        await bus.unlock(MessageBusKeys.session_lock(sid))
    assert await saved_phase(bus, sid) is None
    assert (await phase_events(bus, sid))[-1] == {
        "session_id": sid,
        "reply_id": "new-reply",
        "phase": None,
    }


@pytest.mark.parametrize("outcome", ["error", "cancel"])
async def test_writeback_error_or_cancellation_clears_its_phase(
    memory_app, monkeypatch, outcome
):
    _, memory, bus, sid, _, _ = memory_app
    entered, release = asyncio.Event(), asyncio.Event()

    async def job(self, name, **kwargs):
        if name == "auto_memory":
            entered.set()
            await release.wait()
            raise RuntimeError("test extraction failure")
        return SimpleNamespace(metadata={})

    monkeypatch.setattr(ReMeMiddleware, "_run_job", job)
    run = asyncio.create_task(turn(memory, sid, "reply-failure", []))
    await asyncio.wait_for(entered.wait(), 3)
    try:
        assert (await saved_phase(bus, sid))["phase"] == "memory"
    finally:
        if outcome == "cancel":
            run.cancel()
            with pytest.raises(asyncio.CancelledError):
                await run
        else:
            release.set()
            await asyncio.wait_for(run, 3)
    assert await saved_phase(bus, sid) is None
    assert (await phase_events(bus, sid))[-1]["phase"] is None


async def test_shared_project_memory_keeps_concurrent_reply_progress_separate(
    memory_app, monkeypatch
):
    _, memory, bus, sid, _, _ = memory_app
    other = "other-session"
    entered = {key: asyncio.Event() for key in (sid, other)}
    release = {key: asyncio.Event() for key in (sid, other)}

    async def job(self, name, **kwargs):
        if name == "auto_memory":
            key = kwargs["session_id"]
            entered[key].set()
            await release[key].wait()
        return SimpleNamespace(metadata={})

    monkeypatch.setattr(ReMeMiddleware, "_run_job", job)
    runs = [
        asyncio.create_task(turn(memory, key, reply, []))
        for key, reply in ((sid, "reply-one"), (other, "reply-two"))
    ]
    try:
        await asyncio.wait_for(
            asyncio.gather(*(event.wait() for event in entered.values())), 3
        )
        assert await saved_phase(bus, sid) == {
            "session_id": sid,
            "reply_id": "reply-one",
            "phase": "memory",
        }
        assert await saved_phase(bus, other) == {
            "session_id": other,
            "reply_id": "reply-two",
            "phase": "memory",
        }
        release[sid].set()
        await asyncio.wait_for(runs[0], 3)
        assert await saved_phase(bus, sid) is None
        assert (await saved_phase(bus, other))["reply_id"] == "reply-two"
    finally:
        for event in release.values():
            event.set()
        await asyncio.gather(*runs)
    assert await saved_phase(bus, other) is None


async def test_search_has_no_memory_phase_and_new_turn_clears_stale_progress(
    memory_app, monkeypatch
):
    app, memory, bus, sid, alice, _ = memory_app
    await bus.registry_set(
        MessageBusKeys.projection_namespace(sid),
        FIELD,
        json.dumps({"session_id": sid, "reply_id": "stale", "phase": "memory"}),
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app),
        base_url="http://test",
        headers={"Authorization": "Bearer " + signed_token(alice.user_id)},
    ) as client:
        response = await client.get(f"/manager/tasks/{sid}")
        assert response.status_code == 200
        assert "execution_phase" in response.json()
        assert response.json()["execution_phase"] is None

    searched, continue_reply = asyncio.Event(), asyncio.Event()

    async def job(self, name, **kwargs):
        if name == "search":
            searched.set()
            await asyncio.Event().wait()
        return SimpleNamespace(metadata={})

    async def before_answer():
        await searched.wait()
        await continue_reply.wait()

    monkeypatch.setattr(ReMeMiddleware, "_run_job", job)
    run = asyncio.create_task(
        turn(memory, sid, "after-search", [], before_answer=before_answer)
    )
    try:
        await asyncio.wait_for(searched.wait(), 3)
        assert await saved_phase(bus, sid) is None
        assert all(event["phase"] is None for event in await phase_events(bus, sid))
    finally:
        continue_reply.set()
        await asyncio.wait_for(run, 3)


async def test_progress_transport_failure_does_not_skip_native_writeback(
    memory_app, monkeypatch, caplog
):
    _, memory, bus, sid, _, _ = memory_app
    writes = []

    async def fail(*args, **kwargs):
        raise RuntimeError("test progress unavailable")

    async def job(self, name, **kwargs):
        if name == "auto_memory":
            writes.append(kwargs["session_id"])
        return SimpleNamespace(metadata={})

    monkeypatch.setattr(bus, "registry_set", fail)
    monkeypatch.setattr(bus, "publish", fail)
    monkeypatch.setattr(ReMeMiddleware, "_run_job", job)
    events = []
    await turn(memory, sid, "reply-bus-failure", events)
    assert writes == [sid]
    assert isinstance(events[-1], ReplyEndEvent)
    assert "execution phase" in caplog.text.lower()
