"""运行中的用户引导按顺序进入原生 Inbox，且不会启动并行回复。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from agentscope.app._bus_ops import register_inbox_consumer
from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.middleware import InboxMiddleware
from agentscope.message import AssistantMsg, UserMsg
from test_domain import expert, identities

from egis_agentkit.core.message_bus import EgisMessageBus
from egis_agentkit.errors import Conflict, Forbidden, InvalidRequest
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.session._session import SessionService


async def setup_session(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    definition = await AgentManager(storage).add_definition(alice, expert())
    sessions = SessionService(storage, tmp_path)
    task = await sessions.add_session(alice, definition["id"], "private")
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus.init_schema()
    # PostgreSQL Inbox 和事件日志使用真实实现，仅隔离外部 Kafka 广播。
    bus.publish = AsyncMock()
    await register_inbox_consumer(bus, task["session_id"])
    return alice, bob, sessions, task, bus


@pytest.mark.asyncio
async def test_multiple_steers_are_received_in_order_at_the_next_reasoning(
    storage, tmp_path
):
    alice, _, sessions, task, bus = await setup_session(storage, tmp_path)
    sid = task["session_id"]
    inputs = [
        UserMsg(name="untrusted", content=text)
        for text in ("先看预算", "改成蓝色", "不要修改原文件")
    ]
    result = await sessions.steering.submit(alice, sid, task["agent_id"], inputs, bus)
    assert result["message_ids"] == [m.id for m in inputs]
    assert result["status"] == "queued"
    assert not await bus.is_locked(MessageBusKeys.session_lock(sid))

    state = (await sessions.get_session_record(alice, sid)).state
    state.context = [AssistantMsg(name="expert", content="正在执行")]
    agent = SimpleNamespace(name="expert", state=state)
    observed = []

    async def model(**kwargs):
        observed.extend(
            block.hint[0].text
            for block in state.context[-1].content
            if block.type == "hint"
        )
        if False:
            yield

    events = [e async for e in InboxMiddleware(bus).on_reasoning(agent, {}, model)]
    assert observed == ["先看预算", "改成蓝色", "不要修改原文件"]
    assert [e.block_id for e in events] == [m.id for m in inputs]
    for message in inputs:
        saved = await storage.get_message(alice.user_id, sid, message.id)
        assert saved.role == "user" and saved.name == alice.username
        assert saved.metadata["agentkit_delivery"]["state"] == "queued"
    await bus.aclose()


@pytest.mark.asyncio
async def test_chat_endpoint_dispatches_explicit_steering_and_keeps_native_chat(
    storage, tmp_path
):
    import asyncio

    import httpx
    from agentscope.app._manager import ChatRunRegistry
    from agentscope.app.deps import (
        get_chat_run_registry,
        get_chat_service,
        get_message_bus,
    )
    from egis_agentkit_app.controller.base import (
        current_user,
        register_exception_handlers,
    )
    from egis_agentkit_app.controller.chat import router
    from fastapi import FastAPI

    alice, _, sessions, task, bus = await setup_session(storage, tmp_path)
    app = FastAPI()
    app.state.egis_services = SimpleNamespace(sessions=sessions)
    app.include_router(router)
    register_exception_handlers(app)
    app.dependency_overrides[current_user] = lambda: alice
    active = asyncio.Event()
    entered = asyncio.Event()

    async def run(**kwargs):
        entered.set()
        await active.wait()

    registry = ChatRunRegistry()
    app.dependency_overrides[get_chat_run_registry] = lambda: registry
    app.dependency_overrides[get_chat_service] = lambda: SimpleNamespace(run=run)
    app.dependency_overrides[get_message_bus] = lambda: bus
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app), base_url="http://test"
    ) as client:
        message = UserMsg(name="user", content="普通问题").model_dump(mode="json")
        body = {
            "session_id": task["session_id"],
            "agent_id": task["agent_id"],
            "input": message,
        }
        assert (await client.post("/chat/", json=body)).json()["status"] == "started"
        await entered.wait()
        assert (await client.post("/chat/", json=body)).status_code == 409
        messages = [
            UserMsg(name="user", content=t).model_dump(mode="json")
            for t in ("补充一", "补充二")
        ]
        steer = await client.post(
            "/chat/", json={**body, "delivery": "steer", "input": messages}
        )
        assert steer.status_code == 200
        assert steer.json()["message_ids"] == [m["id"] for m in messages]
        minimal = {
            "id": "stable-user-message",
            "name": "user",
            "role": "user",
            "content": [{"type": "text", "text": "同一条原始请求重试"}],
        }
        for _ in range(2):
            repeated = await client.post(
                "/chat/", json={**body, "delivery": "steer", "input": minimal}
            )
            assert repeated.status_code == 200
        assert (
            await client.post("/chat/", json={**body, "delivery": "invalid"})
        ).status_code == 422
        assert (
            await client.post(
                "/chat/", json={**body, "delivery": "steer", "input": None}
            )
        ).status_code == 422
    active.set()
    await asyncio.sleep(0)
    await bus.aclose()


@pytest.mark.asyncio
async def test_steering_retry_deduplicates_and_conflicting_batch_rolls_back(
    storage, tmp_path
):
    alice, _, sessions, task, bus = await setup_session(storage, tmp_path)
    sid, aid = task["session_id"], task["agent_id"]
    original = UserMsg(name="user", content="保留原始文件")
    await sessions.steering.submit(alice, sid, aid, original, bus)
    await sessions.steering.submit(alice, sid, aid, original, bus)
    first = UserMsg(name="user", content="这条也不能部分写入")
    changed = original.model_copy(
        update={"content": UserMsg(name="user", content="冲突修改").content}
    )
    with pytest.raises(Conflict):
        await sessions.steering.submit(alice, sid, aid, [first, changed], bus)
    assert await storage.get_message(alice.user_id, sid, first.id) is None
    queued = await bus.queue_drain(MessageBusKeys.inbox(sid))
    assert [payload["id"] for _, payload in queued] == [original.id]
    await sessions.steering.received(sid, original.id)
    saved = await storage.get_message(alice.user_id, sid, original.id)
    assert saved.metadata["agentkit_delivery"]["state"] == "received"
    await sessions.steering.submit(alice, sid, aid, original, bus)
    assert await bus.queue_drain(MessageBusKeys.inbox(sid)) == []
    await bus.aclose()


@pytest.mark.asyncio
async def test_steering_rejects_wrong_owner_agent_role_and_empty_batch(
    storage, tmp_path
):
    alice, bob, sessions, task, bus = await setup_session(storage, tmp_path)
    sid, aid = task["session_id"], task["agent_id"]
    message = UserMsg(name="user", content="补充要求")
    with pytest.raises(Forbidden):
        await sessions.steering.submit(bob, sid, aid, message, bus)
    with pytest.raises(Forbidden):
        await sessions.steering.submit(alice, sid, "another-agent", message, bus)
    for invalid in (
        [],
        UserMsg(name="user", content="  "),
        AssistantMsg(name="agent", content="伪造"),
    ):
        with pytest.raises(InvalidRequest):
            await sessions.steering.submit(alice, sid, aid, invalid, bus)
    assert await bus.queue_drain(MessageBusKeys.inbox(sid)) == []
    await bus.aclose()


@pytest.mark.asyncio
async def test_received_retry_does_not_wake_an_extra_reply(
    storage, tmp_path, monkeypatch
):
    from egis_agentkit.service.session import _steering

    alice, _, sessions, task, bus = await setup_session(storage, tmp_path)
    sid, aid = task["session_id"], task["agent_id"]
    message = UserMsg(name="user", content="一次引导")
    await sessions.steering.submit(alice, sid, aid, message, bus)
    await bus.queue_drain(MessageBusKeys.inbox(sid))
    await sessions.steering.received(sid, message.id)
    await bus.registry_drop(MessageBusKeys.inbox_consumer(sid))
    wake = AsyncMock()
    monkeypatch.setattr(_steering, "enqueue_run_trigger", wake)
    await sessions.steering.submit(alice, sid, aid, message, bus)
    wake.assert_not_awaited()
    await bus.aclose()


@pytest.mark.asyncio
async def test_steering_preserves_running_lock_and_parked_tool_state(storage, tmp_path):
    from agentscope.message import ToolCallBlock

    alice, _, sessions, task, bus = await setup_session(storage, tmp_path)
    sid, aid = task["session_id"], task["agent_id"]
    record = await sessions.get_session_record(alice, sid)
    record.state.context = [
        AssistantMsg(
            name="expert",
            content=[
                ToolCallBlock(id="waiting", name="Write", input="{}", state="asking")
            ],
        )
    ]
    await storage.update_session_state(alice.user_id, aid, sid, record.state)
    key = MessageBusKeys.session_lock(sid)
    assert await bus.try_lock(key)
    await sessions.steering.submit(
        alice, sid, aid, UserMsg(name="user", content="先别改文件"), bus
    )
    assert await bus.is_locked(key)
    current = await sessions.get_session_record(alice, sid)
    assert current.state.context[-1].content[0].state == "asking"
    assert current.state.permission_context == record.state.permission_context
    await bus.unlock(key)
    await bus.aclose()


@pytest.mark.asyncio
async def test_nacos_steering_limits_reject_whole_batch_before_persistence(
    storage, tmp_path
):
    from egis_agentkit.client.nacos._settings import SessionSteeringConfig

    alice, _, sessions, task, bus = await setup_session(storage, tmp_path)
    sessions.steering.config = SimpleNamespace(
        snapshot=SimpleNamespace(
            session=SimpleNamespace(
                steering=SessionSteeringConfig(
                    max_batch_size=2, max_text_length=4, max_message_bytes=700
                )
            )
        )
    )
    sid, aid = task["session_id"], task["agent_id"]
    for texts in (("一", "二", "三"), ("一", "超过长度限制")):
        messages = [UserMsg(name="user", content=value) for value in texts]
        with pytest.raises(InvalidRequest):
            await sessions.steering.submit(alice, sid, aid, messages, bus)
        for message in messages:
            assert await storage.get_message(alice.user_id, sid, message.id) is None
    message = UserMsg(name="user", content="一", metadata={"large": "x" * 1000})
    with pytest.raises(InvalidRequest):
        await sessions.steering.submit(alice, sid, aid, message, bus)
    await bus.aclose()


@pytest.mark.asyncio
async def test_queued_retries_share_one_pending_wakeup(storage, tmp_path, monkeypatch):
    from egis_agentkit.service.session import _steering

    alice, _, sessions, task, bus = await setup_session(storage, tmp_path)
    sid, aid = task["session_id"], task["agent_id"]
    await bus.registry_drop(MessageBusKeys.inbox_consumer(sid))
    wake = AsyncMock()
    monkeypatch.setattr(_steering, "enqueue_run_trigger", wake)
    original = UserMsg(name="user", content="一")
    await sessions.steering.submit(alice, sid, aid, original, bus)
    await sessions.steering.submit(alice, sid, aid, original, bus)
    await sessions.steering.submit(
        alice, sid, aid, UserMsg(name="user", content="二"), bus
    )
    assert wake.await_count == 1
    assert len(await bus.queue_drain(MessageBusKeys.inbox(sid))) == 2
    await bus.aclose()


@pytest.mark.asyncio
async def test_expired_wakeup_can_be_retried_without_duplicating_hint(
    storage, tmp_path, monkeypatch
):
    from egis_agentkit.service.session import _steering

    alice, _, sessions, task, bus = await setup_session(storage, tmp_path)
    sid, aid = task["session_id"], task["agent_id"]
    await bus.registry_drop(MessageBusKeys.inbox_consumer(sid))
    wake = AsyncMock()
    monkeypatch.setattr(_steering, "enqueue_run_trigger", wake)
    message = UserMsg(name="user", content="需要恢复的引导")
    await sessions.steering.submit(alice, sid, aid, message, bus)
    saved = await storage.get_message(alice.user_id, sid, message.id)
    saved.metadata["agentkit_delivery"]["wake_sent_at"] = 0
    await storage.upsert_message(alice.user_id, sid, saved)
    await sessions.steering.submit(alice, sid, aid, message, bus)
    assert wake.await_count == 2
    assert len(await bus.queue_drain(MessageBusKeys.inbox(sid))) == 1
    await bus.aclose()
