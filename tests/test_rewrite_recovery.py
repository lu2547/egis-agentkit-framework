"""已提交编辑的回放边界与通知不依赖即时 Redis/Kafka 清理成功。"""

import asyncio
from unittest.mock import AsyncMock

from agentscope.app.message_bus import MessageBusKeys
from agentscope.message import UserMsg
from redis.exceptions import ConnectionError as RedisConnectionError
from redis_helpers import redis_bus as redis_bus
from redis_helpers import redis_buses as redis_buses
from redis_helpers import redis_config as redis_config
from test_message_actions import history
from test_message_rewrite import replace

from egis_agentkit.service.session._message_bus_override import SessionMessageBus
from egis_agentkit.service.session._rewrite_notice import publish_rewrite_notice


async def setup_rewrite(storage, tmp_path, bus, monkeypatch):
    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    sid = task["session_id"]
    monkeypatch.setattr(bus, "publish", AsyncMock())
    original_push = bus.queue_push

    async def push(key, payload, *, ttl_secs=None):
        if key == MessageBusKeys.wakeup_queue():
            return "test-wakeup"
        return await original_push(key, payload, ttl_secs=ttl_secs)

    monkeypatch.setattr(bus, "queue_push", push)
    await bus.log_append(MessageBusKeys.session_events(sid), {"old": "discarded"})
    edited = messages[2].model_copy(deep=True)
    edited.content = UserMsg("alice", "已提交的新问题").content
    return alice, service, task, messages, edited


async def test_failed_redis_trim_cannot_reject_committed_edit_or_replay_discarded_history(
    storage,
    tmp_path,
    redis_bus,
    monkeypatch,
):
    bus = redis_bus
    alice, service, task, messages, edited = await setup_rewrite(
        storage, tmp_path, bus, monkeypatch
    )
    sid = task["session_id"]
    monkeypatch.setattr(
        bus._replay,
        "log_trim",
        AsyncMock(side_effect=RedisConnectionError("unavailable")),
    )
    await replace(service, alice, task, messages[2], edited, bus)
    saved, _ = await storage.list_messages(alice.user_id, sid)
    assert saved == [*messages[:2], edited]
    raw = await bus._replay.log_read(MessageBusKeys.session_events(sid))
    assert any(payload.get("old") for _, payload in raw)
    visible = await bus.log_read(MessageBusKeys.session_events(sid))
    assert [payload["name"] for _, payload in visible] == ["chat_history_rewritten"]
    assert await bus.registry_get(bus.rewrite_notice_namespace(), sid) is None


async def test_failed_notification_is_durable_and_reuses_its_event_id_on_retry(
    storage,
    tmp_path,
    redis_bus,
    monkeypatch,
):
    bus = redis_bus
    alice, service, task, messages, edited = await setup_rewrite(
        storage, tmp_path, bus, monkeypatch
    )
    sid = task["session_id"]
    monkeypatch.setattr(
        bus, "publish", AsyncMock(side_effect=ConnectionError("Kafka unavailable"))
    )
    await replace(service, alice, task, messages[2], edited, bus)
    pending = await bus.registry_get(bus.rewrite_notice_namespace(), sid)
    assert pending is not None
    assert (await storage.get_message(alice.user_id, sid, edited.id)) == edited
    monkeypatch.setattr(bus, "publish", AsyncMock())
    await publish_rewrite_notice(bus, storage, sid)
    assert await bus.registry_get(bus.rewrite_notice_namespace(), sid) is None
    events = [
        payload for _, payload in await bus.log_read(MessageBusKeys.session_events(sid))
    ]
    assert events and all(event["name"] == "chat_history_rewritten" for event in events)
    assert len({event["id"] for event in events}) == 1
    assert all(
        event["value"]["input"]["content"][0]["text"] == "已提交的新问题"
        for event in events
    )


async def test_pending_boundary_is_published_before_a_regenerated_reply_can_start(
    storage,
    tmp_path,
    redis_bus,
    redis_config,
    monkeypatch,
):
    bus = redis_bus
    alice, service, task, messages, edited = await setup_rewrite(
        storage, tmp_path, bus, monkeypatch
    )
    sid = task["session_id"]
    monkeypatch.setattr(
        bus, "publish", AsyncMock(side_effect=ConnectionError("temporary failure"))
    )
    await replace(service, alice, task, messages[2], edited, bus)
    worker = SessionMessageBus(
        deployment_mode="cluster",
        engine=storage.engine,
        redis=redis_config,
        presentation_storage=storage,
    )
    await worker._start_replay()
    failed = asyncio.Event()
    order = []
    attempts = 0

    async def publish(key, payload):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            failed.set()
            raise ConnectionError("temporary failure")
        order.append(payload["name"])

    monkeypatch.setattr(worker, "publish", publish)

    async def run():
        async with worker.acquire_lock(MessageBusKeys.session_lock(sid)):
            order.append("new_reply_started")

    running = asyncio.create_task(run())
    try:
        await asyncio.wait_for(failed.wait(), timeout=3)
        assert order == []
        await asyncio.wait_for(running, timeout=5)
        assert order == ["chat_history_rewritten", "new_reply_started"]
    finally:
        running.cancel()
        await asyncio.gather(running, return_exceptions=True)
        await worker.aclose()
