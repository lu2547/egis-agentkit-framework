"""Real two-instance Kafka/PG transport tests; no service replacements."""

import asyncio
from contextlib import suppress
from uuid import uuid4

import pytest
import pytest_asyncio
from agentscope.app.message_bus import MessageBusKeys
from aiokafka.errors import CommitFailedError
from aiokafka.structs import TopicPartition
from test_message_bus import pg_engine as pg_engine

from egis_agentkit.extensions.message_bus import EgisMessageBus

WAKE_QUEUE = MessageBusKeys.wakeup_queue()
WAKE_SIGNAL = MessageBusKeys.wakeup_signal()


@pytest_asyncio.fixture
async def kafka_buses(pg_engine):
    prefix = "egis-test-" + uuid4().hex
    kwargs = dict(
        deployment_mode="cluster",
        engine=pg_engine,
        topic_prefix=prefix,
        consumer_group=prefix + "-workers",
        buffer_size=5,
    )
    a, b = EgisMessageBus(**kwargs), EgisMessageBus(**kwargs)
    await a.migrate()
    await a.ensure_topics(partitions=4)
    await a.__aenter__()
    await b.__aenter__()
    yield a, b
    await a.aclose()
    await b.aclose()
    # Delete only this test's unique Kafka topics, never the application's topics.
    from aiokafka.admin import AIOKafkaAdminClient

    admin = AIOKafkaAdminClient(bootstrap_servers="localhost:9092")
    await admin.start()
    try:
        await admin.delete_topics([prefix + ".triggers", prefix + ".broadcast"])
    finally:
        await admin.close()


async def cancel(task):
    task.cancel()
    with suppress(asyncio.CancelledError):
        await task


async def drain_until(bus, result, expected, ready, done):
    async for _ in bus.subscribe(WAKE_SIGNAL, on_ready=ready.set):
        result.extend(await bus.queue_drain(WAKE_QUEUE, max_count=3))
        if len(result) >= expected:
            done.set()


async def test_global_group_drains_once_without_broadcast_competition(kafka_buses):
    a, b = kafka_buses
    results = [[], []]
    ready = [asyncio.Event(), asyncio.Event()]
    done = [asyncio.Event(), asyncio.Event()]
    tasks = [
        asyncio.create_task(drain_until(bus, results[n], 90, ready[n], done[n]))
        for n, bus in enumerate((a, b))
    ]
    try:
        await asyncio.gather(*(event.wait() for event in ready))
        for n in range(90):
            await a.queue_push(WAKE_QUEUE, {"n": n, "session_id": str(n)})
            await a.publish(WAKE_SIGNAL, {})  # compatibility signal must not broadcast
        async with asyncio.timeout(15):
            while sum(map(len, results)) < 90:
                await asyncio.sleep(0.02)
        values = [payload["n"] for result in results for _, payload in result]
        assert sorted(values) == list(range(90))
        assert all(results)  # four partitions actually distribute to both nodes
        await asyncio.sleep(0.2)
        assert sum(map(len, results)) == 90
    finally:
        for task in tasks:
            await cancel(task)


async def test_broadcast_public_contract_including_cancel_sse_and_index(kafka_buses):
    a, b = kafka_buses
    for key in (
        MessageBusKeys.session_cancel_channel(),
        MessageBusKeys.session_events("s"),
        MessageBusKeys.index_tasks_signal(),
        "arbitrary:public:key",
    ):
        await a.publish(key, {"old": True})
        calls = []
        subscriptions = [
            bus.subscribe(key, on_ready=lambda: calls.append("ready")) for bus in (a, b)
        ]
        waiting = [asyncio.create_task(anext(sub)) for sub in subscriptions]
        async with asyncio.timeout(5):
            while len(calls) < 2:
                await asyncio.sleep(0.01)
        await a.publish(key, {"new": True})
        assert await asyncio.gather(*waiting) == [{"new": True}, {"new": True}]
        assert calls == ["ready", "ready"]
        for subscription in subscriptions:
            await subscription.aclose()


async def test_unread_bounded_buffer_replays_after_rebalance(kafka_buses):
    a, b = kafka_buses
    for n in range(40):
        await a.queue_push(WAKE_QUEUE, {"n": n, "session_id": str(n)})
    await asyncio.sleep(0.5)
    assert len(a._transport._pending) <= 5
    assert len(b._transport._pending) <= 5
    # Neither node has drained: no group offset may be committed yet.
    await a.aclose()
    result, ready, done = [], asyncio.Event(), asyncio.Event()
    task = asyncio.create_task(drain_until(b, result, 40, ready, done))
    try:
        await asyncio.wait_for(done.wait(), 15)
        assert sorted(payload["n"] for _, payload in result) == list(range(40))
    finally:
        await cancel(task)


async def test_commit_failure_keeps_buffer_and_drain_ack_precedes_business(
    kafka_buses, monkeypatch
):
    a, b = kafka_buses
    await b.aclose()
    signal = a.subscribe(WAKE_SIGNAL)
    pending_signal = asyncio.create_task(anext(signal))
    entry_id = await a.queue_push(WAKE_QUEUE, {"n": 9, "session_id": "single"})
    second_id = await a.queue_push(WAKE_QUEUE, {"n": 10, "session_id": "single"})
    await asyncio.wait_for(pending_signal, 10)
    async with asyncio.timeout(10):
        while len(a._transport._pending) < 2:
            await asyncio.sleep(0.01)
    consumer = a._transport.consumer
    real_commit = consumer.commit

    async def failing_commit(*args, **kwargs):
        raise CommitFailedError("injected before broker commit")

    monkeypatch.setattr(consumer, "commit", failing_commit)
    with pytest.raises(CommitFailedError):
        await a.queue_drain(WAKE_QUEUE, 1)
    monkeypatch.setattr(consumer, "commit", real_commit)
    assert await a.queue_drain(WAKE_QUEUE, 1) == [
        (entry_id, {"n": 9, "session_id": "single"})
    ]
    # Business work has not run. Kafka has already acknowledged the drain.
    topic, partition, offset = entry_id.rsplit(":", 2)
    assert (
        await consumer.committed(TopicPartition(topic, int(partition)))
        == int(offset) + 1
    )
    assert await a.queue_drain(WAKE_QUEUE) == [
        (second_id, {"n": 10, "session_id": "single"})
    ]
    assert await a.queue_drain(WAKE_QUEUE) == []
    await signal.aclose()


async def test_trigger_delete_and_sliding_ttl_do_not_resurrect_old_records(kafka_buses):
    a, b = kafka_buses
    await b.aclose()
    await a.queue_push(WAKE_QUEUE, {"old": True})
    await a.queue_delete(WAKE_QUEUE)
    await a.queue_push(WAKE_QUEUE, {"new": True}, ttl_secs=1)
    await asyncio.sleep(1.1)
    assert await a.queue_drain(WAKE_QUEUE) == []
    await a.queue_push(WAKE_QUEUE, {"live": True})
    result, ready, done = [], asyncio.Event(), asyncio.Event()
    task = asyncio.create_task(drain_until(a, result, 1, ready, done))
    try:
        await asyncio.wait_for(done.wait(), 10)
        assert [payload for _, payload in result] == [{"live": True}]
    finally:
        await cancel(task)


async def test_unmodified_native_dispatchers_spawn_and_cancel_across_nodes(kafka_buses):
    """Exercise native dispatchers/registry; model execution is a narrow test double."""
    from contextlib import AsyncExitStack

    from agentscope.app._bus_ops import enqueue_run_trigger
    from agentscope.app._manager._cancel_dispatcher import CancelDispatcher
    from agentscope.app._manager._chat_run_registry import ChatRunRegistry
    from agentscope.app._manager._wakeup_dispatcher import WakeupDispatcher

    a, b = kafka_buses
    started, cancelled = [], []
    hold = asyncio.Event()

    class Storage:
        async def get_session(self, *_):
            return object()

    class Chat:
        def __init__(self, bus):
            self.bus = bus

        async def run(self, *, user_id, session_id, agent_id, input_msg):
            async with self.bus.acquire_lock(MessageBusKeys.session_lock(session_id)):
                started.append(session_id)
                try:
                    await hold.wait()
                except asyncio.CancelledError:
                    cancelled.append(session_id)
                    raise

    class Background:
        def cancel_session_tasks(self, _):
            return 0

    async with AsyncExitStack() as stack:
        for bus in (a, b):
            registry = await stack.enter_async_context(ChatRunRegistry())
            await stack.enter_async_context(
                CancelDispatcher(bus, registry, Background())
            )
            await stack.enter_async_context(
                WakeupDispatcher(bus, Storage(), Chat(bus), registry)
            )
        for n in range(12):
            await enqueue_run_trigger(
                a, user_id="u", session_id=f"native-{n}", agent_id="a"
            )
        async with asyncio.timeout(10):
            while len(started) < 12:
                await asyncio.sleep(0.02)
        assert sorted(started) == sorted(f"native-{n}" for n in range(12))
        await a.publish(
            MessageBusKeys.session_cancel_channel(), {"session_id": "native-5"}
        )
        async with asyncio.timeout(5):
            while "native-5" not in cancelled:
                await asyncio.sleep(0.02)
        assert not await b.is_locked(MessageBusKeys.session_lock("native-5"))
        hold.set()


async def test_background_transport_failure_reaches_subscriber(
    kafka_buses, monkeypatch
):
    a, _ = kafka_buses
    assert a.health()["is_ready"]

    async def broken_fetch(*args, **kwargs):
        raise RuntimeError("injected unrecoverable receive failure")

    async def subscriber():
        async for _ in a.subscribe(WAKE_SIGNAL):
            await a.queue_drain(WAKE_QUEUE)

    monkeypatch.setattr(a._transport.consumer, "getmany", broken_fetch)
    task = asyncio.create_task(subscriber())
    with pytest.raises(RuntimeError, match="background transport"):
        await asyncio.wait_for(task, 2)
    state = a.health()
    assert state["is_ready"] is False
    assert state["kafka"]["has_background_error"] is True
    assert "injected unrecoverable" not in str(state)


async def test_health_detects_stopped_producer(kafka_buses):
    a, _ = kafka_buses
    assert a.health()["is_ready"]
    await a._transport.producer.stop()
    state = a.health()
    assert state["is_ready"] is False
    assert state["kafka"]["producer_is_running"] is False


async def test_large_native_image_event_and_concurrent_small_events_across_nodes(
    kafka_buses,
):
    """Real PG/Kafka, unchanged native event helper, and two independent receivers."""
    from agentscope.app._bus_ops import publish_session_event

    a, b = kafka_buses
    key = MessageBusKeys.session_events("large-native-image")
    # More than a 20 MiB JPEG after base64 expansion, under the 32 MiB bound.
    event = {
        "type": "tool_result",
        "image": "data:image/jpeg;base64," + "A" * (28 * 1024 * 1024),
    }
    ready = [asyncio.Event(), asyncio.Event()]
    subscriptions = [
        bus.subscribe(key, on_ready=ready[n].set) for n, bus in enumerate((a, b))
    ]

    async def receive(subscription):
        return [await anext(subscription) for _ in range(4)]

    receiving = [asyncio.create_task(receive(sub)) for sub in subscriptions]
    sending = None
    try:
        await asyncio.wait_for(asyncio.gather(*(r.wait() for r in ready)), 5)
        sending = asyncio.create_task(
            publish_session_event(a, "large-native-image", event)
        )
        # Start small events while the large event's PG body/send is in flight.
        async with asyncio.timeout(10):
            while key not in a._transport._publish_locks and not sending.done():
                await asyncio.sleep(0)
        small = [{"n": n} for n in range(1, 4)]
        await asyncio.gather(*(a.publish(key, payload) for payload in small))
        entry_id = await asyncio.wait_for(sending, 10)
        results = await asyncio.wait_for(asyncio.gather(*receiving), 15)
        assert results == [[{**event, "_entry_id": entry_id}, *small]] * 2
        assert await b.log_read(key) == [(entry_id, event)]
        # Trimming public replay does not invalidate live transport bodies.
        await a.log_trim(key)
        assert await b.log_read(key) == []
    finally:
        for task in [*receiving, *([sending] if sending is not None else [])]:
            task.cancel()
        await asyncio.gather(
            *receiving,
            *([sending] if sending is not None else []),
            return_exceptions=True,
        )
        for subscription in subscriptions:
            await subscription.aclose()
