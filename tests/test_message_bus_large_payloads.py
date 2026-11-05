"""Size-limited Kafka boundary with real PG storage and the public bus adapter."""

import asyncio
import json
import traceback
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
import pytest_asyncio
from agentscope.app._bus_ops import publish_session_event
from agentscope.app.message_bus import MessageBusKeys
from aiokafka.errors import MessageSizeTooLargeError
from aiokafka.structs import TopicPartition
from sqlalchemy import text
from sqlalchemy.exc import StatementError
from test_message_bus import pg_engine as pg_engine

from egis_agentkit.message_bus import EgisMessageBus
from egis_agentkit.message_bus._kafka import KafkaTransport


class LimitedBroker:
    """Reject oversized records instead of turning producer.send into a no-op."""

    def __init__(self):
        self.records = []
        self.queues = []
        self.fail_send = False

    async def send_and_wait(self, topic, *, value, key):
        if len(value) + len(key) + 128 > 1024 * 1024:
            raise MessageSizeTooLargeError("record exceeds the 1 MiB boundary")
        if self.fail_send:
            raise RuntimeError("injected send failure")
        record = SimpleNamespace(
            topic=topic, partition=0, offset=len(self.records), value=value, key=key
        )
        self.records.append(record)
        for queue in self.queues:
            await queue.put(record)
        return record

    async def stop(self):
        pass


class BroadcastReader:
    def __init__(self, broker, topic):
        self.broker = broker
        self.tp = TopicPartition(topic, 0)
        self.queue = asyncio.Queue()
        broker.queues.append(self.queue)

    def assignment(self):
        return {self.tp}

    async def end_offsets(self, assignment):
        return {self.tp: len(self.broker.records)}

    async def getmany(self, **kwargs):
        return {self.tp: [await self.queue.get()]}

    async def stop(self):
        pass


@pytest_asyncio.fixture
async def limited_buses(pg_engine):
    broker = LimitedBroker()
    buses = [
        EgisMessageBus(deployment_mode="cluster", engine=pg_engine) for _ in range(2)
    ]
    await buses[0].migrate()
    for bus in buses:
        transport = KafkaTransport(bus._pg, **bus._kafka_args)
        bus._transport = transport
        transport.producer = broker
        transport.broadcast_consumer = BroadcastReader(
            broker, transport.broadcast_topic
        )
        transport._broadcast_ready.set()
        pump = asyncio.create_task(transport._receive_broadcasts())
        pump.add_done_callback(transport._task_finished)
        transport._tasks = [pump]
    try:
        yield *buses, broker
    finally:
        for bus in buses:
            await bus.aclose()


async def test_native_large_image_event_is_delivered_in_order_and_replays(
    limited_buses,
):
    """Removing PG offload must fail at the producer's actual byte-size check."""
    a, b, broker = limited_buses
    key = MessageBusKeys.session_events("large-image")
    event = {
        "type": "tool_result",
        "content": [
            {"type": "image", "url": "data:image/jpeg;base64," + "A" * 1_357_803}
        ],
    }
    subscriptions = [bus.subscribe(key) for bus in (a, b)]
    waiting = [asyncio.create_task(anext(sub)) for sub in subscriptions]
    async with asyncio.timeout(2):
        while any(key not in bus._transport._subscriptions for bus in (a, b)):
            await asyncio.sleep(0)
    try:
        entry_id = await publish_session_event(a, "large-image", event)
        await a.publish(key, {"after": True})
        results = await asyncio.wait_for(asyncio.gather(*waiting), 2)
        assert results == [{**event, "_entry_id": entry_id}] * 2
        assert await asyncio.gather(*(anext(sub) for sub in subscriptions)) == [
            {"after": True},
            {"after": True},
        ]
        assert await b.log_read(key) == [(entry_id, event)]
        assert all(len(record.value) < 1024 * 1024 for record in broker.records)
        assert b"data:image" not in broker.records[0].value
    finally:
        for task in waiting:
            task.cancel()
        await asyncio.gather(*waiting, return_exceptions=True)
        for subscription in subscriptions:
            await subscription.aclose()


async def test_small_broadcast_stays_inline_without_payload_storage(limited_buses):
    a, _, broker = limited_buses
    await a.publish("small", {"_payload_ref": "ordinary user content"})
    assert json.loads(broker.records[0].value) == {
        "key": "small",
        "payload": {"_payload_ref": "ordinary user content"},
    }
    async with a._pg.engine.connect() as conn:
        assert (
            await conn.scalar(text("SELECT count(*) FROM egis_bus_broadcast_payloads"))
            == 0
        )


async def test_concurrent_small_publish_cannot_overtake_large_publish(
    limited_buses, monkeypatch
):
    a, b, _ = limited_buses
    entered, release = asyncio.Event(), asyncio.Event()
    store = a._pg.broadcast_store

    async def delayed_store(*args, **kwargs):
        entered.set()
        await release.wait()
        return await store(*args, **kwargs)

    monkeypatch.setattr(a._pg, "broadcast_store", delayed_store)
    ready = asyncio.Event()
    subscription = b.subscribe("ordered", on_ready=ready.set)
    first = asyncio.create_task(anext(subscription))
    await ready.wait()
    large = asyncio.create_task(
        a.publish("ordered", {"n": 1, "image": "A" * 1_357_803})
    )
    await entered.wait()
    small = asyncio.create_task(a.publish("ordered", {"n": 2}))
    await asyncio.sleep(0.01)
    release.set()
    try:
        await asyncio.gather(large, small)
        assert (await asyncio.wait_for(first, 2))["n"] == 1
        assert await asyncio.wait_for(anext(subscription), 2) == {"n": 2}
    finally:
        await subscription.aclose()


async def test_large_storage_wait_does_not_block_unrelated_control_keys(
    limited_buses, monkeypatch
):
    a, _, broker = limited_buses
    entered, release = asyncio.Event(), asyncio.Event()
    store = a._pg.broadcast_store

    async def delayed_store(*args, **kwargs):
        entered.set()
        await release.wait()
        return await store(*args, **kwargs)

    monkeypatch.setattr(a._pg, "broadcast_store", delayed_store)
    large = asyncio.create_task(a.publish("image-stream", {"image": "A" * 1_357_803}))
    await entered.wait()
    try:
        await asyncio.wait_for(a.publish("cancel-control", {"cancel": True}), 0.2)
        assert json.loads(broker.records[0].value)["payload"] == {"cancel": True}
    finally:
        release.set()
        await large


async def test_oversized_broadcast_is_rejected_before_pg_or_kafka_write(limited_buses):
    a, _, broker = limited_buses
    with pytest.raises(ValueError, match="broadcast.*bytes"):
        await a.publish("too-large", {"content": "A" * (32 * 1024 * 1024)})
    assert broker.records == []
    async with a._pg.engine.connect() as conn:
        assert (
            await conn.scalar(text("SELECT count(*) FROM egis_bus_broadcast_payloads"))
            == 0
        )


async def test_missing_large_payload_fails_subscriber_instead_of_skipping(
    limited_buses,
):
    a, b, broker = limited_buses
    subscription = b.subscribe("missing")
    waiting = asyncio.create_task(anext(subscription))
    async with asyncio.timeout(2):
        while "missing" not in b._transport._subscriptions:
            await asyncio.sleep(0)
    await broker.send_and_wait(
        a._transport.broadcast_topic,
        key=b"missing",
        value=json.dumps(
            {"key": "missing", "payload_ref": "missing", "version": 1}
        ).encode(),
    )
    try:
        with pytest.raises(RuntimeError, match="background transport"):
            await asyncio.wait_for(waiting, 2)
    finally:
        await subscription.aclose()


async def test_payload_expiry_is_individual_and_sweeps_without_tombstones(pg_engine):
    bus = EgisMessageBus(deployment_mode="cluster", engine=pg_engine)
    await bus.migrate()
    expired = await bus._pg.broadcast_store("topic", "key", {"n": 1}, ttl_secs=0.05)
    live = await bus._pg.broadcast_store("topic", "key", {"n": 2}, ttl_secs=30)
    assert await bus._pg.broadcast_read("topic", "key", expired) == {"n": 1}
    with pytest.raises(LookupError):
        await bus._pg.broadcast_read("other-topic", "key", live)
    with pytest.raises(LookupError):
        await bus._pg.broadcast_read("topic", "other-key", live)
    await asyncio.sleep(0.1)
    with pytest.raises(LookupError):
        await bus._pg.broadcast_read("topic", "key", expired)
    assert await bus._pg.broadcast_read("topic", "key", live) == {"n": 2}
    await bus._pg.sweep_expired()
    async with pg_engine.connect() as conn:
        assert (
            await conn.scalar(text("SELECT count(*) FROM egis_bus_broadcast_payloads"))
            == 1
        )
        assert await conn.scalar(text("SELECT count(*) FROM egis_bus_keys")) == 0


async def test_send_failure_is_visible_and_preserves_possible_accepted_reference(
    limited_buses,
):
    a, _, broker = limited_buses
    broker.fail_send = True
    with pytest.raises(RuntimeError, match="injected send failure"):
        await a.publish("failure", {"image": "A" * 1_357_803})
    # A timeout cannot prove that the broker rejected a record. Retain its body
    # until TTL cleanup, so a possibly accepted reference remains resolvable.
    async with a._pg.engine.connect() as conn:
        assert (
            await conn.scalar(text("SELECT count(*) FROM egis_bus_broadcast_payloads"))
            == 1
        )


async def test_payload_storage_error_does_not_expose_sql_parameters(
    limited_buses, monkeypatch
):
    a, _, broker = limited_buses
    secret = "data:image/jpeg;base64,private-image-content"

    @asynccontextmanager
    async def failing_transaction():
        raise StatementError(
            "injected storage error", "INSERT", {"payload": secret}, ValueError()
        )
        yield

    monkeypatch.setattr(a._pg, "engine", SimpleNamespace(begin=failing_transaction))
    with pytest.raises(
        RuntimeError, match="broadcast payload storage failed"
    ) as failure:
        await a.publish("failure", {"image": secret + "A" * 1_357_803})
    assert secret not in "".join(traceback.format_exception(failure.value))
    assert broker.records == []
