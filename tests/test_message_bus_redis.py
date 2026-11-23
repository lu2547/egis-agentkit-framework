"""Only native session replay moves to Redis Streams in cluster mode."""

import asyncio
import json
import socket

import pytest
from agentscope.app.message_bus import MessageBusKeys, RedisMessageBus
from redis.exceptions import ConnectionError
from redis_helpers import redis_buses as redis_buses
from redis_helpers import redis_config as redis_config
from redis_helpers import redis_key as redis_key
from sqlalchemy import text

from egis_agentkit.client.nacos._settings import RedisConfig
from egis_agentkit.core.message_bus import EgisMessageBus


async def test_redis_stream_replay_has_independent_cursors(redis_buses, redis_key):
    a, b = redis_buses
    key = MessageBusKeys.session_events(redis_key)
    assert isinstance(a._replay, RedisMessageBus)
    first = await a.log_append(key, {"type": "REPLY_START"})
    second = await b.log_append(key, {"type": "TEXT_DELTA", "text": "你好"})
    last = await a.log_append(key, {"type": "REPLY_END"})
    entries = await a.log_read(key)
    assert [entry_id for entry_id, _ in entries] == [first, second, last]
    assert await b.log_read(key, first) == entries[1:]
    assert await a.log_read(key, second) == entries[2:]
    assert await b.log_read(key) == entries
    await b.log_trim(key, before_id=second)
    assert await a.log_read(key) == entries[1:]


async def test_replay_retention_applies_to_redis_stream(storage, redis_config, redis_key):
    config = redis_config.model_copy(update={"retention_seconds": 2})
    bus = EgisMessageBus(
        deployment_mode="cluster", engine=storage.engine, redis=config
    )
    await bus._start_replay()
    try:
        key = MessageBusKeys.session_events(redis_key)
        await bus.log_append(key, {"type": "REPLY_START"})
        assert 0 < await bus._replay.get_client().pttl(key) <= 2000
        async with asyncio.timeout(3):
            while await bus.log_read(key):
                await asyncio.sleep(0.05)
    finally:
        await bus.aclose()


async def test_session_replay_is_the_only_bus_state_written_to_redis(
    redis_buses, redis_key
):
    a, b = redis_buses
    inbox = MessageBusKeys.inbox(redis_key)
    checkpoint = MessageBusKeys.session_event_checkpoint(redis_key)
    replay = MessageBusKeys.session_events(redis_key)
    lock = MessageBusKeys.session_lock(redis_key)
    generic_log = f"{redis_key}:generic-log"
    await a._pg.log_append(replay, {"legacy": "PostgreSQL"})
    assert await a.log_read(replay) == []
    await a.queue_push(inbox, {"n": 1})
    await a.registry_set(checkpoint, "cursor", "last")
    await a.log_append(generic_log, {"n": 2})
    replay_id = await a.log_append(replay, {"n": 3})
    assert await a.try_lock(lock)
    try:
        assert await b.is_locked(lock)
        assert [item for _, item in await b.queue_read(inbox)] == [{"n": 1}]
        assert await b.registry_get(checkpoint, "cursor") == "last"
        assert [item for _, item in await b.log_read(generic_log)] == [{"n": 2}]
        assert await b.log_read(replay) == [(replay_id, {"n": 3})]
        client = a._replay.get_client()
        assert await client.exists(inbox, checkpoint, lock, generic_log) == 0
        async with a._pg.engine.connect() as conn:
            assert await conn.scalar(
                text("SELECT count(*) FROM ak_bus_entries WHERE kind='log' AND key=:key"),
                {"key": replay},
            ) == 1
    finally:
        await a.unlock(lock)


async def test_cluster_startup_checks_redis_connection(storage, monkeypatch):
    from egis_agentkit.client._kafka import KafkaTransport

    async def unexpected_kafka_start(_):
        pytest.fail("Kafka startup must wait for the Redis replay probe")

    monkeypatch.setattr(KafkaTransport, "start", unexpected_kafka_start)
    with socket.socket() as unavailable:
        unavailable.bind(("127.0.0.1", 0))
        bus = EgisMessageBus(
            deployment_mode="cluster",
            engine=storage.engine,
            redis=RedisConfig(
                host="127.0.0.1",
                port=unavailable.getsockname()[1],
                connect_timeout_seconds=0.5,
            ),
        )
        try:
            with pytest.raises((ConnectionError, TimeoutError)):
                await bus.__aenter__()
            assert not bus.health()["is_ready"]
            assert not bus.health()["redis"]["is_ready"]
        finally:
            await bus.aclose()


async def test_startup_requires_the_configured_redis_password_reference(storage, monkeypatch):
    name = "AGENTKIT_TEST_REDIS_ABSENT_PASSWORD"
    monkeypatch.delenv(name, raising=False)
    bus = EgisMessageBus(
        deployment_mode="cluster",
        engine=storage.engine,
        redis=RedisConfig(password_env=name),
    )
    try:
        with pytest.raises(RuntimeError, match=name):
            await bus.__aenter__()
        assert not bus.health()["is_ready"]
    finally:
        await bus.aclose()


async def test_rewrite_floor_and_notice_follow_the_history_transaction(
    storage, redis_buses, redis_key
):
    writer, reader = redis_buses
    key = MessageBusKeys.session_events(redis_key)
    assert await writer.capture_replay_tail(redis_key) == "0-0"
    old_event = {"type": "REPLY_END"}
    old_cursor = await writer.log_append(key, old_event)
    assert await writer.capture_replay_tail(redis_key) == old_cursor
    pending_namespace = writer.rewrite_notice_namespace()
    notice = '{"message_id":"edited"}'

    async with storage.engine.connect() as conn:
        txn = await conn.begin()
        await writer.stage_rewrite_replay(conn, redis_key, old_cursor, notice)
        assert await reader.log_read(key) == [(old_cursor, old_event)]
        assert await reader.registry_get(pending_namespace, redis_key) is None
        await txn.rollback()

    assert await reader.log_read(key) == [(old_cursor, old_event)]
    assert await reader.registry_get(pending_namespace, redis_key) is None
    async with storage.engine.begin() as conn:
        await writer.stage_rewrite_replay(conn, redis_key, old_cursor, notice)

    assert await reader.log_read(key) == []
    assert await reader.registry_get(pending_namespace, redis_key) == notice


async def test_rewrite_floor_skips_untrimmed_events_with_numeric_cursors(
    storage, redis_buses, redis_key, monkeypatch
):
    writer, reader = redis_buses
    key = MessageBusKeys.session_events(redis_key)
    redis = writer._replay.get_client()
    for cursor in ("9-1", "9-2"):
        await redis.xadd(key, {"payload": json.dumps({"cursor": cursor})}, id=cursor)
    floor = await writer.capture_replay_tail(redis_key)
    async with storage.engine.begin() as conn:
        await writer.stage_rewrite_replay(conn, redis_key, floor)

    async def unavailable(*_):
        raise ConnectionError("Redis unavailable")

    monkeypatch.setattr(writer._replay, "log_trim", unavailable)
    await writer.log_trim(key, before_id=floor)
    for cursor in ("9-10", "10-0"):
        await redis.xadd(key, {"payload": json.dumps({"cursor": cursor})}, id=cursor)
    assert await redis.xlen(key) == 4
    updated = [(cursor, {"cursor": cursor}) for cursor in ("9-10", "10-0")]
    assert await reader.log_read(key) == updated
    assert await reader.log_read(key, "9-1") == updated
    assert await reader.log_read(key, "9-10") == updated[1:]


async def test_checkpoint_redis_trim_failure_does_not_fail_the_run(
    redis_buses, redis_key, monkeypatch, caplog
):
    bus, _ = redis_buses
    key = MessageBusKeys.session_events(redis_key)

    async def unavailable(*_):
        raise ConnectionError("Redis unavailable")

    monkeypatch.setattr(bus._replay, "log_trim", unavailable)
    await bus.log_trim(key, before_id="1-0")
    assert "checkpoint" in caplog.text
    with pytest.raises(ConnectionError):
        await bus.log_trim(key)

    async def pg_error(*_):
        raise RuntimeError("Postgres trim failed")

    monkeypatch.setattr(bus._pg, "log_trim", pg_error)
    with pytest.raises(RuntimeError, match="Postgres trim failed"):
        await bus.log_trim(f"{redis_key}:generic-log", before_id="1-0")


async def test_session_purge_preserves_success_when_redis_trim_fails(
    storage, redis_buses, redis_key, monkeypatch
):
    bus, reader = redis_buses
    failed, following, unrelated = (f"{redis_key}:{i}" for i in range(3))
    notice = '{"message_id":"edited"}'
    async with storage.engine.begin() as conn:
        for sid in (failed, following, unrelated):
            await bus.stage_rewrite_replay(conn, sid, "0-0", notice)
    for sid in (failed, following, unrelated):
        await bus.queue_push(MessageBusKeys.inbox(sid), {"sid": sid})
        await bus.log_append(MessageBusKeys.session_events(sid), {"sid": sid})

    trim = bus._replay.log_trim

    async def fail_one(key, before_id=None):
        if key == MessageBusKeys.session_events(failed):
            raise ConnectionError("Redis unavailable")
        await trim(key, before_id)

    monkeypatch.setattr(bus._replay, "log_trim", fail_one)
    await bus.purge_sessions([failed, following])
    pending = bus.rewrite_notice_namespace()
    for sid in (failed, following):
        assert await reader.queue_read(MessageBusKeys.inbox(sid)) == []
        assert await reader.registry_get(pending, sid) is None
        assert await reader.registry_getall(bus.replay_rewrite_namespace(sid)) == {}

    assert await reader.log_read(MessageBusKeys.session_events(following)) == []
    assert await reader.log_read(MessageBusKeys.session_events(unrelated))
    assert await reader.registry_get(pending, unrelated) == notice
