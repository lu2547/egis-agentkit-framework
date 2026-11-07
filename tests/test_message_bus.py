"""Native public bus contract; PG tests use an explicit local integration opt-in."""

import asyncio
import os
import shlex
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from agentscope.app.message_bus import InMemoryMessageBus, MessageBus
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from egis_agentkit.extensions.message_bus import EgisMessageBus, LeaseLostError


@pytest.fixture
def key():
    return "test:" + uuid4().hex


@pytest_asyncio.fixture
async def pg_engine():
    if os.environ.get("EGIS_BUS_INTEGRATION") != "1":
        pytest.skip("set EGIS_BUS_INTEGRATION=1 for local PG/Kafka")
    path = Path(__file__).resolve().parents[3] / "deploy/agentkit-infra/pg.env"
    env = dict(
        (name, shlex.split(value)[0])
        for name, value in (
            line.split("=", 1)
            for line in path.read_text().splitlines()
            if line and not line.startswith("#") and "=" in line
        )
    )
    url = URL.create(
        "postgresql+asyncpg",
        username=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
        host="localhost",
        port=5433,
        database=env["POSTGRES_DB"],
    )
    schema = "egis_bus_test_" + uuid4().hex
    admin = create_async_engine(url, hide_parameters=True)
    async with admin.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(
        url,
        pool_size=8,
        hide_parameters=True,
        connect_args={"server_settings": {"search_path": schema}},
    )
    try:
        yield engine
    finally:
        await engine.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest_asyncio.fixture
async def pg_buses(pg_engine):
    a = EgisMessageBus(deployment_mode="cluster", engine=pg_engine)
    b = EgisMessageBus(deployment_mode="cluster", engine=pg_engine)
    await a.migrate()
    yield a, b
    await a.aclose()
    await b.aclose()


async def test_standalone_native_contract_and_lifecycle(key):
    async with EgisMessageBus() as bus:
        assert isinstance(bus, MessageBus)
        assert isinstance(bus.delegate, InMemoryMessageBus)
        eid = await bus.queue_push(key, {"n": 1})
        assert await bus.queue_drain(key) == [(eid, {"n": 1})]
        assert await bus.queue_drain(key) == []
        first = await bus.log_append(key, {"n": 1})
        last = await bus.log_append(key, {"n": 2})
        assert await bus.log_read(key, since=first) == [(last, {"n": 2})]
        ready = asyncio.Event()
        subscription = bus.subscribe(key, on_ready=ready.set)
        waiting = asyncio.create_task(anext(subscription))
        await ready.wait()
        await bus.publish(key, {"cancel": True})
        assert await waiting == {"cancel": True}
        await subscription.aclose()
        await bus.registry_set(key, "f", "old")
        assert await bus.registry_set_if(key, "f", "new", expected="old")
        assert await bus.registry_pop(key, "f") == "new"
        async with bus.acquire_lock(key):
            assert await bus.is_locked(key)
        assert not await bus.is_locked(key)


def test_cluster_requires_explicit_pg_engine():
    with pytest.raises(ValueError):
        EgisMessageBus(deployment_mode="cluster")
    with pytest.raises(ValueError):
        EgisMessageBus(deployment_mode="invalid")


async def test_pg_queue_competing_drains_are_disjoint(pg_buses, key):
    a, b = pg_buses
    await asyncio.gather(*(a.queue_push(key, {"n": n}) for n in range(90)))
    batches = await asyncio.gather(*(bus.queue_drain(key, 7) for bus in [a, b] * 8))
    entries = [entry for batch in batches for entry in batch]
    assert len(entries) == 90
    assert {p["n"] for _, p in entries} == set(range(90))
    assert len({eid for eid, _ in entries}) == 90
    assert await b.queue_drain(key) == []
    await a.queue_push(key, {"deleted": True})
    await b.queue_delete(key)
    assert await a.queue_drain(key) == []


async def test_pg_log_cursors_follow_commit_order_and_do_not_reset(pg_buses, key):
    a, b = pg_buses
    ids = await asyncio.gather(
        *(bus.log_append(key, {"n": n}) for n, bus in enumerate([a, b] * 20))
    )
    entries = await b.log_read(key)
    seqs = [int(eid.split("-")[0]) for eid, _ in entries]
    assert seqs == list(range(1, 41))
    assert set(ids) == {eid for eid, _ in entries}
    assert await b.log_read(key, entries[19][0]) == entries[20:]
    await a.log_trim(key, entries[20][0])
    assert await b.log_read(key) == entries[20:]
    await b.log_trim(key)
    newer = await a.log_append(key, {"new": True})
    assert int(newer.split("-")[0]) > 40
    assert await b.log_read(key, ids[-1]) == [(newer, {"new": True})]
    with pytest.raises(ValueError):
        await a.log_read(key, "bad-cursor")
    await a.log_append(key, {}, max_len=1)
    assert len(await b.log_read(key)) == 1


async def test_pg_key_ttl_is_sliding_for_entire_queue_log_registry(pg_buses, key):
    a, b = pg_buses
    await a.queue_push(key, {"n": 1}, ttl_secs=1)
    await a.log_append(key, {"n": 1}, ttl_secs=1)
    await a.registry_set(key, "f", "1", ttl_secs=1)
    await asyncio.sleep(0.65)
    await b.queue_push(key, {"n": 2}, ttl_secs=1)
    await b.log_append(key, {"n": 2}, ttl_secs=1)
    await b.registry_set(key, "g", "2", ttl_secs=1)
    await asyncio.sleep(0.5)
    assert len(await a.log_read(key)) == 2
    assert await a.registry_getall(key) == {"f": "1", "g": "2"}
    assert len(await a.queue_drain(key)) == 2
    await asyncio.sleep(0.6)
    assert await a.log_read(key) == []
    assert await a.registry_getall(key) == {}
    await a.queue_push(key, {"n": 3}, ttl_secs=1)
    await asyncio.sleep(1.05)
    assert await b.queue_drain(key) == []


async def test_pg_registry_cas_pop_and_namespace_expiry(pg_buses, key):
    a, b = pg_buses
    await a.registry_set(key, "f", "old")
    changed = await asyncio.gather(
        a.registry_set_if(key, "f", "a", expected="old"),
        b.registry_set_if(key, "f", "b", expected="old"),
    )
    assert sorted(changed) == [False, True]
    assert await b.registry_exists(key, "f")
    popped = await asyncio.gather(a.registry_pop(key, "f"), b.registry_pop(key, "f"))
    assert sum(x is not None for x in popped) == 1
    assert await a.registry_get(key, "f") is None
    assert not await b.registry_set_if(key, "f", "x", expected="old")
    await a.registry_set(key, "x", "1")
    await b.registry_del(key, "x")
    assert not await a.registry_exists(key, "x")
    await a.registry_set(key, "y", "2")
    await b.registry_drop(key)
    assert await a.registry_getall(key) == {}


async def test_pg_lease_stale_owner_cannot_release_successor(pg_buses, key):
    a, b = pg_buses
    assert await a.try_lock(key, ttl_secs=1)
    assert not await b.try_lock(key, ttl_secs=1)
    await b.unlock(key)
    assert await a.is_locked(key)
    await asyncio.sleep(1.05)
    assert await b.try_lock(key, ttl_secs=2)
    await a.unlock(key)
    assert await a.is_locked(key)
    await b.unlock(key)
    assert not await a.is_locked(key)


async def test_pg_acquire_renews_and_excludes_other_processes(pg_buses, key):
    a, b = pg_buses
    async with a.acquire_lock(key, ttl_secs=1):
        await asyncio.sleep(1.4)
        assert await b.is_locked(key)
        assert not await b.try_lock(key)
    assert await b.try_lock(key)
    await b.unlock(key)


async def test_pg_lease_loss_cancels_body_and_surfaces_error(pg_buses, pg_engine, key):
    a, _ = pg_buses
    entered = asyncio.Event()

    async def holder():
        async with a.acquire_lock(key, ttl_secs=1):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(holder())
    await entered.wait()
    async with pg_engine.begin() as conn:
        await conn.execute(
            text("DELETE FROM egis_bus_leases WHERE key=:key"), {"key": key}
        )
    with pytest.raises(LeaseLostError):
        await asyncio.wait_for(task, 2)


async def test_bus_does_not_dispose_supplied_engine(pg_buses, pg_engine):
    await pg_buses[0].aclose()
    async with pg_engine.connect() as conn:
        assert await conn.scalar(text("SELECT 1")) == 1


async def test_standalone_subscription_close_releases_inner_immediately(key):
    async with EgisMessageBus() as bus:
        ready = asyncio.Event()
        subscription = bus.subscribe(key, on_ready=ready.set)
        waiting = asyncio.create_task(anext(subscription))
        await ready.wait()
        await bus.publish(key, {"event": 1})
        await waiting
        await subscription.aclose()
        assert len(bus.delegate._subscribers.get(key, ())) == 0


async def test_pg_expiry_sweeper_does_not_starve_after_first_batch(
    pg_buses, pg_engine, key
):
    a, _ = pg_buses
    async with pg_engine.begin() as conn:
        await conn.execute(
            text("""INSERT INTO egis_bus_keys(kind,key,seq,expires_at)
            SELECT 'queue', :prefix || n::text, 1, clock_timestamp()-interval '1 second'
            FROM generate_series(1,501) AS n"""),
            {"prefix": key},
        )
        await conn.execute(
            text("""INSERT INTO egis_bus_entries(kind,key,seq,payload)
            SELECT 'queue', :prefix || n::text, 1, '{}'::jsonb
            FROM generate_series(1,501) AS n"""),
            {"prefix": key},
        )
    for _ in range(4):
        await a._pg.sweep_expired()
    async with pg_engine.connect() as conn:
        assert (
            await conn.scalar(
                text("SELECT count(*) FROM egis_bus_entries WHERE key LIKE :prefix"),
                {"prefix": key + "%"},
            )
            == 0
        )


async def test_close_cancels_owner_after_nested_lease_exits(pg_buses, key):
    a, _ = pg_buses
    entered = asyncio.Event()

    async def holder():
        async with a.acquire_lock(key):
            async with a.acquire_lock(key + ":inner"):
                pass
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(holder())
    try:
        await entered.wait()
        await a.aclose()
        assert task.done()
        assert task.cancelled()
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


async def test_lease_loss_is_reported_even_when_release_connection_hangs(
    pg_buses, key, monkeypatch
):
    a, _ = pg_buses
    entered = asyncio.Event()

    async def lost(*_):
        return False

    async def hung_release(*_):
        await asyncio.Event().wait()

    monkeypatch.setattr(a._pg, "renew", lost)
    monkeypatch.setattr(a._pg, "release", hung_release)

    async def holder():
        async with a.acquire_lock(key, ttl_secs=1):
            entered.set()
            await asyncio.Event().wait()

    task = asyncio.create_task(holder())
    await entered.wait()
    with pytest.raises(LeaseLostError):
        await asyncio.wait_for(task, 2)


async def test_expiring_claim_does_not_retain_finished_owner_task(pg_buses, key):
    import gc
    import weakref

    a, _ = pg_buses
    task = asyncio.create_task(a.try_lock(key, ttl_secs=1))
    assert await task
    reference = weakref.ref(task)
    del task
    await asyncio.sleep(0)  # release asyncio's completed-task callback references
    gc.collect()
    assert reference() is None


async def test_standalone_health_follows_context_lifecycle():
    bus = EgisMessageBus()
    assert bus.health() == {
        "is_ready": False,
        "deployment_mode": "standalone",
        "is_closed": False,
    }
    await bus.__aenter__()
    assert bus.health()["is_ready"]
    await bus.aclose()
    assert bus.health() == {
        "is_ready": False,
        "deployment_mode": "standalone",
        "is_closed": True,
    }
