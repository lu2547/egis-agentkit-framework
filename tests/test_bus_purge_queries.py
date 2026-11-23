"""Cluster deletion clears session bus state in one bounded transaction."""

import asyncio
import json
from datetime import datetime, timedelta, timezone

import pytest
from agentscope.app.message_bus import MessageBusKeys
from redis_helpers import redis_bus as redis_bus
from redis_helpers import redis_buses as redis_buses
from redis_helpers import redis_config as redis_config
from redis_helpers import redis_key as redis_key
from sqlalchemy import text
from test_session_sql_queries import sql_queries

from egis_agentkit.core.message_bus import EgisMessageBus
from egis_agentkit.service.project._project_manager import purge_session_bus
from egis_agentkit.service.session._steering_projection import (
    steering_presentation_namespace,
)


def drop_namespaces(sid):
    return [
        MessageBusKeys.chat_input_payloads(sid),
        MessageBusKeys.chat_input_claim(sid),
        MessageBusKeys.inbox_consumer(sid),
        MessageBusKeys.bg_tasks(sid),
        MessageBusKeys.session_event_checkpoint(sid),
        MessageBusKeys.projection_namespace(sid),
        steering_presentation_namespace(sid),
    ]


async def seed_bus(storage, sids):
    expiry = datetime.now(timezone.utc) + timedelta(minutes=5)
    keys, entries, registry = {}, [], []
    for sid in sids:
        for kind, key in [
            ("queue", MessageBusKeys.inbox(sid)),
            ("queue", MessageBusKeys.chat_inputs(sid)),
            ("log", MessageBusKeys.session_events(sid)),
        ]:
            keys[kind, key] = {"kind": kind, "key": key, "seq": 13, "expiry": expiry}
            entries.append(
                {
                    "kind": kind,
                    "key": key,
                    "seq": 13,
                    "payload": json.dumps({"id": "old-" + sid, "session_id": sid}),
                }
            )
        for namespace in drop_namespaces(sid):
            keys["registry", namespace] = {
                "kind": "registry",
                "key": namespace,
                "seq": 0,
                "expiry": expiry,
            }
            registry.append(
                {"key": namespace, "field": "payload", "value": "session-state"}
            )
        owner = "bob" if sid.endswith("foreign") else "alice"
        pending = (
            "not-json" if sid.endswith("invalid") else json.dumps({"user_id": owner})
        )
        for namespace, value in [
            (MessageBusKeys.chat_input_pending_registry(), pending),
            (MessageBusKeys.chat_input_inflight_registry(), "inflight"),
            (MessageBusKeys.chat_active_reply_registry(), "reply"),
            (MessageBusKeys.chat_input_user_sessions(owner), "indexed"),
        ]:
            keys["registry", namespace] = {
                "kind": "registry",
                "key": namespace,
                "seq": 0,
                "expiry": expiry,
            }
            registry.append({"key": namespace, "field": sid, "value": value})
    async with storage.engine.begin() as conn:
        await conn.execute(
            text(
                "INSERT INTO ak_bus_keys(kind,key,seq,expires_at) VALUES(:kind,:key,:seq,:expiry) ON CONFLICT DO NOTHING"
            ),
            list(keys.values()),
        )
        await conn.execute(
            text(
                "INSERT INTO ak_bus_entries(kind,key,seq,payload) VALUES(:kind,:key,:seq,CAST(:payload AS jsonb))"
            ),
            entries,
        )
        await conn.execute(
            text(
                "INSERT INTO ak_bus_registry(key,field,value) VALUES(:key,:field,:value)"
            ),
            registry,
        )
    return expiry


@pytest.mark.parametrize("count", [1, 50])
async def test_cluster_session_purge_has_constant_sql_and_preserves_unrelated_state(
    storage, redis_bus, redis_key, count
):
    bus = redis_bus
    sids = [f"{redis_key}-remove-{i}" for i in range(count)]
    unrelated = f"{redis_key}-unrelated"
    expiry = await seed_bus(storage, [*sids, unrelated])
    for sid in [*sids, unrelated]:
        await bus.log_append(MessageBusKeys.session_events(sid), {"live": sid})
    assert await bus.try_lock(MessageBusKeys.session_lock(sids[0]))
    assert await bus.try_lock(MessageBusKeys.session_lock(unrelated))
    try:
        with sql_queries(storage) as statements:
            assert callable(getattr(bus, "purge_sessions", None)), (
                "cluster session purge must expose a batch operation"
            )
            await purge_session_bus(bus, sids)
        print(f"BUS_PURGE_SQL sessions={count} statements={len(statements)}")
        assert len(statements) <= 8, (
            f"purging {count} sessions executed {len(statements)} SQLs"
        )
        for sid in sids:
            assert await bus.queue_read(MessageBusKeys.inbox(sid)) == []
            assert await bus.queue_read(MessageBusKeys.chat_inputs(sid)) == []
            replay_key = MessageBusKeys.session_events(sid)
            assert await bus.log_read(replay_key) == []
            assert await bus._pg.log_read(replay_key) == [
                ("13-0", {"id": "old-" + sid, "session_id": sid})
            ]
            assert (
                await bus.registry_get(
                    MessageBusKeys.chat_input_pending_registry(), sid
                )
                is None
            )
            assert (
                await bus.registry_get(
                    MessageBusKeys.chat_input_inflight_registry(), sid
                )
                is None
            )
            assert (
                await bus.registry_get(MessageBusKeys.chat_active_reply_registry(), sid)
                is None
            )
            assert (
                await bus.registry_get(
                    MessageBusKeys.chat_input_user_sessions("alice"), sid
                )
                is None
            )
            for namespace in drop_namespaces(sid):
                assert await bus.registry_getall(namespace) == {}
        assert await bus.is_locked(MessageBusKeys.session_lock(sids[0]))
        assert await bus.is_locked(MessageBusKeys.session_lock(unrelated))
        assert await bus.queue_read(MessageBusKeys.chat_inputs(unrelated))
        assert await bus.log_read(MessageBusKeys.session_events(unrelated))
        assert await bus.registry_get(
            MessageBusKeys.chat_input_pending_registry(), unrelated
        )
        assert (
            await bus.registry_get(
                MessageBusKeys.chat_input_user_sessions("alice"), unrelated
            )
            == "indexed"
        )
        assert (
            await bus.registry_get(
                MessageBusKeys.chat_input_inflight_registry(), unrelated
            )
            == "inflight"
        )
        assert (
            await bus.registry_get(
                MessageBusKeys.chat_active_reply_registry(), unrelated
            )
            == "reply"
        )
        async with storage.engine.connect() as conn:
            registry_expiry = (
                await conn.scalars(
                    text(
                        "SELECT expires_at FROM ak_bus_keys WHERE kind='registry' AND key=ANY(CAST(:keys AS text[]))"
                    ),
                    {
                        "keys": [
                            MessageBusKeys.chat_input_pending_registry(),
                            MessageBusKeys.chat_input_inflight_registry(),
                            MessageBusKeys.chat_active_reply_registry(),
                            MessageBusKeys.chat_input_user_sessions("alice"),
                        ]
                    },
                )
            ).all()
        assert registry_expiry == [expiry] * 4

        async with storage.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT kind,key,seq,expires_at FROM ak_bus_keys WHERE key=ANY(CAST(:keys AS text[]))"
                        ),
                        {
                            "keys": [
                                MessageBusKeys.inbox(sids[0]),
                                MessageBusKeys.chat_inputs(sids[0]),
                                MessageBusKeys.session_events(sids[0]),
                            ]
                        },
                    )
                )
                .mappings()
                .all()
            )
        assert all(row["seq"] == 13 for row in rows)
        assert all(row["expires_at"] is None for row in rows if row["kind"] == "queue")
        assert next(row["expires_at"] for row in rows if row["kind"] == "log") == expiry
        await purge_session_bus(bus, sids)
        assert (
            await bus.queue_push(MessageBusKeys.chat_inputs(sids[0]), {"id": "new"})
            == "14-0"
        )
        assert not await bus.queue_remove_if(
            MessageBusKeys.chat_inputs(sids[0]), [{"id": "old-" + sids[0]}]
        )
        assert await bus.queue_read(MessageBusKeys.chat_inputs(sids[0])) == [
            ("14-0", {"id": "new"})
        ]
    finally:
        await bus.unlock(MessageBusKeys.session_lock(sids[0]))
        await bus.unlock(MessageBusKeys.session_lock(unrelated))


async def test_batch_purge_parses_foreign_and_invalid_pending_owners_without_erasing_other_fields(
    storage,
):
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus._pg.init_schema()
    await seed_bus(storage, ["remove-foreign", "remove-invalid", "unrelated-foreign"])
    await bus._pg.purge_sessions(["remove-foreign", "remove-invalid"])
    assert (
        await bus.registry_get(
            MessageBusKeys.chat_input_user_sessions("bob"), "remove-foreign"
        )
        is None
    )
    assert (
        await bus.registry_get(
            MessageBusKeys.chat_input_user_sessions("bob"), "unrelated-foreign"
        )
        == "indexed"
    )
    assert (
        await bus.registry_get(
            MessageBusKeys.chat_input_pending_registry(), "remove-invalid"
        )
        is None
    )
    # Invalid ownership leaves unrelated owner indexes untouched, matching cleanup_session.
    assert (
        await bus.registry_get(
            MessageBusKeys.chat_input_user_sessions("alice"), "remove-invalid"
        )
        == "indexed"
    )


async def test_concurrent_old_queue_removal_cannot_delete_post_purge_append(storage):
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus._pg.init_schema()
    await seed_bus(storage, ["remove"])
    key = MessageBusKeys.chat_inputs("remove")
    async with storage.engine.begin() as conn:
        await conn.execute(
            text(
                "SELECT 1 FROM ak_bus_keys WHERE kind='queue' AND key=:key FOR UPDATE"
            ),
            {"key": key},
        )
        purge = asyncio.create_task(bus._pg.purge_sessions(["remove"]))
        await asyncio.sleep(0.02)
        stale_remove = asyncio.create_task(
            bus.queue_remove_if(key, [{"id": "old-remove"}])
        )
    await purge
    await bus.queue_push(key, {"id": "new"})
    await stale_remove
    assert await bus.queue_read(key) == [("14-0", {"id": "new"})]


async def test_purge_preserves_expired_metadata_without_resurrecting_global_fields(
    storage,
):
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus._pg.init_schema()
    await seed_bus(storage, ["remove", "unrelated"])
    past = datetime.now(timezone.utc) - timedelta(seconds=10)
    globals_ = [
        MessageBusKeys.chat_input_pending_registry(),
        MessageBusKeys.chat_input_inflight_registry(),
        MessageBusKeys.chat_active_reply_registry(),
    ]
    expired_keys = [
        *globals_,
        MessageBusKeys.chat_inputs("remove"),
        MessageBusKeys.session_events("remove"),
        *drop_namespaces("remove"),
    ]
    async with storage.engine.begin() as conn:
        await conn.execute(
            text(
                "UPDATE ak_bus_keys SET expires_at=:past WHERE key=ANY(CAST(:keys AS text[]))"
            ),
            {"past": past, "keys": expired_keys},
        )
    await bus._pg.purge_sessions(
        ["remove"],
        extra_registry_namespaces=[steering_presentation_namespace("remove")],
    )
    async with storage.engine.connect() as conn:
        remaining = (
            await conn.execute(
                text(
                    "SELECT key,field FROM ak_bus_registry WHERE key=ANY(CAST(:keys AS text[]))"
                ),
                {"keys": globals_},
            )
        ).all()
        metadata = dict(
            (
                await conn.execute(
                    text(
                        "SELECT key,expires_at FROM ak_bus_keys WHERE key=ANY(CAST(:keys AS text[]))"
                    ),
                    {"keys": expired_keys},
                )
            ).all()
        )
    assert remaining == []  # expired global namespaces clear stale invisible fields
    assert all(metadata[key] == past for key in globals_)
    assert metadata[MessageBusKeys.session_events("remove")] == past
    assert metadata[MessageBusKeys.chat_inputs("remove")] is None
    assert all(metadata[key] is None for key in drop_namespaces("remove"))
    # Expired pending-owner metadata must not be used to erase a live user index.
    assert (
        await bus.registry_get(
            MessageBusKeys.chat_input_user_sessions("alice"), "remove"
        )
        == "indexed"
    )
    assert (
        await bus.registry_get(
            MessageBusKeys.chat_input_user_sessions("alice"), "unrelated"
        )
        == "indexed"
    )
    assert (
        await bus._pg.log_append(
            MessageBusKeys.session_events("remove"), {"type": "after"}
        )
        == "14-0"
    )
    assert await bus._pg.log_read(
        MessageBusKeys.session_events("remove"), since="13-0"
    ) == [("14-0", {"type": "after"})]


async def test_pending_owner_change_retries_with_canonical_key_locks(storage):
    from sqlalchemy import event

    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus._pg.init_schema()
    await seed_bus(storage, ["remove", "unrelated-foreign"])
    pending = MessageBusKeys.chat_input_pending_registry()
    bob = MessageBusKeys.chat_input_user_sessions("bob")
    reached_lock = asyncio.Event()

    def observe(_conn, _cursor, statement, _params, _context, _many):
        if "FOR UPDATE OF k" in statement:
            reached_lock.set()

    async with storage.engine.begin() as conn:
        await conn.execute(
            text(
                "SELECT 1 FROM ak_bus_keys WHERE kind='registry' AND key=:key FOR UPDATE"
            ),
            {"key": pending},
        )
        event.listen(storage.engine.sync_engine, "before_cursor_execute", observe)
        operation = asyncio.create_task(bus._pg.purge_sessions(["remove"]))
        try:
            await asyncio.wait_for(reached_lock.wait(), 5)
            await conn.execute(
                text(
                    "UPDATE ak_bus_registry SET value=:value WHERE key=:key AND field='remove'"
                ),
                {"key": pending, "value": json.dumps({"user_id": "bob"})},
            )
            await conn.execute(
                text(
                    "INSERT INTO ak_bus_registry(key,field,value) VALUES(:key,'remove','indexed')"
                ),
                {"key": bob},
            )
        finally:
            event.remove(storage.engine.sync_engine, "before_cursor_execute", observe)
    await asyncio.wait_for(operation, 5)
    assert await bus.registry_get(pending, "remove") is None
    assert await bus.registry_get(bob, "remove") is None
    assert await bus.registry_get(bob, "unrelated-foreign") == "indexed"
    assert (
        await bus.registry_get(
            MessageBusKeys.chat_input_user_sessions("alice"), "remove"
        )
        == "indexed"
    )


async def test_standalone_batch_purge_uses_native_public_operations():
    async with EgisMessageBus(deployment_mode="standalone") as bus:
        await bus.queue_push(MessageBusKeys.chat_inputs("remove"), {"id": "old"})
        await bus.registry_set(
            MessageBusKeys.chat_input_pending_registry(),
            "remove",
            json.dumps({"user_id": "alice"}),
        )
        await bus.registry_set(
            MessageBusKeys.chat_input_user_sessions("alice"), "remove", "indexed"
        )
        await bus.registry_set(
            MessageBusKeys.chat_input_user_sessions("alice"), "unrelated", "indexed"
        )
        await bus.registry_set(steering_presentation_namespace("remove"), "x", "hint")
        assert await bus.try_lock(MessageBusKeys.session_lock("remove"))
        await purge_session_bus(bus, ["remove"])
        assert await bus.queue_read(MessageBusKeys.chat_inputs("remove")) == []
        assert (
            await bus.registry_get(
                MessageBusKeys.chat_input_user_sessions("alice"), "remove"
            )
            is None
        )
        assert (
            await bus.registry_get(
                MessageBusKeys.chat_input_user_sessions("alice"), "unrelated"
            )
            == "indexed"
        )
        assert (
            await bus.registry_getall(steering_presentation_namespace("remove")) == {}
        )
        assert await bus.is_locked(MessageBusKeys.session_lock("remove"))


@pytest.mark.parametrize("count", [1, 50])
async def test_real_cluster_project_delete_batches_bus_purge_after_all_session_locks(
    storage, tmp_path, redis_bus, count
):
    from test_domain import expert, identities

    from egis_agentkit.service.agent._agent_manager import AgentManager
    from egis_agentkit.service.session._session import SessionService

    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    tasks = SessionService(storage, tmp_path)
    project = await tasks.projects.create(alice, "删除项目")
    session_ids = [
        (
            await tasks.add_session(
                alice, source["id"], "private", project_id=project["id"]
            )
        )["session_id"]
        for _ in range(count)
    ]
    keep = await tasks.add_session(alice, source["id"], "private")
    bus = redis_bus
    await seed_bus(storage, [*session_ids, keep["session_id"]])
    replay_keys = [
        MessageBusKeys.session_events(sid) for sid in [*session_ids, keep["session_id"]]
    ]
    try:
        for key in replay_keys:
            await bus.log_append(key, {"live": True})
        with sql_queries(storage) as statements:
            await tasks.projects.delete(alice, project["id"], bus)
        purge_queries = [
            statement
            for statement in statements
            if "ak_bus_" in statement and "ak_bus_leases" not in statement
        ]
        print(
            f"PROJECT_PURGE_SQL sessions={count} purge={len(purge_queries)} "
            f"total_with_leases={len(statements)}"
        )
        assert len(purge_queries) == 8
        assert await storage.projects.get(alice.user_id, project["id"]) is None
        assert await storage.sessions.in_project(alice.user_id, project["id"]) == []
        for key in replay_keys[:-1]:
            assert await bus.log_read(key) == []
        assert await bus.log_read(replay_keys[-1])
        assert await bus.queue_read(MessageBusKeys.chat_inputs(keep["session_id"]))
        assert (await tasks.get_session(alice, keep["session_id"]))["session_id"] == keep[
            "session_id"
        ]
    finally:
        for key in replay_keys:
            await bus.log_trim(key)
