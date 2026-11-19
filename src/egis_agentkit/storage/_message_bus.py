"""PostgreSQL primitives. Per-key row locks also serialize replay cursor commits."""

from __future__ import annotations

import json
from contextlib import asynccontextmanager
from uuid import uuid4

from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncEngine

BUS_SCHEMA_LOCK = 5728140031001
BUS_TABLE_SUFFIXES = ("keys", "entries", "registry", "leases", "broadcast_payloads")
BUS_EXPIRY_INDEXES = ("keys", "leases", "broadcast_payloads")


async def rename_bus_tables(conn, *, apply=True) -> list[str]:
    """在显式升级事务中保留总线数据并统一表和索引名称。"""
    if apply:
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(:key)"), {"key": BUS_SCHEMA_LOCK}
        )
    schema = await conn.scalar(text("SELECT current_schema()"))
    existing = set(await conn.run_sync(lambda sync: inspect(sync).get_table_names(schema=schema)))
    quote = conn.dialect.identifier_preparer.quote_identifier
    names = []
    for suffix in BUS_TABLE_SUFFIXES:
        previous, current = f"egis_bus_{suffix}", f"ak_bus_{suffix}"
        if previous not in existing:
            continue
        if current in existing:
            raise ValueError(f"总线新旧表同时存在：{previous}、{current}")
        names.append(previous)
        if not apply:
            continue
        qualified_old = f"{quote(schema)}.{quote(previous)}"
        qualified_new = f"{quote(schema)}.{quote(current)}"
        await conn.execute(text(f"ALTER TABLE {qualified_old} RENAME TO {quote(current)}"))
        await conn.execute(text(
            f"ALTER TABLE {qualified_new} RENAME CONSTRAINT {quote(previous + '_pkey')} "
            f"TO {quote(current + '_pkey')}"
        ))
        if suffix in BUS_EXPIRY_INDEXES:
            await conn.execute(text(
                f"ALTER INDEX IF EXISTS {quote(schema)}.{quote(previous + '_expiry_idx')} "
                f"RENAME TO {quote(current + '_expiry_idx')}"
            ))
    return names


class BroadcastPayloadUnavailableError(LookupError):
    """A live Kafka reference cannot be resolved inside its retention window."""


class PostgresPrimitives:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def init_schema(self):
        statements = [
            """CREATE TABLE IF NOT EXISTS ak_bus_keys (
                kind text NOT NULL, key text NOT NULL, seq bigint NOT NULL DEFAULT 0,
                expires_at timestamptz, PRIMARY KEY(kind,key))""",
            """CREATE TABLE IF NOT EXISTS ak_bus_entries (
                kind text NOT NULL, key text NOT NULL, seq bigint NOT NULL,
                payload jsonb NOT NULL, PRIMARY KEY(kind,key,seq))""",
            """CREATE TABLE IF NOT EXISTS ak_bus_registry (
                key text NOT NULL, field text NOT NULL, value text NOT NULL,
                PRIMARY KEY(key,field))""",
            """CREATE TABLE IF NOT EXISTS ak_bus_leases (
                key text PRIMARY KEY, token text NOT NULL, expires_at timestamptz NOT NULL)""",
            """CREATE TABLE IF NOT EXISTS ak_bus_broadcast_payloads (
                id text PRIMARY KEY, topic text NOT NULL, key text NOT NULL,
                payload jsonb NOT NULL, expires_at timestamptz NOT NULL)""",
            """CREATE INDEX IF NOT EXISTS ak_bus_keys_expiry_idx
                ON ak_bus_keys(expires_at) WHERE expires_at IS NOT NULL""",
            """CREATE INDEX IF NOT EXISTS ak_bus_leases_expiry_idx
                ON ak_bus_leases(expires_at)""",
            """CREATE INDEX IF NOT EXISTS ak_bus_broadcast_payloads_expiry_idx
                ON ak_bus_broadcast_payloads(expires_at)""",
        ]
        async with self.engine.begin() as conn:
            # Serialize schema initialization across application instances.
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(:key)"), {"key": BUS_SCHEMA_LOCK}
            )
            if await rename_bus_tables(conn, apply=False):
                raise ValueError("消息总线存在旧表，请先执行 upgrade-db")
            for statement in statements:
                await conn.execute(text(statement))

    @asynccontextmanager
    async def _key(self, kind, key):
        async with self.engine.begin() as conn:
            args = {"kind": kind, "key": key}
            await conn.execute(
                text("""INSERT INTO ak_bus_keys(kind,key)
                VALUES (:kind,:key) ON CONFLICT DO NOTHING"""),
                args,
            )
            row = (
                (
                    await conn.execute(
                        text("""SELECT seq, expires_at,
                expires_at <= clock_timestamp() AS expired FROM ak_bus_keys
                WHERE kind=:kind AND key=:key FOR UPDATE"""),
                        args,
                    )
                )
                .mappings()
                .one()
            )
            if row["expired"]:
                await self._clear(conn, kind, key)
                # Retain sequence tombstones: expired/deleted log cursors never regress.
            yield conn, row

    async def _clear(self, conn, kind, key):
        if kind == "registry":
            await conn.execute(
                text("DELETE FROM ak_bus_registry WHERE key=:key"), {"key": key}
            )
        else:
            await conn.execute(
                text("DELETE FROM ak_bus_entries WHERE kind=:kind AND key=:key"),
                {"kind": kind, "key": key},
            )

    @staticmethod
    def _ttl(ttl_secs):
        if ttl_secs is not None and ttl_secs <= 0:
            raise ValueError("ttl_secs must be positive")

    @staticmethod
    def _count(count):
        if count <= 0:
            raise ValueError("max_count must be positive")

    @staticmethod
    def _cursor(value):
        try:
            seq, suffix = value.split("-")
            if not seq.isdecimal() or suffix != "0":
                raise ValueError
            return int(seq)
        except (AttributeError, ValueError):
            raise ValueError("cursor must be a non-negative <integer>-0 id") from None

    async def _expiry(self, conn, kind, key, ttl_secs):
        await conn.execute(
            text("""UPDATE ak_bus_keys SET expires_at=
            CASE WHEN CAST(:ttl AS double precision) IS NULL THEN NULL
            ELSE clock_timestamp() + CAST(:ttl AS double precision) * interval '1 second' END
            WHERE kind=:kind AND key=:key"""),
            {"kind": kind, "key": key, "ttl": ttl_secs},
        )

    async def _append(self, kind, key, payload, ttl_secs=None, max_len=None):
        self._ttl(ttl_secs)
        if max_len is not None and max_len <= 0:
            raise ValueError("max_len must be positive")
        serialized = json.dumps(payload, allow_nan=False)
        async with self._key(kind, key) as (conn, row):
            seq = row["seq"] + 1
            await conn.execute(
                text("UPDATE ak_bus_keys SET seq=:seq WHERE kind=:kind AND key=:key"),
                {"kind": kind, "key": key, "seq": seq},
            )
            await self._expiry(conn, kind, key, ttl_secs)
            await conn.execute(
                text("""INSERT INTO ak_bus_entries(kind,key,seq,payload)
                VALUES (:kind,:key,:seq,CAST(:payload AS jsonb))"""),
                {"kind": kind, "key": key, "seq": seq, "payload": serialized},
            )
            if max_len is not None:
                await conn.execute(
                    text("""DELETE FROM ak_bus_entries WHERE kind=:kind
                    AND key=:key AND seq <= :cut"""),
                    {"kind": kind, "key": key, "cut": seq - max_len},
                )
        return f"{seq}-0"

    async def queue_push(self, key, payload, *, ttl_secs=None):
        return await self._append("queue", key, payload, ttl_secs)

    async def queue_drain(self, key, max_count=100):
        self._count(max_count)
        async with self.engine.begin() as conn:
            args = {"key": key, "count": max_count}
            # SHARE permits competing drains, excludes delete/expiry-refresh writers.
            live = await conn.scalar(
                text("""SELECT 1 FROM ak_bus_keys
                WHERE kind='queue' AND key=:key AND
                (expires_at IS NULL OR expires_at>clock_timestamp()) FOR SHARE"""),
                args,
            )
            if not live:
                return []
            rows = (
                await conn.execute(
                    text("""WITH claimed AS (
                SELECT seq FROM ak_bus_entries WHERE kind='queue' AND key=:key
                ORDER BY seq LIMIT :count FOR UPDATE SKIP LOCKED)
                DELETE FROM ak_bus_entries e USING claimed c
                WHERE e.kind='queue' AND e.key=:key AND e.seq=c.seq
                RETURNING e.seq,e.payload"""),
                    args,
                )
            ).all()
        return [(f"{seq}-0", payload) for seq, payload in sorted(rows)]

    async def queue_delete(self, key):
        async with self._key("queue", key) as (conn, _):
            await self._clear(conn, "queue", key)
            await self._expiry(conn, "queue", key, None)

    async def log_append(self, key, payload, *, ttl_secs=None, max_len=None):
        return await self._append("log", key, payload, ttl_secs, max_len)

    async def log_read(self, key, since=None, max_count=100):
        self._count(max_count)
        cursor = self._cursor(since) if since is not None else -1
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("""SELECT e.seq,e.payload FROM ak_bus_entries e
                JOIN ak_bus_keys k ON e.kind=k.kind AND e.key=k.key
                WHERE e.kind='log' AND e.key=:key AND e.seq>:cursor
                AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp())
                ORDER BY e.seq LIMIT :count"""),
                    {"key": key, "cursor": cursor, "count": max_count},
                )
            ).all()
        return [(f"{seq}-0", payload) for seq, payload in rows]

    async def log_trim(self, key, before_id=None):
        cursor = self._cursor(before_id) if before_id is not None else None
        async with self._key("log", key) as (conn, _):
            await conn.execute(
                text("""DELETE FROM ak_bus_entries WHERE kind='log'
                AND key=:key AND (CAST(:cursor AS bigint) IS NULL OR seq<:cursor)"""),
                {"key": key, "cursor": cursor},
            )

    async def broadcast_store(self, topic, key, payload, *, ttl_secs):
        """Commit a short-lived body before publishing its Kafka reference."""
        self._ttl(ttl_secs)
        reference = uuid4().hex
        try:
            async with self.engine.begin() as conn:
                await conn.execute(
                    text("""INSERT INTO ak_bus_broadcast_payloads
                    (id,topic,key,payload,expires_at) VALUES
                    (:id,:topic,:key,CAST(:payload AS jsonb),
                     clock_timestamp()+:ttl*interval '1 second')"""),
                    {
                        "id": reference,
                        "topic": topic,
                        "key": key,
                        "payload": json.dumps(payload, allow_nan=False),
                        "ttl": ttl_secs,
                    },
                )
        except Exception as exc:
            # SQLAlchemy errors can include parameters, including image bytes.
            # Suppress their exception context before native services log them.
            raise RuntimeError(
                f"Kafka broadcast payload storage failed ({type(exc).__name__})"
            ) from None
        return reference

    async def broadcast_read(self, topic, key, reference):
        """Non-destructive read: every live node must resolve the same body."""
        try:
            async with self.engine.connect() as conn:
                payload = await conn.scalar(
                    text("""SELECT payload FROM ak_bus_broadcast_payloads
                    WHERE id=:id AND topic=:topic AND key=:key
                    AND expires_at>clock_timestamp()"""),
                    {"id": reference, "topic": topic, "key": key},
                )
        except Exception as exc:
            raise RuntimeError(
                f"Kafka broadcast payload retrieval failed ({type(exc).__name__})"
            ) from None
        if payload is None:
            raise BroadcastPayloadUnavailableError(
                "Kafka broadcast payload is missing or expired"
            )
        return payload

    async def registry_set(self, namespace, field, value, *, ttl_secs=None):
        self._ttl(ttl_secs)
        async with self._key("registry", namespace) as (conn, _):
            await conn.execute(
                text("""INSERT INTO ak_bus_registry(key,field,value)
                VALUES (:key,:field,:value) ON CONFLICT(key,field) DO UPDATE SET value=:value"""),
                {"key": namespace, "field": field, "value": value},
            )
            await self._expiry(conn, "registry", namespace, ttl_secs)

    async def registry_set_if(
        self, namespace, field, value, *, expected, ttl_secs=None
    ):
        self._ttl(ttl_secs)
        statement = (
            """INSERT INTO ak_bus_registry(key,field,value)
            VALUES (:key,:field,:value) ON CONFLICT(key,field) DO NOTHING RETURNING field"""
            if expected is None
            else """UPDATE ak_bus_registry SET value=:value
            WHERE key=:key AND field=:field AND value=:expected RETURNING field"""
        )
        async with self._key("registry", namespace) as (conn, _):
            result = await conn.execute(
                text(statement),
                {
                    "key": namespace,
                    "field": field,
                    "value": value,
                    "expected": expected,
                },
            )
            changed = result.first() is not None
            if changed:
                await self._expiry(conn, "registry", namespace, ttl_secs)
        return changed

    async def registry_pop(self, namespace, field):
        async with self._key("registry", namespace) as (conn, _):
            return await conn.scalar(
                text("""DELETE FROM ak_bus_registry
                WHERE key=:key AND field=:field RETURNING value"""),
                {"key": namespace, "field": field},
            )

    async def registry_del(self, namespace, field):
        await self.registry_pop(namespace, field)

    async def registry_getall(self, namespace):
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    text("""SELECT r.field,r.value FROM ak_bus_registry r
                JOIN ak_bus_keys k ON k.kind='registry' AND r.key=k.key
                WHERE r.key=:key AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp())"""),
                    {"key": namespace},
                )
            ).all()
        return dict(rows)

    async def registry_get(self, namespace, field):
        async with self.engine.connect() as conn:
            return await conn.scalar(
                text("""SELECT r.value FROM ak_bus_registry r
                JOIN ak_bus_keys k ON k.kind='registry' AND r.key=k.key
                WHERE r.key=:key AND r.field=:field
                AND (k.expires_at IS NULL OR k.expires_at>clock_timestamp())"""),
                {"key": namespace, "field": field},
            )

    async def registry_exists(self, namespace, field):
        return await self.registry_get(namespace, field) is not None

    async def registry_drop(self, namespace):
        async with self._key("registry", namespace) as (conn, _):
            await self._clear(conn, "registry", namespace)
            await self._expiry(conn, "registry", namespace, None)

    async def claim(self, key, ttl_secs):
        self._ttl(ttl_secs)
        token = uuid4().hex
        async with self.engine.begin() as conn:
            result = await conn.scalar(
                text("""INSERT INTO ak_bus_leases(key,token,expires_at)
                VALUES (:key,:token,clock_timestamp()+:ttl*interval '1 second')
                ON CONFLICT(key) DO UPDATE SET token=:token,
                expires_at=clock_timestamp()+:ttl*interval '1 second'
                WHERE ak_bus_leases.expires_at<=clock_timestamp() RETURNING token"""),
                {"key": key, "token": token, "ttl": ttl_secs},
            )
        return result

    async def renew(self, key, token, ttl_secs):
        async with self.engine.begin() as conn:
            result = await conn.scalar(
                text("""UPDATE ak_bus_leases
                SET expires_at=clock_timestamp()+:ttl*interval '1 second'
                WHERE key=:key AND token=:token AND expires_at>clock_timestamp() RETURNING token"""),
                {"key": key, "token": token, "ttl": ttl_secs},
            )
        return result is not None

    async def release(self, key, token):
        async with self.engine.begin() as conn:
            await conn.execute(
                text("DELETE FROM ak_bus_leases WHERE key=:key AND token=:token"),
                {"key": key, "token": token},
            )

    async def is_locked(self, key):
        async with self.engine.connect() as conn:
            return bool(
                await conn.scalar(
                    text("""SELECT 1 FROM ak_bus_leases
                WHERE key=:key AND expires_at>clock_timestamp()"""),
                    {"key": key},
                )
            )

    async def trigger_state(self, key, *, push=False, delete=False, ttl_secs=None):
        self._ttl(ttl_secs)
        async with self._key("trigger", key) as (conn, row):
            generation = row["seq"] + int(bool(delete or (push and row["expired"])))
            if push or delete:
                await conn.execute(
                    text("""UPDATE ak_bus_keys SET seq=:seq
                    WHERE kind='trigger' AND key=:key """),
                    {"key": key, "seq": generation},
                )
                await self._expiry(conn, "trigger", key, ttl_secs)
            return generation, bool(row["expired"] and not push and not delete)

    async def sweep_expired(self):
        # Bounded cleanup with SKIP LOCKED; retain tiny cursor/generation tombstones.
        async with self.engine.begin() as conn:
            keys = (
                await conn.execute(
                    text("""SELECT kind,key FROM ak_bus_keys
                WHERE expires_at<=clock_timestamp() AND (
                    (kind='registry' AND EXISTS (SELECT 1 FROM ak_bus_registry r
                        WHERE r.key=ak_bus_keys.key)) OR
                    (kind IN ('queue','log') AND EXISTS (SELECT 1 FROM ak_bus_entries e
                        WHERE e.kind=ak_bus_keys.kind AND e.key=ak_bus_keys.key)))
                LIMIT 500 FOR UPDATE SKIP LOCKED""")
                )
            ).all()
            for kind, key in keys:
                await self._clear(conn, kind, key)
            await conn.execute(
                text("DELETE FROM ak_bus_leases WHERE expires_at<=clock_timestamp()")
            )
            # Ephemeral bodies have no public cursors and need no tombstones.
            await conn.execute(
                text("""WITH expired AS (
                SELECT id FROM ak_bus_broadcast_payloads
                WHERE expires_at<=clock_timestamp()
                ORDER BY expires_at LIMIT 500 FOR UPDATE SKIP LOCKED)
                DELETE FROM ak_bus_broadcast_payloads p USING expired e
                WHERE p.id=e.id""")
            )
