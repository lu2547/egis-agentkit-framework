"""实现原生 MessageBus，协调 PostgreSQL、Kafka 传输和 Redis 会话回放。"""

from __future__ import annotations

import asyncio
import logging
import os
from contextlib import aclosing, asynccontextmanager
from uuid import uuid4
from weakref import WeakKeyDictionary

from agentscope.app._chat_input_store import cleanup_session
from agentscope.app.message_bus import (
    InMemoryMessageBus,
    MessageBus,
    MessageBusKeys,
    RedisMessageBus,
)
from redis.exceptions import RedisError

from egis_agentkit.client.nacos._settings import KafkaConfig, RedisConfig
from egis_agentkit.storage._message_bus import PostgresPrimitives

logger = logging.getLogger(__name__)


def _stream_id_order(cursor: str) -> tuple[int, int]:
    milliseconds, _, sequence = cursor.partition("-")
    return int(milliseconds), int(sequence or "0")


class LeaseLostError(RuntimeError):
    """The distributed lease was lost; protected work was cancelled."""


class EgisMessageBus(MessageBus):
    """Supply an externally-owned async PG engine for cluster mode.

    Redis serves cluster session replay. ``init_schema`` and ``ensure_topics``
    remain explicit provisioning operations. Standalone delegates to the
    native InMemoryMessageBus.
    """

    def __init__(
        self,
        *,
        deployment_mode=None,
        engine=None,
        kafka_bootstrap_servers=None,
        topic_prefix=None,
        consumer_group=None,
        node_id=None,
        buffer_size=None,
        kafka_options=None,
        kafka: KafkaConfig | None = None,
        redis: RedisConfig | None = None,
    ):
        self._kafka_config = kafka or KafkaConfig()
        self._redis_config = redis or RedisConfig()
        connection = self._kafka_config.connection
        if deployment_mode is None:
            deployment_mode = connection.deployment_mode
        if buffer_size is None:
            buffer_size = self._kafka_config.transport.buffer_size
        if deployment_mode not in ("standalone", "cluster"):
            raise ValueError("deployment_mode must be standalone or cluster")
        if deployment_mode == "cluster" and engine is None:
            raise ValueError(
                "cluster mode requires an externally owned async PG engine"
            )
        if buffer_size <= 0:
            raise ValueError("buffer_size must be positive")
        self.deployment_mode = deployment_mode
        self.delegate = (
            InMemoryMessageBus() if deployment_mode == "standalone" else None
        )
        self._pg = (
            PostgresPrimitives(engine)
            if engine is not None and self.delegate is None
            else None
        )
        self._transport = None
        self._replay: RedisMessageBus | None = None
        self._kafka_args = dict(
            bootstrap_servers=(
                connection.bootstrap_servers
                if kafka_bootstrap_servers is None
                else kafka_bootstrap_servers
            ),
            topic_prefix=connection.topic_prefix
            if topic_prefix is None
            else topic_prefix,
            consumer_group=connection.consumer_group
            if consumer_group is None
            else consumer_group,
            node_id=node_id or uuid4().hex,
            buffer_size=buffer_size,
            options=kafka_options or {},
            transport=self._kafka_config.transport,
            broadcast=self._kafka_config.broadcast,
        )
        self._claims = WeakKeyDictionary()
        self._leases = {}
        self._maintenance = None
        self._closed = False
        self._opened = False

    @property
    def _primitives(self):
        return self.delegate if self.delegate is not None else self._pg

    def health(self) -> dict:
        """Local lifecycle/task readiness only; contains no connection secrets.

        Combine with a database/network probe for end-to-end readiness.
        Fatal receiver or sender termination always makes this unready.
        """
        state = {
            "is_ready": self._opened and not self._closed,
            "deployment_mode": self.deployment_mode,
            "is_closed": self._closed,
        }
        if self.delegate is None:
            kafka = (
                self._transport.health()
                if self._transport is not None
                else {
                    "is_ready": False,
                    "producer_is_running": False,
                    "wakeup_receiver_is_running": False,
                    "broadcast_receiver_is_running": False,
                    "consumers_are_open": False,
                    "has_background_error": False,
                    "subscriptions_are_ready": False,
                }
            )
            state["kafka"] = kafka
            replay_ready = self._replay is not None and not self._closed
            state["redis"] = {"is_ready": replay_ready}
            state["is_ready"] = (
                state["is_ready"] and kafka["is_ready"] and replay_ready
            )
        return state

    async def init_schema(self):
        """Create only additive bus tables/indexes; never auto-run on startup."""
        if self._pg is not None:
            await self._pg.init_schema()

    async def ensure_topics(self, *, partitions=None, replication_factor=None):
        """Explicitly provision topics. Production may provision them externally."""
        if self.delegate is None:
            from egis_agentkit.client._kafka import KafkaTransport

            topics = self._kafka_config.topics
            await KafkaTransport.ensure_topics(
                **self._kafka_args,
                partitions=topics.partitions if partitions is None else partitions,
                replication_factor=(
                    topics.replication_factor
                    if replication_factor is None
                    else replication_factor
                ),
            )

    async def __aenter__(self):
        if self._closed:
            raise RuntimeError("closed bus cannot be reopened")
        if self.delegate is not None:
            await self.delegate.__aenter__()
        elif self._transport is None:
            from egis_agentkit.client._kafka import KafkaTransport

            transport = KafkaTransport(self._pg, **self._kafka_args)
            try:
                await self._start_replay()
                await transport.start()
            except BaseException:
                try:
                    await transport.close()
                finally:
                    await self._close_replay()
                raise
            self._transport = transport
            self._maintenance = asyncio.create_task(
                self._sweep(), name="egis-bus-expiry"
            )
        self._opened = True
        return self

    async def _start_replay(self):
        """Open the official Redis backend and verify the connection."""
        if self._replay is not None:
            return
        config = self._redis_config
        password = None
        if config.password_env is not None:
            password = os.environ.get(config.password_env)
            if not password:
                raise RuntimeError(
                    f"Redis password environment variable {config.password_env} is required"
                )
        replay = RedisMessageBus(
            host=config.host,
            port=config.port,
            db=config.db,
            password=password,
            max_connections=config.max_connections,
            socket_connect_timeout=config.connect_timeout_seconds,
            socket_timeout=config.socket_timeout_seconds,
        )
        try:
            await replay.__aenter__()
            async with asyncio.timeout(config.connect_timeout_seconds):
                if not await replay.get_client().ping():
                    raise RuntimeError("Redis replay startup PING failed")
        except BaseException:
            await replay.aclose()
            raise
        self._replay = replay

    async def _close_replay(self):
        if self._replay is not None:
            replay, self._replay = self._replay, None
            await replay.aclose()

    async def _sweep(self):
        while True:
            await asyncio.sleep(self._kafka_config.transport.expiry_sweep_seconds)
            try:
                await self._pg.sweep_expired()
            except Exception:
                logger.exception("Bus expiry cleanup failed; retrying next interval")

    def _kafka(self):
        if self._transport is None or self._closed:
            raise RuntimeError("cluster Kafka operations require an open bus context")
        return self._transport

    async def aclose(self):
        if self._closed:
            return
        self._closed = True
        if self.delegate is not None:
            await self.delegate.aclose()
            return
        if self._maintenance is not None:
            self._maintenance.cancel()
            await asyncio.gather(self._maintenance, return_exceptions=True)
        # Cancel protected tasks first. Their contexts release only their own tokens.
        current = asyncio.current_task()
        owners = {owner for owner in self._leases if owner is not current}
        for owner in owners:
            owner.cancel()
        if owners:
            await asyncio.gather(*owners, return_exceptions=True)
        try:
            if self._transport is not None:
                await self._transport.close()
        finally:
            await self._close_replay()
        for claims in list(self._claims.values()):
            for key, token in list(claims.items()):
                try:
                    async with asyncio.timeout(5):
                        await self._pg.release(key, token)
                except Exception:
                    logger.exception(
                        "Bus lease release failed; lease will expire",
                        extra={"component": "EgisMessageBus.aclose"},
                    )
        self._claims.clear()

    async def queue_push(self, key, payload, *, ttl_secs=None):
        if self.delegate is None and key == MessageBusKeys.wakeup_queue():
            return await self._kafka().push(payload, ttl_secs=ttl_secs)
        return await self._primitives.queue_push(key, payload, ttl_secs=ttl_secs)

    async def queue_drain(self, key, max_count=100):
        if self.delegate is None and key == MessageBusKeys.wakeup_queue():
            return await self._kafka().drain(max_count)
        return await self._primitives.queue_drain(key, max_count)

    async def queue_read(self, key, max_count=100):
        return await self._primitives.queue_read(key, max_count)

    async def queue_replace(self, key, payloads):
        return await self._primitives.queue_replace(key, payloads)

    async def queue_remove_if(self, key, matches):
        return await self._primitives.queue_remove_if(key, matches)

    async def queue_delete(self, key):
        if self.delegate is None and key == MessageBusKeys.wakeup_queue():
            await self._pg.trigger_state(
                self._kafka_args["topic_prefix"] + ".triggers", delete=True
            )
            return
        await self._primitives.queue_delete(key)

    async def clear_rewrite_runtime(
        self, conn, session_ids, *, extra_registry_namespaces=()
    ):
        """在消息历史事务内清理 PG 运行状态，保留聊天凭据与 FIFO。"""
        if self.delegate is not None:
            raise ValueError("单机运行状态应在历史提交后清理")
        await self._pg.clear_rewrite_runtime(
            conn, session_ids, extra_registry_namespaces=extra_registry_namespaces
        )

    @staticmethod
    def replay_rewrite_namespace(session_id: str) -> str:
        return f"egis:session:replay_rewrite:{session_id}"

    @staticmethod
    def rewrite_notice_namespace() -> str:
        return "egis:session:rewrite_pending"

    @staticmethod
    def rewrite_notice_lock(session_id: str) -> str:
        return f"egis:session:rewrite_notice_lock:{session_id}"

    async def capture_replay_tail(self, session_id: str) -> str:
        """Capture the newest Redis replay cursor before rewriting history."""
        if self._pg is None:
            raise ValueError("durable replay boundaries require cluster mode")
        key = MessageBusKeys.session_events(session_id)
        replay = self._log_backend(key)
        entries = await replay.get_client().xrevrange(key, count=1)
        return entries[0][0] if entries else "0-0"

    async def stage_rewrite_replay(
        self, conn, session_id: str, cursor: str, pending_notice: str | None = None
    ) -> None:
        """Commit the exclusive replay boundary and notice with history."""
        if self._pg is None:
            raise ValueError("durable replay boundaries require cluster mode")
        await self._pg.registry_set(
            self.replay_rewrite_namespace(session_id),
            MessageBusKeys.SESSION_EVENT_CURSOR_FIELD,
            cursor,
            conn=conn,
        )
        if pending_notice is not None:
            await self._pg.registry_set(
                self.rewrite_notice_namespace(), session_id, pending_notice, conn=conn
            )

    async def purge_sessions(self, session_ids, *, extra_registry_namespaces=()):
        """一次清理范围内的会话总线数据，单机继续调用原生公共操作。"""
        session_ids = sorted(set(session_ids))
        if self.delegate is None:
            await self._pg.purge_sessions(
                session_ids,
                extra_registry_namespaces=(
                    *extra_registry_namespaces,
                    *(self.replay_rewrite_namespace(sid) for sid in session_ids),
                ),
                extra_field_namespaces=(self.rewrite_notice_namespace(),),
            )
            replay = self._replay
            if replay is not None and not self._closed:
                for sid in session_ids:
                    try:
                        await replay.log_trim(MessageBusKeys.session_events(sid))
                    except RedisError:
                        logger.warning(
                            "Redis replay cleanup failed after session purge",
                            extra={"context": {"session_id": sid}},
                            exc_info=True,
                        )
            return
        for sid in session_ids:
            await self.log_trim(MessageBusKeys.session_events(sid))
            await self.queue_delete(MessageBusKeys.inbox(sid))
            await self.queue_delete(MessageBusKeys.chat_inputs(sid))
            await self.registry_del(MessageBusKeys.chat_input_inflight_registry(), sid)
            await cleanup_session(self, sid)
            await self.registry_del(MessageBusKeys.chat_active_reply_registry(), sid)
            for namespace in (
                MessageBusKeys.inbox_consumer(sid),
                MessageBusKeys.bg_tasks(sid),
                MessageBusKeys.session_event_checkpoint(sid),
                MessageBusKeys.projection_namespace(sid),
            ):
                await self.registry_drop(namespace)
        for namespace in extra_registry_namespaces:
            await self.registry_drop(namespace)

    def _log_backend(self, key):
        if self.delegate is None and key.startswith(
            MessageBusKeys.session_events("")
        ):
            if self._replay is None or self._closed:
                raise RuntimeError("session replay requires an open Redis backend")
            return self._replay
        return self._primitives

    async def log_append(self, key, payload, *, ttl_secs=None, max_len=None):
        backend = self._log_backend(key)
        if backend is self._replay and ttl_secs is None:
            ttl_secs = self._redis_config.retention_seconds
        return await backend.log_append(
            key, payload, ttl_secs=ttl_secs, max_len=max_len
        )

    async def log_read(self, key, since=None, max_count=100):
        backend = self._log_backend(key)
        if backend is self._replay:
            session_id = key[len(MessageBusKeys.session_events("")) :]
            floor = await self._pg.registry_get(
                self.replay_rewrite_namespace(session_id),
                MessageBusKeys.SESSION_EVENT_CURSOR_FIELD,
            )
            if floor is not None and (
                since is None or _stream_id_order(since) < _stream_id_order(floor)
            ):
                since = floor
        return await backend.log_read(key, since, max_count)

    async def log_trim(self, key, before_id=None):
        backend = self._log_backend(key)
        is_checkpoint_trim = backend is self._replay and before_id is not None
        try:
            await backend.log_trim(key, before_id)
        except RedisError:
            if not is_checkpoint_trim:
                raise
            logger.warning("Redis checkpoint replay trim failed", exc_info=True)

    async def publish(self, key, payload):
        if self.delegate is not None:
            await self.delegate.publish(key, payload)
        elif key != MessageBusKeys.wakeup_signal():
            await self._kafka().publish(key, payload)
        # In cluster mode, the native wakeup signal needs no extra publish. The Kafka
        # record itself wakes exactly its assigned node's native Dispatcher.

    async def subscribe(self, key, *, on_ready=None):
        backend = self.delegate if self.delegate is not None else self._kafka()
        async with aclosing(backend.subscribe(key, on_ready=on_ready)) as subscription:
            async for payload in subscription:
                yield payload

    async def try_lock(self, key, *, ttl_secs=600):
        if self.delegate is not None:
            return await self.delegate.try_lock(key, ttl_secs=ttl_secs)
        token = await self._pg.claim(key, ttl_secs)
        if token:
            self._claims.setdefault(asyncio.current_task(), {})[key] = token
        return token is not None

    async def unlock(self, key):
        if self.delegate is not None:
            await self.delegate.unlock(key)
            return
        owner = asyncio.current_task()
        claims = self._claims.get(owner, {})
        token = claims.get(key)
        if token is not None:
            await self._pg.release(key, token)
            claims.pop(key, None)
            if not claims:
                self._claims.pop(owner, None)

    async def is_locked(self, key):
        return await self._primitives.is_locked(key)

    async def locked_keys(self, keys):
        """批量读取当前持有的租约，单机保持原生内存锁的过期语义。"""
        if self.delegate is None:
            return await self._pg.locked_keys(keys)
        return {key for key in keys if await self.delegate.is_locked(key)}

    @asynccontextmanager
    async def acquire_lock(self, key, *, ttl_secs=600):
        if self.delegate is not None:
            async with self.delegate.acquire_lock(key, ttl_secs=ttl_secs):
                yield
            return
        owner = asyncio.current_task()
        token = None
        while token is None:
            if self._closed:
                raise RuntimeError("bus is closed")
            token = await self._pg.claim(key, ttl_secs)
            if token is None:
                await asyncio.sleep(min(0.1, ttl_secs / 4))
        lost = asyncio.Event()
        self._leases[owner] = self._leases.get(owner, 0) + 1

        async def heartbeat():
            try:
                while True:
                    await asyncio.sleep(ttl_secs / 3)
                    # A blocked database must not leave work running beyond its lease.
                    async with asyncio.timeout(ttl_secs / 3):
                        if not await self._pg.renew(key, token, ttl_secs):
                            raise LeaseLostError("lease ownership or expiry changed")
            except asyncio.CancelledError:
                raise
            except Exception:
                logger.exception(
                    "Bus lease renewal failed; cancelling the protected task",
                    extra={"component": "EgisMessageBus.acquire_lock"},
                )
                lost.set()
                owner.cancel()

        renewal = asyncio.create_task(heartbeat(), name="egis-bus-lease-renewal")
        try:
            try:
                yield
            except asyncio.CancelledError:
                if lost.is_set():
                    raise LeaseLostError(
                        "distributed lease lost; protected task cancelled"
                    ) from None
                raise
            if lost.is_set():
                raise LeaseLostError("distributed lease lost")
        finally:
            renewal.cancel()
            await asyncio.gather(renewal, return_exceptions=True)
            remaining = self._leases[owner] - 1
            if remaining:
                self._leases[owner] = remaining
            else:
                self._leases.pop(owner, None)
            try:
                async with asyncio.timeout(min(5, ttl_secs / 3)):
                    await self._pg.release(key, token)
            except Exception:
                logger.exception(
                    "Bus lease release failed; lease will expire",
                    extra={"component": "EgisMessageBus.acquire_lock"},
                )

    async def registry_set(self, namespace, field, value, *, ttl_secs=None):
        await self._primitives.registry_set(namespace, field, value, ttl_secs=ttl_secs)

    async def registry_set_if(
        self, namespace, field, value, *, expected, ttl_secs=None
    ):
        return await self._primitives.registry_set_if(
            namespace, field, value, expected=expected, ttl_secs=ttl_secs
        )

    async def registry_pop(self, namespace, field):
        return await self._primitives.registry_pop(namespace, field)

    async def registry_del(self, namespace, field):
        await self._primitives.registry_del(namespace, field)

    async def registry_del_if(self, namespace, field, *, expected):
        return await self._primitives.registry_del_if(
            namespace, field, expected=expected
        )

    async def registry_exists(self, namespace, field):
        return await self._primitives.registry_exists(namespace, field)

    async def registry_getall(self, namespace):
        return await self._primitives.registry_getall(namespace)

    async def registry_get(self, namespace, field):
        return await self._primitives.registry_get(namespace, field)

    async def registry_drop(self, namespace):
        await self._primitives.registry_drop(namespace)
