"""Native MessageBus adapter; only the global trigger queue is Kafka grouped."""

from __future__ import annotations

import asyncio
import logging
from contextlib import aclosing, asynccontextmanager
from uuid import uuid4
from weakref import WeakKeyDictionary

from agentscope.app.message_bus import InMemoryMessageBus, MessageBus, MessageBusKeys

from egis_agentkit.storage.message_bus import PostgresPrimitives

logger = logging.getLogger(__name__)


class LeaseLostError(RuntimeError):
    """The distributed lease was lost; protected work was cancelled."""


class EgisMessageBus(MessageBus):
    """Supply an externally-owned async PG engine for cluster mode.

    ``migrate`` and ``ensure_topics`` are explicit provisioning operations.
    The bus never disposes the supplied engine. Standalone delegates to the
    unmodified native InMemoryMessageBus, including its documented limits.
    """

    def __init__(
        self,
        *,
        deployment_mode="standalone",
        engine=None,
        kafka_bootstrap_servers="localhost:9092",
        topic_prefix="egis-agentkit",
        consumer_group="egis-agentkit-wakeup-v1",
        node_id=None,
        buffer_size=256,
        kafka_options=None,
    ):
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
        self._kafka_args = dict(
            bootstrap_servers=kafka_bootstrap_servers,
            topic_prefix=topic_prefix,
            consumer_group=consumer_group,
            node_id=node_id or uuid4().hex,
            buffer_size=buffer_size,
            options=kafka_options or {},
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
            state["is_ready"] = state["is_ready"] and kafka["is_ready"]
        return state

    async def migrate(self):
        """Create only additive bus tables/indexes; never auto-run on startup."""
        if self._pg is not None:
            await self._pg.migrate()

    async def ensure_topics(self, *, partitions=8, replication_factor=1):
        """Explicitly provision topics. Production may provision them externally."""
        if self.delegate is None:
            from egis_agentkit.integration.kafka import KafkaTransport

            await KafkaTransport.ensure_topics(
                **self._kafka_args,
                partitions=partitions,
                replication_factor=replication_factor,
            )

    async def __aenter__(self):
        if self._closed:
            raise RuntimeError("closed bus cannot be reopened")
        if self.delegate is not None:
            await self.delegate.__aenter__()
        elif self._transport is None:
            from egis_agentkit.integration.kafka import KafkaTransport

            transport = KafkaTransport(self._pg, **self._kafka_args)
            try:
                await transport.start()
            except BaseException:
                await transport.close()
                raise
            self._transport = transport
            self._maintenance = asyncio.create_task(
                self._sweep(), name="egis-bus-expiry"
            )
        self._opened = True
        return self

    async def _sweep(self):
        while True:
            await asyncio.sleep(60)
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
        if self._transport is not None:
            await self._transport.close()
        for claims in list(self._claims.values()):
            for key, token in list(claims.items()):
                try:
                    async with asyncio.timeout(5):
                        await self._pg.release(key, token)
                except Exception as exc:
                    logger.error(
                        "Bus lease release failed; lease will expire (%s)",
                        type(exc).__name__,
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

    async def queue_delete(self, key):
        if self.delegate is None and key == MessageBusKeys.wakeup_queue():
            await self._pg.trigger_state(
                self._kafka_args["topic_prefix"] + ".triggers", delete=True
            )
            return
        await self._primitives.queue_delete(key)

    async def log_append(self, key, payload, *, ttl_secs=None, max_len=None):
        return await self._primitives.log_append(
            key, payload, ttl_secs=ttl_secs, max_len=max_len
        )

    async def log_read(self, key, since=None, max_count=100):
        return await self._primitives.log_read(key, since, max_count)

    async def log_trim(self, key, before_id=None):
        await self._primitives.log_trim(key, before_id)

    async def publish(self, key, payload):
        if self.delegate is not None:
            await self.delegate.publish(key, payload)
        elif key != MessageBusKeys.wakeup_signal():
            await self._kafka().publish(key, payload)
        # Wakeup compatibility signal is deliberately a no-op. The Kafka
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
            except Exception as exc:
                logger.error(
                    "Bus lease release failed; lease will expire (%s)",
                    type(exc).__name__,
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

    async def registry_exists(self, namespace, field):
        return await self._primitives.registry_exists(namespace, field)

    async def registry_getall(self, namespace):
        return await self._primitives.registry_getall(namespace)

    async def registry_get(self, namespace, field):
        return await self._primitives.registry_get(namespace, field)

    async def registry_drop(self, namespace):
        await self._primitives.registry_drop(namespace)
