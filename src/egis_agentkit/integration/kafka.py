"""Bounded local Kafka trigger inbox with ack-on-drain, and per-node fanout.

No Kafka acknowledgement represents completion of an AgentScope run.
"""

from __future__ import annotations

import asyncio
import json
import logging
from collections import deque
from dataclasses import dataclass, field
from uuid import uuid4
from weakref import WeakValueDictionary

from agentscope.app.message_bus import MessageBusKeys
from aiokafka import AIOKafkaConsumer, AIOKafkaProducer, ConsumerRebalanceListener
from aiokafka.admin import AIOKafkaAdminClient, NewTopic
from aiokafka.errors import KafkaError, TopicAlreadyExistsError
from aiokafka.structs import TopicPartition

logger = logging.getLogger(__name__)

# Keep Kafka's default 1 MiB record/request limits. Large transient broadcasts
# use PG bodies, independent of user replay-log trimming and cursor retention.
_INLINE_BROADCAST_BYTES = 512 * 1024
_MAX_BROADCAST_BYTES = 32 * 1024 * 1024
_MAX_BROADCAST_KEY_BYTES = 64 * 1024
_BROADCAST_PAYLOAD_TTL_SECS = 3600
_BROADCAST_STORAGE_TIMEOUT_SECS = 30


@dataclass(eq=False)
class _Subscription:
    queue: asyncio.Queue
    offsets: dict
    closed: asyncio.Event = field(default_factory=asyncio.Event)

    async def deliver(self, payload):
        """Bounded backpressure; closing an abandoned generator unblocks delivery."""
        if self.closed.is_set():
            return
        put = asyncio.create_task(self.queue.put(payload))
        closed = asyncio.create_task(self.closed.wait())
        try:
            await asyncio.wait((put, closed), return_when=asyncio.FIRST_COMPLETED)
        finally:
            for task in (put, closed):
                task.cancel()
            await asyncio.gather(put, closed, return_exceptions=True)


class _Rebalance(ConsumerRebalanceListener):
    def __init__(self, transport, *, broadcast=False):
        self.transport = transport
        self.broadcast = broadcast

    async def on_partitions_revoked(self, revoked):
        if self.broadcast:
            self.transport._broadcast_ready.clear()
            return
        # Never commit records that have merely been fetched into memory.
        # The new owner reads them again from the group's committed offsets.
        async with self.transport._pending_lock:
            self.transport._epoch += 1
            self.transport._assigned.difference_update(revoked)
            self.transport._pending = deque(
                record
                for record in self.transport._pending
                if TopicPartition(record.topic, record.partition) not in revoked
            )
            self.transport._signal.set()

    async def on_partitions_assigned(self, assigned):
        if self.broadcast:
            # Resolve the initial latest offset before on_ready can permit a
            # publisher. Otherwise lazy reset could skip that first publish.
            if assigned:
                await self.transport.broadcast_consumer.seek_to_end(*assigned)
            self.transport._broadcast_ready.set()
            return
        async with self.transport._pending_lock:
            self.transport._assigned.update(assigned)
            self.transport._ready.set()  # also ready for an idle node with no partitions
            self.transport._signal.set()


class KafkaTransport:
    def __init__(
        self,
        pg,
        *,
        bootstrap_servers,
        topic_prefix,
        consumer_group,
        node_id,
        buffer_size,
        options,
    ):
        self.pg = pg
        self.bootstrap_servers = bootstrap_servers
        self.topic = topic_prefix + ".triggers"
        self.broadcast_topic = topic_prefix + ".broadcast"
        self.group = consumer_group
        self.node_id = node_id
        self.capacity = buffer_size
        self.options = dict(options)
        self.producer = None
        self.consumer = None
        self.broadcast_consumer = None
        self._pending = deque()
        self._pending_lock = asyncio.Lock()
        self._assigned = set()
        self._epoch = 0
        self._signal = asyncio.Event()
        self._ready = asyncio.Event()
        self._broadcast_ready = asyncio.Event()
        self._broadcast_lock = asyncio.Lock()
        self._publish_locks = WeakValueDictionary()
        self._subscriptions = {}
        self._tasks = []
        self._closed = False
        self._error = None
        # Security/connection options are common to admin, producer, consumer.
        allowed = {
            "security_protocol",
            "ssl_context",
            "sasl_mechanism",
            "sasl_plain_username",
            "sasl_plain_password",
            "sasl_oauth_token_provider",
            "request_timeout_ms",
            "retry_backoff_ms",
            "api_version",
            "connections_max_idle_ms",
            "metadata_max_age_ms",
        }
        unknown = set(options) - allowed
        if unknown:
            raise ValueError(
                "unsupported Kafka connection option names: "
                + ", ".join(sorted(unknown))
            )

    @staticmethod
    async def ensure_topics(
        *, bootstrap_servers, topic_prefix, partitions, replication_factor, options, **_
    ):
        if partitions <= 0 or replication_factor <= 0:
            raise ValueError("partitions and replication_factor must be positive")
        admin = AIOKafkaAdminClient(bootstrap_servers=bootstrap_servers, **options)
        await admin.start()
        try:
            for topic in (topic_prefix + ".triggers", topic_prefix + ".broadcast"):
                try:
                    await admin.create_topics(
                        [NewTopic(topic, partitions, replication_factor)]
                    )
                except TopicAlreadyExistsError:
                    pass
        finally:
            await admin.close()

    async def start(self):
        common = dict(bootstrap_servers=self.bootstrap_servers, **self.options)
        self.producer = AIOKafkaProducer(**common, enable_idempotence=True, acks="all")
        self.consumer = AIOKafkaConsumer(
            **common,
            group_id=self.group,
            enable_auto_commit=False,
            auto_offset_reset="earliest",
            max_poll_records=self.capacity,
            max_partition_fetch_bytes=1024 * 1024,
            fetch_max_bytes=4 * 1024 * 1024,
        )
        self.consumer.subscribe([self.topic], listener=_Rebalance(self))
        # Every live node gets every broadcast. Fresh group IDs plus latest
        # offsets prevent events published while offline from being replayed.
        self.broadcast_consumer = AIOKafkaConsumer(
            **common,
            group_id=f"{self.group}.broadcast.{self.node_id}.{uuid4().hex}",
            enable_auto_commit=False,
            auto_offset_reset="latest",
            max_poll_records=self.capacity,
            fetch_max_bytes=4 * 1024 * 1024,
        )
        self.broadcast_consumer.subscribe(
            [self.broadcast_topic], listener=_Rebalance(self, broadcast=True)
        )
        await self.producer.start()
        self.producer._sender.sender_task.add_done_callback(self._task_finished)
        await self.consumer.start()
        await self.broadcast_consumer.start()
        self._tasks = [
            asyncio.create_task(self._receive_triggers(), name="egis-kafka-triggers"),
            asyncio.create_task(
                self._receive_broadcasts(), name="egis-kafka-broadcast"
            ),
        ]
        for task in self._tasks:
            task.add_done_callback(self._task_finished)
        async with asyncio.timeout(45):
            await self._ready.wait()
            await self._broadcast_ready.wait()
        self._check_error()

    def health(self) -> dict:
        """Status flags, with no exception text, broker URLs or credentials."""

        def running(task):
            return task is not None and not task.done()

        sender = getattr(getattr(self.producer, "_sender", None), "sender_task", None)
        producer = (
            self.producer is not None and not self.producer._closed and running(sender)
        )
        wakeup = len(self._tasks) > 0 and running(self._tasks[0])
        broadcast = len(self._tasks) > 1 and running(self._tasks[1])
        consumers = (
            self.consumer is not None
            and not self.consumer._closed
            and self.broadcast_consumer is not None
            and not self.broadcast_consumer._closed
        )
        subscriptions = self._ready.is_set() and self._broadcast_ready.is_set()
        failed = self._error is not None
        return {
            "is_ready": bool(
                not self._closed
                and not failed
                and producer
                and wakeup
                and broadcast
                and consumers
                and subscriptions
            ),
            "producer_is_running": bool(producer),
            "wakeup_receiver_is_running": wakeup,
            "broadcast_receiver_is_running": broadcast,
            "consumers_are_open": bool(consumers),
            "has_background_error": failed,
            "subscriptions_are_ready": subscriptions,
        }

    def _task_finished(self, task):
        if task.cancelled() or self._closed:
            return
        error = task.exception()
        if error is not None:
            self._error = error
            logger.error(
                "Kafka background transport stopped (%s)", type(error).__name__
            )
            self._ready.set()
            self._broadcast_ready.set()
            self._signal.set()
            for subscriptions in self._subscriptions.values():
                for subscription in subscriptions:
                    subscription.closed.set()

    def _check_error(self):
        if self._error is not None:
            raise RuntimeError(
                "Kafka background transport failed; restart bus after remediation"
            ) from self._error

    async def close(self):
        if self._closed:
            return
        self._closed = True
        self._signal.set()
        for subscriptions in self._subscriptions.values():
            for subscription in subscriptions:
                subscription.closed.set()
        for task in self._tasks:
            task.cancel()
        await asyncio.gather(*self._tasks, return_exceptions=True)
        # Unread records are uncommitted; stop causes reassignment/replay.
        for client in (self.consumer, self.broadcast_consumer, self.producer):
            if client is not None:
                await client.stop()
        self._pending.clear()

    async def push(self, payload, *, ttl_secs=None):
        self._check_error()
        if not isinstance(payload, dict):
            raise TypeError("message bus payload must be a dict")
        # Validate before touching metadata. Send failures are visible to caller.
        json.dumps(payload, allow_nan=False)
        generation, _ = await self.pg.trigger_state(
            self.topic, push=True, ttl_secs=ttl_secs
        )
        value = json.dumps(
            {"generation": generation, "payload": payload}, allow_nan=False
        ).encode()
        session = payload.get("session_id")
        metadata = await self.producer.send_and_wait(
            self.topic,
            value=value,
            key=str(session).encode() if session is not None else None,
        )
        return f"{metadata.topic}:{metadata.partition}:{metadata.offset}"

    async def _receive_triggers(self):
        while not self._closed:
            try:
                free = self.capacity - len(self._pending)
                assigned = self.consumer.assignment()
                if assigned:
                    if free <= 0:
                        self.consumer.pause(*assigned)
                    else:
                        self.consumer.resume(*assigned)
                epoch = self._epoch
                # Poll even when paused/full, keeping group membership alive.
                batches = await self.consumer.getmany(
                    timeout_ms=200, max_records=max(1, free)
                )
                async with self._pending_lock:
                    if epoch != self._epoch:
                        # A fetch can finish across a rebalance. Rewind any
                        # currently-owned returned partition before retrying;
                        # merely discarding this result would skip its offsets.
                        for tp, records in batches.items():
                            if tp in self._assigned and records:
                                self.consumer.seek(tp, records[0].offset)
                        continue
                    for tp, records in batches.items():
                        if tp not in self._assigned:
                            continue
                        for record in records:
                            if len(self._pending) >= self.capacity:
                                self.consumer.seek(tp, record.offset)
                                break
                            self._pending.append(record)
                    if self._pending:
                        self._signal.set()
                if free <= 0:
                    await asyncio.sleep(0.05)
            except KafkaError as exc:
                if not exc.retriable:
                    raise
                logger.warning(
                    "Kafka trigger receive failed; retrying (%s)", type(exc).__name__
                )
                await asyncio.sleep(0.5)

    async def drain(self, max_count=100):
        self._check_error()
        self.pg._count(max_count)
        async with self._pending_lock:
            if not self._pending:
                return []
            generation, expired = await self.pg.trigger_state(self.topic)
            batch = list(self._pending)[:max_count]
            offsets = {}
            entries = []
            for record in batch:
                tp = TopicPartition(record.topic, record.partition)
                body = json.loads(record.value)
                if not isinstance(body, dict) or not isinstance(
                    body.get("payload"), dict
                ):
                    raise ValueError("malformed Kafka trigger record; not acknowledged")
                offsets[tp] = record.offset + 1
                if not expired and body.get("generation") == generation:
                    entries.append(
                        (
                            f"{record.topic}:{record.partition}:{record.offset}",
                            body["payload"],
                        )
                    )
            try:
                # Explicit per-partition offsets, never consumer.position(),
                # which could acknowledge prefetched/unread records.
                await self.consumer.commit(offsets)
            except BaseException:
                self._signal.set()
                raise
            for _ in batch:
                self._pending.popleft()
            if self._pending:
                self._signal.set()
            return entries

    async def publish(self, key, payload):
        self._check_error()
        if not isinstance(payload, dict):
            raise TypeError("message bus payload must be a dict")
        if not isinstance(key, str):
            raise TypeError("message bus broadcast key must be a string")
        key_bytes = key.encode()
        if len(key_bytes) > _MAX_BROADCAST_KEY_BYTES:
            raise ValueError("message bus broadcast key exceeds 65536 bytes")
        value = json.dumps({"key": key, "payload": payload}, allow_nan=False).encode()
        if len(value) > _MAX_BROADCAST_BYTES:
            raise ValueError("message bus broadcast exceeds 33554432 bytes")
        # A later small event must not overtake an earlier PG-backed event
        # while its body is being committed. Kafka still orders across nodes
        # by the actual record offsets, as it does for inline broadcasts.
        # Weak locks live only while publishers hold/wait for them. Session
        # keys do not accumulate, and slow image storage cannot block cancel.
        publish_lock = self._publish_locks.setdefault(key, asyncio.Lock())
        async with publish_lock:
            if len(value) + len(key_bytes) + 128 > _INLINE_BROADCAST_BYTES:
                async with asyncio.timeout(_BROADCAST_STORAGE_TIMEOUT_SECS):
                    reference = await self.pg.broadcast_store(
                        self.broadcast_topic,
                        key,
                        payload,
                        ttl_secs=_BROADCAST_PAYLOAD_TTL_SECS,
                    )
                value = json.dumps(
                    {"key": key, "payload_ref": reference, "version": 1},
                    allow_nan=False,
                ).encode()
            # Do not delete stored bodies on send failure: a lost acknowledgement
            # can still mean the reference was accepted. TTL also reclaims orphans.
            await self.producer.send_and_wait(
                self.broadcast_topic, value=value, key=key_bytes
            )

    async def _receive_broadcasts(self):
        while not self._closed:
            try:
                batches = await self.broadcast_consumer.getmany(
                    timeout_ms=200, max_records=self.capacity
                )
                for tp, records in batches.items():
                    for record in records:
                        try:
                            body = json.loads(record.value)
                            key = body["key"]
                            if not isinstance(key, str) or (
                                "payload_ref" not in body
                                and not isinstance(body.get("payload"), dict)
                            ):
                                raise ValueError("invalid broadcast envelope")
                        except (ValueError, TypeError, KeyError):
                            logger.error(
                                "Malformed transient Kafka broadcast at partition=%s offset=%s",
                                record.partition,
                                record.offset,
                            )
                            continue
                        async with self._broadcast_lock:
                            subscriptions = tuple(
                                subscription
                                for subscription in self._subscriptions.get(key, ())
                                if record.offset
                                >= subscription.offsets.get(tp, float("inf"))
                            )
                        if not subscriptions:
                            continue
                        if "payload_ref" in body:
                            if body.get("version") != 1 or not isinstance(
                                body["payload_ref"], str
                            ):
                                raise ValueError(
                                    "invalid Kafka broadcast payload reference"
                                )
                            # Resolve in Kafka order, once per node, then share
                            # the same unchanged dict with all local subscribers.
                            # A missing/expired body is fatal, never silently
                            # skipped; native session logs remain replayable.
                            async with asyncio.timeout(_BROADCAST_STORAGE_TIMEOUT_SECS):
                                payload = await self.pg.broadcast_read(
                                    self.broadcast_topic, key, body["payload_ref"]
                                )
                            if not isinstance(payload, dict):
                                raise ValueError(
                                    "invalid stored Kafka broadcast payload"
                                )
                        else:
                            payload = body["payload"]
                        for subscription in subscriptions:
                            await subscription.deliver(payload)
            except KafkaError as exc:
                if not exc.retriable:
                    raise
                logger.warning(
                    "Kafka broadcast receive failed; retrying (%s)", type(exc).__name__
                )
                await asyncio.sleep(0.5)

    async def subscribe(self, key, *, on_ready=None):
        self._check_error()
        if key == MessageBusKeys.wakeup_signal():
            await self._ready.wait()
            if on_ready is not None:
                on_ready()
            if self._pending:
                self._signal.set()
            while not self._closed:
                await self._signal.wait()
                self._check_error()
                self._signal.clear()
                if not self._closed:
                    yield {}
            return
        await self._broadcast_ready.wait()
        async with self._broadcast_lock:
            # Barrier at the broker's next offsets: no pre-subscription history,
            # including records fetched by the pump but not yet dispatched.
            assignment = self.broadcast_consumer.assignment()
            offsets = await self.broadcast_consumer.end_offsets(assignment)
            subscription = _Subscription(asyncio.Queue(maxsize=self.capacity), offsets)
            self._subscriptions.setdefault(key, set()).add(subscription)
        try:
            if on_ready is not None:
                on_ready()
            while not self._closed:
                get = asyncio.create_task(subscription.queue.get())
                closed = asyncio.create_task(subscription.closed.wait())
                try:
                    done, _ = await asyncio.wait(
                        (get, closed), return_when=asyncio.FIRST_COMPLETED
                    )
                    if closed in done:
                        self._check_error()
                        return
                    yield get.result()
                finally:
                    for task in (get, closed):
                        task.cancel()
                    await asyncio.gather(get, closed, return_exceptions=True)
        finally:
            subscription.closed.set()
            async with self._broadcast_lock:
                subscriptions = self._subscriptions.get(key)
                if subscriptions is not None:
                    subscriptions.discard(subscription)
                    if not subscriptions:
                        self._subscriptions.pop(key, None)
