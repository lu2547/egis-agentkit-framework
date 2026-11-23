"""覆盖会话锁、订阅及终态观测，接入执行准备并保留原生事件传递语义。

核心能力
- acquire_lock 在原生锁内调用会话准备 Service，失败交原生运行错误处理。
- subscribe 传递订阅就绪通知，不缓存或重排 SSE 事件。
- publish 观察原生失败终态，保持原消息及传输方式不变。
"""

import asyncio
import logging
from contextlib import (
    AsyncExitStack,
    aclosing,
    asynccontextmanager,
    nullcontext,
    suppress,
)

from agentscope.app.message_bus import MessageBusKeys
from agentscope.event import EventType, ReplyFinishedReason

from egis_agentkit.client._logging import log_session
from egis_agentkit.client._tracing import mark_runtime_error, trace_phase
from egis_agentkit.core.message_bus import EgisMessageBus
from egis_agentkit.core.session._context import (
    session_execution_context,
    session_setup_error,
    stream_subscription_ready,
)
from egis_agentkit.errors import AgentKitError
from egis_agentkit.service.session._authorization import ChatAuthorizationStore
from egis_agentkit.service.session._preparation import SessionPreparationService
from egis_agentkit.service.session._rewrite_notice import publish_rewrite_notice
from egis_agentkit.service.session._steering_projection import SteeringPresentation
from egis_agentkit.service.team._collaboration import collaborators
from egis_agentkit.service.team._progress import OBSERVED_EVENTS, CollaboratorProgress

logger = logging.getLogger(__name__)


class SessionMessageBus(EgisMessageBus):
    """在原生消息总线上接入执行上下文和会话事件。"""

    coordinator: SessionPreparationService | None = None

    def __init__(self, *, tracing=None, presentation_storage=None, **kwargs):
        """复用原生总线构造；Tracing 只观察锁及运行，不改变消息或租约协议。"""
        super().__init__(**kwargs)
        self.steering_presentation = (
            SteeringPresentation(self, presentation_storage)
            if presentation_storage is not None
            else None
        )
        self.tracing = tracing
        self.collaborator_progress = CollaboratorProgress(self)
        self._rewrite_notifier = None

    async def __aenter__(self):
        await super().__aenter__()
        if (
            self.delegate is None
            and self.steering_presentation is not None
            and self._rewrite_notifier is None
        ):
            self._rewrite_notifier = asyncio.create_task(self._retry_rewrite_notices())
        return self

    async def aclose(self):
        if self._rewrite_notifier is not None:
            self._rewrite_notifier.cancel()
            with suppress(asyncio.CancelledError):
                await self._rewrite_notifier
            self._rewrite_notifier = None
        await super().aclose()

    async def _retry_rewrite_notices(self):
        while True:
            try:
                notices = await self.registry_getall(self.rewrite_notice_namespace())
                for sid in notices:
                    try:
                        await publish_rewrite_notice(
                            self, self.steering_presentation.storage, sid
                        )
                    except Exception:
                        logger.exception(
                            "message.rewrite_notification_retry_failed",
                            extra={"context": {"session_id": sid}},
                        )
            except Exception:
                logger.exception("message.rewrite_notification_scan_failed")
            await asyncio.sleep(1)

    async def _publish_rewrite_before_run(self, session_id):
        """先投递历史边界，再开始新回复，避免迟到通知抹掉新回复。"""
        while (
            await self.registry_get(self.rewrite_notice_namespace(), session_id)
            is not None
        ):
            try:
                await publish_rewrite_notice(
                    self, self.steering_presentation.storage, session_id
                )
            except Exception:
                logger.exception(
                    "message.rewrite_notification_waiting",
                    extra={"context": {"session_id": session_id}},
                )
                await asyncio.sleep(1)

    async def log_read(self, key, since=None, max_count=100):
        prefix = MessageBusKeys.session_events("")
        if (
            self.delegate is None
            and self.steering_presentation is not None
            and key.startswith(prefix)
        ):
            try:
                await publish_rewrite_notice(
                    self, self.steering_presentation.storage, key[len(prefix) :]
                )
            except Exception:
                logger.exception(
                    "message.rewrite_notification_read_failed",
                    extra={"context": {"session_id": key[len(prefix) :]}},
                )
        return await super().log_read(key, since=since, max_count=max_count)

    async def purge_sessions(self, session_ids, *, extra_registry_namespaces=()):
        session_ids = tuple(session_ids)
        return await super().purge_sessions(
            session_ids,
            extra_registry_namespaces=(
                *extra_registry_namespaces,
                *(ChatAuthorizationStore.namespace(sid) for sid in session_ids),
            ),
        )

    async def queue_remove_if(self, key, matches):
        staged = (
            await self.steering_presentation.stage(key, matches)
            if self.steering_presentation
            else []
        )
        removed = await super().queue_remove_if(key, matches)
        if not removed and staged:
            await self.steering_presentation.discard(
                key[len(MessageBusKeys.chat_inputs("")) :], staged
            )
        return removed

    async def log_append(self, key, payload, *, ttl_secs=None, max_len=None):
        prefix = MessageBusKeys.session_events("")
        if (
            self.steering_presentation is not None
            and key.startswith(prefix)
            and payload.get("type") == EventType.HINT_BLOCK
            and payload.get("source") == "chat_input_steering"
        ):
            await self.steering_presentation.project(key[len(prefix) :], payload)
        return await super().log_append(
            key, payload, ttl_secs=ttl_secs, max_len=max_len
        )

    async def queue_push(self, key, payload, *, ttl_secs=None):
        result = await super().queue_push(key, payload, ttl_secs=ttl_secs)
        scope = session_execution_context.get()
        if scope is not None and key.startswith(MessageBusKeys.inbox("")):
            for member in collaborators(scope.binding):
                worker = member.get("worker_session_id")
                if worker and key == MessageBusKeys.inbox(worker):
                    await self.collaborator_progress.dispatched(
                        scope.session_id, member, repeat=True
                    )
                    break
        return result

    async def publish(self, key, payload):
        """观察原生失败终态和协作进度。"""
        if (
            key.startswith(MessageBusKeys.session_events(""))
            and isinstance(payload, dict)
            and payload.get("type") == EventType.REPLY_END
            and payload.get("finished_reason") == ReplyFinishedReason.ERROR
        ):
            mark_runtime_error("ReplyError", session_id=payload.get("session_id"))
        result = await super().publish(key, payload)
        prefix = MessageBusKeys.session_events("")
        if (
            key.startswith(prefix)
            and isinstance(payload, dict)
            and payload.get("type") in OBSERVED_EVENTS
        ):
            await self._observe_collaborator(key[len(prefix) :], payload)
        return result

    async def _observe_collaborator(self, session_id, event):
        try:
            scope = session_execution_context.get()
            if scope is not None and scope.session_id == session_id:
                parent = scope.binding.get("collaboration_parent_session_id")
                if parent:
                    await self.collaborator_progress.observe(
                        parent,
                        session_id,
                        scope.agent_id,
                        scope.definition["agent_display_name"],
                        event,
                    )
            elif (
                event["type"] == EventType.REPLY_END
                and session_setup_error.get() is not None
                and isinstance(self.coordinator, SessionPreparationService)
            ):
                target = await self.coordinator.collaboration_progress_target(
                    session_id
                )
                if target is not None:
                    parent, agent_id, name = target
                    await self.collaborator_progress.observe(
                        parent, session_id, agent_id, name, event
                    )
        except Exception:
            logger.exception(
                "Failed to project collaborator progress",
                extra={"context": {"worker_session_id": session_id}},
            )

    async def subscribe(self, key, *, on_ready=None):
        ready = stream_subscription_ready.get()

        def subscribed():
            if on_ready is not None:
                on_ready()
            if ready is not None and key.startswith(MessageBusKeys.session_events("")):
                ready.set()

        async with aclosing(
            super().subscribe(key, on_ready=subscribed)
        ) as subscription:
            async for payload in subscription:
                yield payload

    @asynccontextmanager
    async def acquire_lock(self, key, ttl_secs=600):
        prefix = MessageBusKeys.session_lock("")
        scoped = key.startswith(prefix) and self.coordinator is not None
        session_id = key[len(prefix) :] if scoped else None
        observation = (
            self.tracing.span(
                "session.operation",
                root=True,
                attributes={"session.id": session_id},
            )
            if scoped and self.tracing is not None
            else nullcontext()
        )
        with observation, log_session(session_id):
            # 1. 独立计时等待锁；取得锁后才加载会话，保留原生并发边界。
            async with AsyncExitStack() as lifetime:
                with trace_phase("session.lock.wait"):
                    await lifetime.enter_async_context(
                        super().acquire_lock(key, ttl_secs=ttl_secs)
                    )
                if (
                    key.startswith(prefix)
                    and self.delegate is None
                    and self.steering_presentation is not None
                ):
                    await self._publish_rewrite_before_run(key[len(prefix) :])
                async with self._session_scope(key, prefix, scoped):
                    yield

    @asynccontextmanager
    async def _session_scope(self, key, prefix, scoped):
        """进入已有执行准备能力；失败仍交原生错误处理，不改变事件顺序。"""
        if scoped:
            async with AsyncExitStack() as stack:
                try:
                    await stack.enter_async_context(
                        self.coordinator.bind(key[len(prefix) :])
                    )
                except AgentKitError as exc:
                    mark_runtime_error(type(exc).__name__)
                    # The lock is outside native ChatService's setup try.
                    # Defer to the workspace adapter, after native setup
                    # resolves Team identity and before tools/models run.
                    # Native failure handling can then notify the leader.
                    token = session_setup_error.set(exc)
                    try:
                        yield
                    finally:
                        session_setup_error.reset(token)
                else:
                    yield
        else:
            yield
