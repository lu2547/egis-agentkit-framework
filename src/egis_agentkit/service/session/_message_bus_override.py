"""覆盖会话锁、订阅及终态观测，接入执行准备并保留原生事件传递语义。

核心能力
- acquire_lock 在原生锁内调用会话准备 Service，失败交原生运行错误处理。
- subscribe 传递订阅就绪通知，不缓存或重排 SSE 事件。
- publish 观察原生失败终态，保持原消息及传输方式不变。
"""

from contextlib import AsyncExitStack, aclosing, asynccontextmanager, nullcontext

from agentscope.app.message_bus import MessageBusKeys
from agentscope.event import EventType, ReplyFinishedReason

from egis_agentkit.client._tracing import mark_runtime_error, trace_phase
from egis_agentkit.core.message_bus import EgisMessageBus
from egis_agentkit.core.session._context import (
    session_setup_error,
    stream_subscription_ready,
)
from egis_agentkit.errors import AgentKitError
from egis_agentkit.service.session._preparation import SessionPreparationService


class SessionMessageBus(EgisMessageBus):
    """Delegate native bus primitives; add only the session-lock scope."""

    coordinator: SessionPreparationService | None = None

    def __init__(self, *, tracing=None, **kwargs):
        """复用原生总线构造；Tracing 只观察锁及运行，不改变消息或租约协议。"""
        super().__init__(**kwargs)
        self.tracing = tracing

    async def publish(self, key, payload):
        """观察原生失败终态；异常被 ChatService 捕获时，运行根仍保留错误状态。"""
        if (
            key.startswith(MessageBusKeys.session_events(""))
            and isinstance(payload, dict)
            and payload.get("type") == EventType.REPLY_END
            and payload.get("finished_reason") == ReplyFinishedReason.ERROR
        ):
            mark_runtime_error("ReplyError", session_id=payload.get("session_id"))
        return await super().publish(key, payload)

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
        observation = (
            self.tracing.span(
                "session.operation",
                root=True,
                attributes={"session.id": key[len(prefix) :]},
            )
            if scoped and self.tracing is not None
            else nullcontext()
        )
        with observation:
            # 1. 独立计时等待锁；取得锁后才加载会话，保留原生并发边界。
            async with AsyncExitStack() as lifetime:
                with trace_phase("session.lock.wait"):
                    await lifetime.enter_async_context(
                        super().acquire_lock(key, ttl_secs=ttl_secs)
                    )
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
