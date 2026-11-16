"""覆盖会话锁及订阅，接入执行准备并保留原生事件传递语义。

核心能力
- acquire_lock 在原生锁内调用会话准备 Service，失败交原生运行错误处理。
- subscribe 传递订阅就绪通知，不缓存或重排 SSE 事件。
"""

from contextlib import AsyncExitStack, aclosing, asynccontextmanager

from agentscope.app.message_bus import MessageBusKeys

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
        async with super().acquire_lock(key, ttl_secs=ttl_secs):
            if key.startswith(prefix) and self.coordinator is not None:
                async with AsyncExitStack() as stack:
                    try:
                        await stack.enter_async_context(
                            self.coordinator.bind(key[len(prefix) :])
                        )
                    except AgentKitError as exc:
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
