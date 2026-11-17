"""管理运行中用户引导，复用原生 Inbox 与唤醒协议。

核心能力
- submit 检查会话权限，按提交顺序保存和投递多条引导。
- received 接收原生 HintBlockEvent 确认，不改变当前回复、工具或审批状态。
"""

from agentscope.app._bus_ops import (
    enqueue_run_trigger,
    publish_session_event,
)
from agentscope.app.message_bus import MessageBusKeys
from agentscope.event import CustomEvent

from egis_agentkit.client.nacos._settings import SessionSteeringConfig
from egis_agentkit.core.session._steering import (
    DELIVERY_METADATA,
    STEERING_EVENT,
    prepare_steering,
    steering_hint,
)


class SessionSteeringService:
    """按会话投递用户引导，不申请执行锁或启动并行 Agent。"""

    def __init__(self, sessions, storage, config=None):
        self.sessions, self.storage, self.config = sessions, storage, config

    def settings(self):
        return (
            self.config.snapshot.session.steering
            if self.config
            else SessionSteeringConfig()
        )

    async def submit(self, user, session_id, agent_id, inputs, bus):
        """先通过当前资源授权，再在项目与原生 Inbox 的锁边界内接收消息。"""
        # 1. 校验真实会话归属及资源使用权限，构建规范的用户消息。
        session = await self.sessions.get_session_record(
            user, session_id, agent_id=agent_id
        )
        messages = prepare_steering(user, inputs, self.settings())
        persistent = getattr(bus, "deployment_mode", None) == "cluster"
        project_id = session.state.middle_context["egis_agentkit"]["project_id"]
        # 2. 项目删除与投递互斥；原生 Inbox 锁串行化接收和结束交接。
        async with self.sessions.projects.guard(user, project_id):
            async with bus.acquire_lock(
                MessageBusKeys.inbox_lock(session_id),
                ttl_secs=MessageBusKeys.INBOX_LOCK_TTL_SECS,
            ):
                saved, fresh = await self.storage.session_steering.accept(
                    session_id, messages, persistent_inbox=persistent
                )
                if not persistent:
                    for message in fresh:
                        await bus.queue_push(
                            MessageBusKeys.inbox(session_id),
                            steering_hint(message).model_dump(mode="json"),
                        )
                consumer = await bus.registry_get(
                    MessageBusKeys.inbox_consumer(session_id),
                    MessageBusKeys.INBOX_CONSUMER_FIELD,
                )
                # 事务已提交；在同一 Inbox 锁内合并并发重试的唤醒。
                pending = any(
                    message.metadata[DELIVERY_METADATA]["state"] == "queued"
                    for message in saved
                )
                needs_wakeup = consumer is None and pending
                if (
                    needs_wakeup
                    and not await bus.is_locked(MessageBusKeys.session_lock(session_id))
                    and not await self.storage.session_steering.wakeup_recent(
                        session_id, self.settings().wakeup_retry_seconds
                    )
                ):
                    await enqueue_run_trigger(
                        bus,
                        user_id=user.user_id,
                        session_id=session_id,
                        agent_id=agent_id,
                    )
                    await self.storage.session_steering.record_wakeup(
                        session_id, [message.id for message in saved]
                    )
            # 3. 广播已接受的用户消息；HintBlockEvent 另行确认模型已接收。
            await publish_session_event(
                bus,
                session_id,
                CustomEvent(
                    name=STEERING_EVENT,
                    value={"messages": [m.model_dump(mode="json") for m in saved]},
                ).model_dump(mode="json"),
            )
        return {
            "status": "queued",
            "session_id": session_id,
            "message_ids": [m.id for m in saved],
        }

    async def received(self, session_id, message_id):
        """原生消费者已经把这条引导加入上下文，记录状态供刷新后恢复。"""
        await self.storage.session_steering.received(session_id, message_id)
