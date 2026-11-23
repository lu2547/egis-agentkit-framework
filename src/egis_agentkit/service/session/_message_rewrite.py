"""编辑重发用户消息：只截断对话与执行状态，不回退项目文件。"""

import logging
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field

from agentscope.app._bus_ops import (
    delete_chat_input,
    nudge_chat_inputs,
    publish_session_event,
)
from agentscope.app._service import SessionService as NativeSessionService
from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.storage._utils import _ensure_team_members
from agentscope.event import CustomEvent
from agentscope.message import Msg
from agentscope.state import AgentState

from egis_agentkit.client.nacos._settings import SessionBranchConfig
from egis_agentkit.core.message_bus import EgisMessageBus
from egis_agentkit.errors import Conflict, Forbidden, InvalidRequest, NotFound
from egis_agentkit.service._reply import build_branch_context
from egis_agentkit.service.session._steering_projection import (
    steering_presentation_namespace,
)

logger = logging.getLogger(__name__)


@dataclass
class RewriteReceipt:
    queue_item_id: str | None = None
    replacement: Msg | None = None
    retained_message_ids: list[str] = field(default_factory=list)


class MessageRewriteService:
    def __init__(self, sessions):
        self.sessions = sessions

    @staticmethod
    @asynccontextmanager
    async def input_guard(bus, session_id):
        """普通发送和编辑重发串行入队，避免截断期间插入另一条用户消息。"""
        async with bus.acquire_lock(f"egis:chat:submission:{session_id}"):
            yield

    async def _members(self, user_id, session):
        if not session.team_id:
            return []
        team = await self.sessions.storage.get_team(user_id, session.team_id)
        if team is None or team.session_id != session.id:
            raise Forbidden("对话的专家协作关系已失效")
        workers = []
        for member in await _ensure_team_members(self.sessions.storage, user_id, team):
            if member.session_id == session.id:
                continue
            worker = await self.sessions.storage.get_session(
                user_id, member.agent_id, member.session_id
            )
            if worker is None or worker.team_id != session.team_id:
                raise Forbidden("对话的专家协作关系已失效")
            workers.append(worker)
        return workers

    @staticmethod
    async def _clear_memory_runtime(bus, session_ids):
        """单机状态在数据库成功提交后才清理，拒绝/入队失败不改变原现场。"""
        for sid in session_ids:
            await bus.log_trim(MessageBusKeys.session_events(sid))
            await bus.queue_delete(MessageBusKeys.inbox(sid))
            await bus.registry_del(MessageBusKeys.chat_active_reply_registry(), sid)
            for namespace in (
                MessageBusKeys.inbox_consumer(sid),
                MessageBusKeys.session_event_checkpoint(sid),
                MessageBusKeys.projection_namespace(sid),
                steering_presentation_namespace(sid),
            ):
                await bus.registry_drop(namespace)

    @staticmethod
    def _reset_state(record, context):
        record.state = AgentState(
            session_id=record.state.session_id,
            context=context,
            permission_context=record.state.permission_context.model_copy(deep=True),
            middle_context={
                "egis_agentkit": record.state.middle_context["egis_agentkit"]
            },
        )

    @asynccontextmanager
    async def replace(self, user, session_id, agent_id, message_id, bus):
        tasks = self.sessions
        session = await tasks.get_session_record(user, session_id, agent_id=agent_id)
        if session.origin.type != "user":
            raise Forbidden("只能编辑主对话中的用户消息")
        project_id = session.state.middle_context["egis_agentkit"]["project_id"]
        receipt = RewriteReceipt()
        async with tasks.projects.guard(user, project_id), AsyncExitStack() as locks:
            # 先保护组长，再加载成员，邀请/移除成员也使用同一组长锁。
            root_lock = MessageBusKeys.session_lock(session_id)
            if not await bus.try_lock(root_lock):
                raise Conflict("对话正在运行，请先停止生成后再编辑消息")
            locks.push_async_callback(bus.unlock, root_lock)
            session = await tasks.get_session_record(
                user, session_id, agent_id=agent_id
            )
            workers = await self._members(user.user_id, session)
            for record in [session, *sorted(workers, key=lambda worker: worker.id)]:
                for key in (
                    MessageBusKeys.chat_input_dispatch_lock(record.id),
                    *(
                        []
                        if record.id == session_id
                        else [MessageBusKeys.session_lock(record.id)]
                    ),
                ):
                    if not await bus.try_lock(key):
                        raise Conflict(
                            "对话或受邀专家正在运行，请等待本轮完成后再编辑消息"
                        )
                    locks.push_async_callback(bus.unlock, key)
                if (
                    await bus.queue_read(
                        MessageBusKeys.chat_inputs(record.id), max_count=1
                    )
                    or await bus.registry_exists(
                        MessageBusKeys.chat_input_inflight_registry(), record.id
                    )
                    or await bus.registry_getall(MessageBusKeys.bg_tasks(record.id))
                ):
                    raise Conflict(
                        "对话仍有待执行消息或后台任务，请处理完成后再编辑消息"
                    )
                if record.id != session_id:
                    fresh = await tasks.storage.get_session(
                        user.user_id, record.agent_id, record.id
                    )
                    if fresh is None:
                        raise NotFound("专家会话不存在")
                    record.state = fresh.state
                if (
                    NativeSessionService.derive_parked_status(
                        record.state.context
                    ).value
                    != "idle"
                ):
                    raise Conflict(
                        "对话或受邀专家正在等待工具确认，请先处理或停止后再编辑消息"
                    )
            original = await tasks.storage.get_message(
                user.user_id, session_id, message_id
            )
            if original is None:
                raise NotFound("消息不存在或不可访问")
            if original.role != "user":
                raise InvalidRequest("只能编辑用户消息")
            limits = (
                tasks.config.snapshot.session.branch
                if tasks.config
                else SessionBranchConfig()
            )
            messages = await tasks.storage.sessions.list_messages_through(
                session_id, message_id, limit=limits.max_messages
            )
            if len(messages) > limits.max_messages:
                raise InvalidRequest(
                    f"重发历史超过 {limits.max_messages} 条，请选择较早的消息"
                )
            context = build_branch_context(messages[:-1], session.state.context)
            receipt.retained_message_ids = [message.id for message in messages[:-1]]
            self._reset_state(session, context)
            for worker in workers:
                self._reset_state(worker, [])
            try:
                session_ids = [session_id, *[worker.id for worker in workers]]
                shared_runtime = (
                    isinstance(bus, EgisMessageBus) and bus.delegate is None
                )
                async with tasks.storage.sessions.rewrite_before(
                    user.user_id, session, message_id, workers
                ) as conn:
                    yield receipt
                    if not receipt.queue_item_id or receipt.replacement is None:
                        raise RuntimeError("编辑后的消息未进入队列")
                    if shared_runtime:
                        await bus.clear_rewrite_runtime(
                            conn,
                            session_ids,
                            extra_registry_namespaces=[
                                steering_presentation_namespace(sid)
                                for sid in session_ids
                            ],
                        )
            except BaseException:
                if receipt.queue_item_id:
                    await delete_chat_input(
                        bus, user.user_id, session_id, agent_id, receipt.queue_item_id
                    )
                raise
            if not shared_runtime:
                await self._clear_memory_runtime(bus, session_ids)
            # 其他已打开页面使用服务器的保留边界同步，不能继续展示旧问题和后续回答。
            try:
                await publish_session_event(
                    bus,
                    session_id,
                    CustomEvent(
                        name="chat_history_rewritten",
                        value={
                            "message_id": message_id,
                            "input": receipt.replacement.model_dump(mode="json"),
                            "retained_message_ids": receipt.retained_message_ids,
                        },
                    ).model_dump(mode="json"),
                )
            except Exception:
                logger.exception(
                    "message.rewrite_notification_failed",
                    extra={"context": {"session_id": session_id}},
                )
        # 入队时持有运行/派发锁，显式唤醒避免等下一次后台扫描。
        await nudge_chat_inputs(bus, user.user_id, session_id, agent_id)
        logger.info(
            "message.rewritten",
            extra={
                "context": {
                    "user_id": user.user_id,
                    "session_id": session_id,
                    "message_id": message_id,
                    "queue_item_id": receipt.queue_item_id,
                }
            },
        )
