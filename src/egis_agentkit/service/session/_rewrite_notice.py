"""从已提交的 NAS 历史恢复编辑通知，失败可由下一次订阅或后台重试。"""

import json
from contextlib import nullcontext

from agentscope.app._bus_ops import publish_session_event
from agentscope.event import CustomEvent


async def publish_rewrite_notice(bus, storage, session_id, *, lock_held=False):
    namespace = bus.rewrite_notice_namespace()
    if await bus.registry_get(namespace, session_id) is None:
        return
    guard = (
        nullcontext()
        if lock_held
        else bus.acquire_lock(bus.rewrite_notice_lock(session_id), ttl_secs=30)
    )
    async with guard:
        saved = await bus.registry_get(namespace, session_id)
        if saved is None:
            return
        notice = json.loads(saved)
        message = await storage.get_message(
            notice["user_id"], session_id, notice["message_id"]
        )
        if message is None:
            if await storage.locate_session(session_id) is not None:
                raise RuntimeError("已提交的编辑消息无法从 NAS 历史恢复")
        else:
            await publish_session_event(
                bus,
                session_id,
                CustomEvent(
                    id=notice["event_id"],
                    name="chat_history_rewritten",
                    value={
                        "message_id": message.id,
                        "input": message.model_dump(mode="json"),
                        "retained_message_ids": notice["retained_message_ids"],
                    },
                ).model_dump(mode="json"),
            )
        await bus.registry_del_if(namespace, session_id, expected=saved)
