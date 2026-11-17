"""保存用户引导和 Inbox 内容，保证集群重试不会重复投递。

核心能力
- accept 在同一事务中插入用户消息和 PostgreSQL Inbox，批量冲突整体回滚。
- received 仅更新对应用户消息的接收状态，保留原文、附件及原生会话状态。
- wakeup_recent / record_wakeup 合并未开始的触发，超时后允许重试恢复。
"""

from datetime import datetime, timezone

from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.storage._sql._tables import MessageRow
from agentscope.message import Msg
from sqlalchemy import func, select, update
from sqlalchemy.dialects.postgresql import insert

from egis_agentkit.client._postgres import PostgresTransaction
from egis_agentkit.core.session._steering import (
    DELIVERY_METADATA,
    steering_hint,
    steering_identity,
)
from egis_agentkit.errors import Conflict
from egis_agentkit.storage._message_bus import PostgresPrimitives


class SessionSteeringStorage:
    """复用原生 messages 和已有消息总线表，不额外建立引导表。"""

    def __init__(self, engine):
        self.engine = engine

    async def accept(self, session_id, messages, *, persistent_inbox):
        """新 ID 只写一次；相同 ID 的重试必须指向同一份用户内容。"""
        saved, fresh = [], []
        async with self.engine.begin() as conn:
            inbox = PostgresPrimitives(PostgresTransaction(conn))
            for message in messages:
                payload = message.model_dump(mode="json")
                inserted = await conn.scalar(
                    insert(MessageRow)
                    .values(
                        session_id=session_id,
                        msg_id=message.id,
                        created_at=datetime.now(timezone.utc).replace(tzinfo=None),
                        payload=payload,
                    )
                    .on_conflict_do_nothing()
                    .returning(MessageRow.msg_id)
                )
                if inserted is None:
                    previous = await conn.scalar(
                        select(MessageRow.payload).where(
                            MessageRow.session_id == session_id,
                            MessageRow.msg_id == message.id,
                        )
                    )
                    original = Msg.model_validate(previous)
                    if (
                        original.role != "user"
                        or DELIVERY_METADATA not in original.metadata
                        or steering_identity(original) != steering_identity(message)
                    ):
                        raise Conflict("消息 ID 已用于其他内容，请重新创建引导")
                    saved.append(original)
                    continue
                if persistent_inbox:
                    await inbox.queue_push(
                        MessageBusKeys.inbox(session_id),
                        steering_hint(message).model_dump(mode="json"),
                    )
                fresh.append(message)
                saved.append(message)
        return saved, fresh

    async def received(self, session_id, message_id):
        """只修改引导自身元数据，不覆盖正在运行的 SessionRecord。"""
        async with self.engine.begin() as conn:
            payload = await conn.scalar(
                select(MessageRow.payload)
                .where(
                    MessageRow.session_id == session_id,
                    MessageRow.msg_id == message_id,
                )
                .with_for_update()
            )
            if payload is None or DELIVERY_METADATA not in payload.get("metadata", {}):
                return
            payload = {
                **payload,
                "metadata": {
                    **payload["metadata"],
                    DELIVERY_METADATA: {"kind": "steer", "state": "received"},
                },
            }
            await conn.execute(
                update(MessageRow)
                .where(
                    MessageRow.session_id == session_id,
                    MessageRow.msg_id == message_id,
                )
                .values(payload=payload)
            )

    async def wakeup_recent(self, session_id, retry_seconds):
        """合并同一会话尚未被接收的唤醒；超时后允许修复失败或丢失的触发。"""
        delivery = MessageRow.payload["metadata"][DELIVERY_METADATA]
        async with self.engine.connect() as conn:
            recent = await conn.scalar(
                select(MessageRow.msg_id)
                .where(
                    MessageRow.session_id == session_id,
                    delivery["state"].as_string() == "queued",
                    delivery["wake_sent_at"].as_float()
                    > func.extract("epoch", func.clock_timestamp()) - retry_seconds,
                )
                .limit(1)
            )
        return recent is not None

    async def record_wakeup(self, session_id, message_ids):
        """Kafka 已确认后记录触发时间；不覆盖并发到达的模型接收确认。"""
        async with self.engine.begin() as conn:
            stamp = float(
                await conn.scalar(select(func.extract("epoch", func.clock_timestamp())))
            )
            rows = await conn.execute(
                select(MessageRow.msg_id, MessageRow.payload)
                .where(
                    MessageRow.session_id == session_id,
                    MessageRow.msg_id.in_(message_ids),
                )
                .with_for_update()
            )
            for message_id, payload in rows:
                delivery = payload.get("metadata", {}).get(DELIVERY_METADATA, {})
                if delivery.get("state") != "queued":
                    continue
                payload = {
                    **payload,
                    "metadata": {
                        **payload["metadata"],
                        DELIVERY_METADATA: {**delivery, "wake_sent_at": stamp},
                    },
                }
                await conn.execute(
                    update(MessageRow)
                    .where(
                        MessageRow.session_id == session_id,
                        MessageRow.msg_id == message_id,
                    )
                    .values(payload=payload)
                )
