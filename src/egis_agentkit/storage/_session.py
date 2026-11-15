"""按所有者和项目查询原生会话，复用原生删除规则。"""

from agentscope.app.storage._sql._mappers import _from_record
from agentscope.app.storage._sql._tables import MessageRow, SessionRow
from agentscope.message import Msg
from sqlalchemy import column, insert, literal, select, tuple_, update
from sqlalchemy.ext.asyncio import AsyncSession

from egis_agentkit.client._postgres import transaction_connection
from egis_agentkit.storage.model._tables import reply_artifacts

# 由 AgentKit 的 PostgreSQL schema 从原生会话状态生成。
_PROJECT_ID = column("project_id")
_SESSIONS = SessionRow.__table__


class SessionStorage:
    """提供原生存储接口之外的项目查询和事务内清理。"""

    def __init__(self, engine, native):
        self.engine = engine
        self.native = native

    async def _get_message_cursor(self, conn, session_id, message_id):
        return (
            await conn.execute(
                select(MessageRow.created_at, MessageRow.msg_id).where(
                    MessageRow.session_id == session_id,
                    MessageRow.msg_id == message_id,
                )
            )
        ).one_or_none()

    async def list_messages_through(self, session_id, message_id, *, limit):
        """按数据库消息顺序取分支边界，预算外多取一条供 Service 拒绝。"""
        async with self.engine.connect() as conn:
            cursor = await self._get_message_cursor(conn, session_id, message_id)
            if cursor is None:
                return []
            rows = await conn.scalars(
                select(MessageRow.payload)
                .where(
                    MessageRow.session_id == session_id,
                    tuple_(MessageRow.created_at, MessageRow.msg_id)
                    <= tuple_(cursor.created_at, cursor.msg_id),
                )
                .order_by(MessageRow.created_at, MessageRow.msg_id)
                .limit(limit + 1)
            )
            return [Msg.model_validate(payload) for payload in rows]

    async def get_question_before_reply(self, session_id, reply_id):
        async with self.engine.connect() as conn:
            cursor = await self._get_message_cursor(conn, session_id, reply_id)
            if cursor is None:
                return None
            payload = await conn.scalar(
                select(MessageRow.payload)
                .where(
                    MessageRow.session_id == session_id,
                    MessageRow.payload["role"].as_string() == "user",
                    tuple_(MessageRow.created_at, MessageRow.msg_id)
                    < tuple_(cursor.created_at, cursor.msg_id),
                )
                .order_by(MessageRow.created_at.desc(), MessageRow.msg_id.desc())
                .limit(1)
            )
            return Msg.model_validate(payload) if payload else None

    async def save_branch_history(
        self, user_id, record, source_session_id, message_ids
    ):
        """原子发布分支上下文、原生消息和产物引用；保留原消息排序及 ID。"""
        ids = message_ids
        row = _from_record(SessionRow, record)
        async with self.engine.begin() as conn:
            await conn.execute(
                update(SessionRow)
                .where(
                    SessionRow.id == record.id,
                    SessionRow.user_id == user_id,
                    SessionRow.agent_id == record.agent_id,
                )
                .values(payload=row.payload)
            )
            messages = MessageRow.__table__
            await conn.execute(
                insert(messages).from_select(
                    ["session_id", "msg_id", "created_at", "payload"],
                    select(
                        literal(record.id),
                        messages.c.msg_id,
                        messages.c.created_at,
                        messages.c.payload,
                    ).where(
                        messages.c.session_id == source_session_id,
                        messages.c.msg_id.in_(ids),
                    ),
                )
            )
            await conn.execute(
                insert(reply_artifacts).from_select(
                    ["project_id", "session_id", "reply_id", "path", "digest"],
                    select(
                        reply_artifacts.c.project_id,
                        literal(record.id),
                        reply_artifacts.c.reply_id,
                        reply_artifacts.c.path,
                        reply_artifacts.c.digest,
                    ).where(
                        reply_artifacts.c.session_id == source_session_id,
                        reply_artifacts.c.reply_id.in_(ids),
                    ),
                )
            )

    async def locate(self, session_id):
        """用消息总线锁的会话 ID 找到原生所有者和 Agent。"""
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    select(_SESSIONS.c.user_id, _SESSIONS.c.agent_id).where(
                        _SESSIONS.c.id == session_id
                    )
                )
            ).one_or_none()
        return (row.user_id, row.agent_id) if row is not None else None

    async def in_project(self, user_id, project_id):
        """按项目及所有者列出主会话和成员会话。"""
        async with self.engine.connect() as conn:
            return (
                await conn.execute(
                    select(_SESSIONS.c.id, _SESSIONS.c.agent_id)
                    .where(_PROJECT_ID == project_id, _SESSIONS.c.user_id == user_id)
                    .order_by(_SESSIONS.c.id)
                )
            ).all()

    async def list_main_ids(self, user_id, *, limit):
        """按更新时间列出用户可打开的项目主会话 ID。"""
        async with self.engine.connect() as conn:
            return (
                await conn.scalars(
                    select(_SESSIONS.c.id)
                    .where(
                        _SESSIONS.c.user_id == user_id,
                        _SESSIONS.c.source == "user",
                        _PROJECT_ID.is_not(None),
                    )
                    .order_by(_SESSIONS.c.updated_at.desc())
                    .limit(limit)
                )
            ).all()

    async def delete_sessions(self, user_id, sessions, owned_projections):
        """借用清理事务，沿用原生消息和 Team 级联删除规则。"""
        conn = transaction_connection(self.engine, None)
        async with AsyncSession(
            bind=conn, join_transaction_mode="rollback_only"
        ) as session:
            for sid, agent_id in sessions:
                await self.native._delete_session_impl(session, user_id, agent_id, sid)
            for agent_id in owned_projections:
                await self.native._delete_agent_impl(session, user_id, agent_id)
