"""按所有者和项目查询原生会话，复用原生删除规则。"""

from agentscope.app.storage._sql._tables import SessionRow
from sqlalchemy import column, select
from sqlalchemy.ext.asyncio import AsyncSession

from egis_agentkit.backend._postgres import transaction_connection

# 由 AgentKit 的 PostgreSQL schema 从原生会话状态生成。
_PROJECT_ID = column("project_id")
_SESSIONS = SessionRow.__table__


class SessionStorage:
    """提供原生存储接口之外的项目查询和事务内清理。"""

    def __init__(self, engine, native):
        self.engine = engine
        self.native = native

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
