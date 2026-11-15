"""持久化回复反馈及管理员游标分页，不依赖源会话的生命周期。"""

from sqlalchemy import select, tuple_
from sqlalchemy.dialects.postgresql import insert

from egis_agentkit.errors import InvalidRequest
from egis_agentkit.storage.model import new_id, utcnow
from egis_agentkit.storage.model._tables import feedback


class SessionFeedbackStorage:
    def __init__(self, engine):
        self.engine = engine

    async def list_reply_feedback(self, user_id, session_id):
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        select(feedback.c.reply_id, feedback.c.comment).where(
                            feedback.c.user_id == user_id,
                            feedback.c.session_id == session_id,
                        )
                    )
                )
                .mappings()
                .all()
            )
            return [dict(row) for row in rows]

    async def save_reply_feedback(self, values):
        async with self.engine.begin() as conn:
            statement = insert(feedback).values(id=new_id(), **values)
            statement = statement.on_conflict_do_update(
                index_elements=[
                    feedback.c.user_id,
                    feedback.c.session_id,
                    feedback.c.reply_id,
                ],
                set_={"comment": statement.excluded.comment, "updated_at": utcnow()},
            ).returning(feedback)
            return dict((await conn.execute(statement)).mappings().one())

    async def page_reply_feedback(self, *, limit, cursor):
        query = select(feedback)
        async with self.engine.connect() as conn:
            if cursor:
                anchor = (
                    await conn.execute(
                        select(feedback.c.created_at, feedback.c.id).where(
                            feedback.c.id == cursor
                        )
                    )
                ).one_or_none()
                if anchor is None:
                    raise InvalidRequest("反馈分页游标已失效，请刷新")
                query = query.where(
                    tuple_(feedback.c.created_at, feedback.c.id)
                    < tuple_(anchor.created_at, anchor.id)
                )
            rows = (
                (
                    await conn.execute(
                        query.order_by(
                            feedback.c.created_at.desc(), feedback.c.id.desc()
                        ).limit(limit + 1)
                    )
                )
                .mappings()
                .all()
            )
        return {
            "items": [dict(row) for row in rows[:limit]],
            "next_cursor": rows[limit - 1]["id"] if len(rows) > limit else None,
        }
