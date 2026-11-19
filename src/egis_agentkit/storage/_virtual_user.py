"""保存虚拟用户和凭证摘要。摘要仅用于服务端鉴权。"""

from sqlalchemy import func, select, update

from . import tables as t


class VirtualUserStorage:
    def __init__(self, engine):
        self.engine = engine

    async def get(self, user_id):
        statement = select(t.virtual_users).where(t.virtual_users.c.user_id == user_id)
        async with self.engine.connect() as conn:
            row = (await conn.execute(statement)).mappings().one_or_none()
            return dict(row) if row is not None else None

    async def list(self, *, offset: int = 0, limit: int | None = None):
        table = t.virtual_users
        statement = select(
            table.c.user_id, table.c.username, table.c.is_enabled
        ).order_by(table.c.username, table.c.user_id)
        if limit is not None:
            statement = statement.offset(offset).limit(limit)
        async with self.engine.connect() as conn:
            return [dict(row) for row in (await conn.execute(statement)).mappings()]

    async def create(self, *, user_id, username, digest, actor):
        async with self.engine.begin() as conn:
            await conn.execute(
                t.virtual_users.insert().values(
                    user_id=user_id,
                    username=username,
                    credential_sha256=digest,
                    created_by=actor,
                    updated_by=actor,
                )
            )

    async def update(self, user_id, *, actor, **values):
        table = t.virtual_users
        statement = (
            update(table)
            .where(table.c.user_id == user_id)
            .values(**values, updated_by=actor, updated_at=func.now())
            .returning(table.c.user_id, table.c.username, table.c.is_enabled)
        )
        async with self.engine.begin() as conn:
            row = (await conn.execute(statement)).mappings().one_or_none()
            return dict(row) if row is not None else None
