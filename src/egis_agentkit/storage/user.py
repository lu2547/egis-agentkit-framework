"""Persistence for enterprise identities and groups."""

from collections.abc import Sequence

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.models import Group, User, utcnow

from . import tables as t


class UserStorage:
    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def get_user(
        self, user_id: str, *, conn: AsyncConnection | None = None
    ) -> dict | None:
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.get_user(user_id, conn=connection)
        row = (
            (await conn.execute(select(t.users).where(t.users.c.user_id == user_id)))
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None

    async def upsert_identity(self, identity: User, *, conn: AsyncConnection) -> None:
        # Login never updates roles, membership or enablement.
        statement = insert(t.users).values(
            **identity.model_dump(), created_by="identity", updated_by="identity"
        )
        await conn.execute(
            statement.on_conflict_do_update(
                index_elements=[t.users.c.user_id],
                set_={
                    "username": identity.username,
                    "updated_at": utcnow(),
                    "updated_by": "identity",
                },
            )
        )

    async def set_admin(
        self, user_id: str, *, actor: str, conn: AsyncConnection
    ) -> bool:
        result = await conn.execute(
            update(t.users)
            .where(t.users.c.user_id == user_id)
            .values(role_type="admin", updated_by=actor, updated_at=utcnow())
        )
        return result.rowcount == 1

    async def list_users(self) -> list[dict]:
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(select(t.users).order_by(t.users.c.user_id))
            ).mappings()
            return [dict(row) for row in rows]

    async def enabled_group_ids(
        self, group_ids: Sequence[str], *, conn: AsyncConnection
    ) -> set[str]:
        rows = await conn.execute(
            select(t.groups.c.group_id).where(
                t.groups.c.group_id.in_(group_ids), t.groups.c.is_enabled
            )
        )
        return set(rows.scalars())

    async def lock_enabled_admins(self, *, conn: AsyncConnection) -> list[str]:
        rows = await conn.execute(
            select(t.users.c.user_id)
            .where(t.users.c.role_type == "admin", t.users.c.is_enabled)
            .order_by(t.users.c.user_id)
            .with_for_update()
        )
        return list(rows.scalars())

    async def update_user(
        self, data: User, *, actor: str, conn: AsyncConnection
    ) -> dict | None:
        rows = await conn.execute(
            update(t.users)
            .where(t.users.c.user_id == data.user_id)
            .values(**data.model_dump(), updated_by=actor, updated_at=utcnow())
            .returning(t.users)
        )
        row = rows.mappings().first()
        return dict(row) if row is not None else None

    async def list_groups_with_members(self) -> tuple[list[dict], list[dict]]:
        async with self.engine.connect() as conn:
            groups = (
                await conn.execute(select(t.groups).order_by(t.groups.c.group_name))
            ).mappings()
            members = (
                await conn.execute(
                    select(t.users.c.user_id, t.users.c.username, t.users.c.group_ids)
                )
            ).mappings()
            return [dict(group) for group in groups], [
                dict(member) for member in members
            ]

    async def save_group(
        self, data: Group, *, actor: str, conn: AsyncConnection
    ) -> dict:
        values = data.model_dump()
        statement = insert(t.groups).values(
            **values, created_by=actor, updated_by=actor
        )
        rows = await conn.execute(
            statement.on_conflict_do_update(
                index_elements=[t.groups.c.group_id],
                set_={**values, "updated_by": actor, "updated_at": utcnow()},
            ).returning(t.groups)
        )
        return dict(rows.mappings().one())
