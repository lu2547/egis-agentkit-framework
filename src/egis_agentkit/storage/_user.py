"""Persistence for enterprise identities and groups."""

from collections.abc import Sequence
from contextlib import asynccontextmanager

from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.client._postgres import PostgresTransaction, transaction_connection
from egis_agentkit.storage.model import Group, User, utcnow

from . import tables as t


class UserStorage:
    def __init__(self, engine: AsyncEngine | PostgresTransaction):
        self.engine = engine

    @asynccontextmanager
    async def write(self):
        """在同一事务内校验并修改用户和组织组。"""
        if isinstance(self.engine, PostgresTransaction):
            self.engine.connection()
            yield self
            return
        async with self.engine.begin() as connection:
            yield UserStorage(PostgresTransaction(connection))

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

    async def upsert_identity(self, identity: User) -> None:
        conn = transaction_connection(self.engine, None)
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

    async def set_admin(self, user_id: str, *, actor: str) -> bool:
        conn = transaction_connection(self.engine, None)
        result = await conn.execute(
            update(t.users)
            .where(t.users.c.user_id == user_id)
            .values(role_type="admin", updated_by=actor, updated_at=utcnow())
        )
        return result.rowcount == 1

    async def list_users(
        self, *, group_id: str | None = None, offset: int = 0, limit: int | None = None
    ) -> list[dict]:
        statement = select(t.users).order_by(t.users.c.user_id)
        if group_id is not None:
            statement = statement.where(t.users.c.group_ids.contains([group_id]))
        if limit is not None:
            statement = statement.offset(offset).limit(limit)
        async with self.engine.connect() as conn:
            rows = (await conn.execute(statement)).mappings()
            return [dict(row) for row in rows]

    async def enabled_group_ids(
        self, group_ids: Sequence[str], *, conn: AsyncConnection | None = None
    ) -> set[str]:
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.enabled_group_ids(group_ids, conn=connection)
        rows = await conn.execute(
            select(t.groups.c.group_id).where(
                t.groups.c.group_id.in_(group_ids), t.groups.c.is_enabled
            )
        )
        return set(rows.scalars())

    async def lock_enabled_admins(self) -> list[str]:
        conn = transaction_connection(self.engine, None)
        rows = await conn.execute(
            select(t.users.c.user_id)
            .where(t.users.c.role_type == "admin", t.users.c.is_enabled)
            .order_by(t.users.c.user_id)
            .with_for_update()
        )
        return list(rows.scalars())

    async def update_user(self, data: User, *, actor: str) -> dict | None:
        conn = transaction_connection(self.engine, None)
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

    async def list_group_options(self) -> list[dict]:
        groups = t.groups
        statement = select(
            groups.c.group_id, groups.c.group_name, groups.c.is_enabled
        ).order_by(groups.c.group_name, groups.c.group_id)
        async with self.engine.connect() as conn:
            return [dict(row) for row in (await conn.execute(statement)).mappings()]

    async def search_recipients(
        self, principal_type: str, query: str, *, offset: int, limit: int
    ) -> list[dict]:
        """按类型查询启用的授权对象，先匹配名称和 ID，再分页。"""
        if principal_type == "user":
            table, id_field, name_field = t.users, t.users.c.user_id, t.users.c.username
        else:
            table = t.groups
            id_field, name_field = table.c.group_id, table.c.group_name
        statement = select(
            id_field.label("principal_id"), name_field.label("display_name")
        ).where(table.c.is_enabled)
        if query:
            statement = statement.where(
                or_(
                    id_field.icontains(query, autoescape=True),
                    name_field.icontains(query, autoescape=True),
                )
            )
        statement = statement.order_by(name_field, id_field).offset(offset).limit(limit)
        async with self.engine.connect() as conn:
            return [dict(row) for row in (await conn.execute(statement)).mappings()]

    async def list_groups_page(self, *, offset: int, limit: int) -> list[dict]:
        groups = (
            select(t.groups)
            .order_by(t.groups.c.group_name, t.groups.c.group_id)
            .offset(offset)
            .limit(limit)
            .subquery()
        )
        member_count = (
            select(func.count())
            .select_from(t.users)
            .where(
                t.users.c.group_ids.contains(func.jsonb_build_array(groups.c.group_id))
            )
            .scalar_subquery()
            .label("member_count")
        )
        statement = select(groups, member_count).order_by(
            groups.c.group_name, groups.c.group_id
        )
        async with self.engine.connect() as conn:
            rows = (await conn.execute(statement)).mappings()
            return [dict(row) for row in rows]

    async def ensure_group(self, data: Group, *, actor: str) -> None:
        """Initialize a group without changing existing settings."""
        conn = transaction_connection(self.engine, None)
        await conn.execute(
            insert(t.groups)
            .values(**data.model_dump(), created_by=actor, updated_by=actor)
            .on_conflict_do_nothing(index_elements=[t.groups.c.group_id])
        )

    async def save_group(self, data: Group, *, actor: str) -> dict:
        conn = transaction_connection(self.engine, None)
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

    async def create_group(self, data: Group, *, actor: str) -> dict:
        conn = transaction_connection(self.engine, None)
        row = await conn.execute(
            insert(t.groups)
            .values(**data.model_dump(), created_by=actor, updated_by=actor)
            .returning(t.groups)
        )
        return dict(row.mappings().one())

    async def update_group(self, data: Group, *, actor: str) -> dict | None:
        conn = transaction_connection(self.engine, None)
        result = await conn.execute(
            update(t.groups)
            .where(t.groups.c.group_id == data.group_id)
            .values(
                **data.model_dump(exclude={"group_id"}),
                updated_by=actor,
                updated_at=utcnow(),
            )
            .returning(t.groups)
        )
        row = result.mappings().first()
        return dict(row) if row is not None else None
