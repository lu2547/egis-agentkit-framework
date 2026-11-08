from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.errors import Conflict
from egis_agentkit.models import utcnow
from egis_agentkit.storage import tables as t


class AccessStorage:
    """Enterprise grant and review persistence in caller-owned write transactions."""

    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def list_grants(self) -> list[dict]:
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                select(t.grants).order_by(t.grants.c.granted_at.desc())
            )
            return [dict(row) for row in rows.mappings()]

    async def active_grants(self, user_id: str, group_ids: list[str]) -> list[dict]:
        enabled_groups = select(t.groups.c.group_id).where(
            t.groups.c.group_id.in_(group_ids), t.groups.c.is_enabled
        )
        statement = select(t.grants).where(
            t.grants.c.revoked_at.is_(None),
            or_(
                and_(
                    t.grants.c.principal_type == "user",
                    t.grants.c.principal_id == user_id,
                ),
                and_(
                    t.grants.c.principal_type == "group",
                    t.grants.c.principal_id.in_(enabled_groups),
                ),
                t.grants.c.principal_type == "all_users",
            ),
        )
        async with self.engine.connect() as conn:
            rows = await conn.execute(statement)
            return [dict(row) for row in rows.mappings()]

    async def upsert_grant(
        self, values: dict, *, actor: str, conn: AsyncConnection
    ) -> dict:
        statement = insert(t.grants).values(
            **values, granted_by=actor, granted_at=utcnow()
        )
        result = await conn.execute(
            statement.on_conflict_do_update(
                index_elements=[
                    t.grants.c.principal_type,
                    t.grants.c.principal_id,
                    t.grants.c.resource_type,
                    t.grants.c.resource_id,
                    t.grants.c.resource_view_type,
                    t.grants.c.resource_owner_type,
                    t.grants.c.resource_owner_id,
                    t.grants.c.grant_source_type,
                ],
                index_where=t.grants.c.revoked_at.is_(None),
                set_={
                    "source_review_id": statement.excluded.source_review_id,
                    "granted_by": statement.excluded.granted_by,
                    "granted_at": statement.excluded.granted_at,
                },
            ).returning(t.grants)
        )
        return dict(result.mappings().one())

    async def revoke_grant(
        self, grant_id: str, *, actor: str, conn: AsyncConnection
    ) -> bool:
        result = await conn.execute(
            update(t.grants)
            .where(t.grants.c.id == grant_id)
            .values(revoked_at=utcnow(), revoked_by=actor)
        )
        return bool(result.rowcount)

    async def approved_review(
        self,
        resource_id: str,
        *,
        resource_type: str | None = None,
        conn: AsyncConnection | None = None,
    ) -> dict | None:
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.approved_review(
                    resource_id, resource_type=resource_type, conn=connection
                )
        statement = select(t.reviews).where(
            t.reviews.c.resource_id == resource_id,
            t.reviews.c.is_current,
            t.reviews.c.review_status_type == "approved",
        )
        if resource_type is not None:
            statement = statement.where(t.reviews.c.resource_type == resource_type)
        row = (await conn.execute(statement)).mappings().first()
        return dict(row) if row is not None else None

    async def insert_review(self, values: dict, *, conn: AsyncConnection) -> dict:
        try:
            result = await conn.execute(
                insert(t.reviews).values(**values).returning(t.reviews)
            )
        except IntegrityError as exc:
            raise Conflict("此资源已有待审核的共享申请") from exc
        return dict(result.mappings().one())

    async def list_reviews(self, *, owner_id: str | None = None) -> list[dict]:
        statement = select(t.reviews).order_by(t.reviews.c.submitted_at.desc())
        if owner_id is not None:
            statement = statement.where(t.reviews.c.owner_id == owner_id)
        async with self.engine.connect() as conn:
            rows = await conn.execute(statement)
            return [dict(row) for row in rows.mappings()]

    async def locked_review(
        self, review_id: str, *, conn: AsyncConnection
    ) -> dict | None:
        row = (
            (
                await conn.execute(
                    select(t.reviews)
                    .where(t.reviews.c.review_id == review_id)
                    .with_for_update()
                )
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None

    async def latest_shared_version(
        self, resource_type: str, resource_id: str, *, conn: AsyncConnection
    ) -> int | None:
        return await conn.scalar(
            select(func.max(t.reviews.c.shared_version)).where(
                t.reviews.c.resource_type == resource_type,
                t.reviews.c.resource_id == resource_id,
            )
        )

    async def clear_current_review(
        self, resource_type: str, resource_id: str, *, conn: AsyncConnection
    ) -> None:
        await conn.execute(
            update(t.reviews)
            .where(
                t.reviews.c.resource_type == resource_type,
                t.reviews.c.resource_id == resource_id,
                t.reviews.c.is_current,
            )
            .values(is_current=False)
        )

    async def update_review(
        self, review_id: str, values: dict, *, conn: AsyncConnection
    ) -> dict:
        result = await conn.execute(
            update(t.reviews)
            .where(t.reviews.c.review_id == review_id)
            .values(**values)
            .returning(t.reviews)
        )
        return dict(result.mappings().one())
