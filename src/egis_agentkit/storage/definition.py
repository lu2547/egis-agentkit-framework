from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.errors import NotFound
from egis_agentkit.models import utcnow
from egis_agentkit.storage import tables as t


class DefinitionStorage:
    """Definition persistence; callers own authorization and write transactions."""

    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def _first(self, statement, *, conn=None):
        if conn is None:
            async with self.engine.connect() as connection:
                return await self._first(statement, conn=connection)
        row = (await conn.execute(statement)).mappings().first()
        return dict(row) if row is not None else None

    async def get_definition(self, definition_id: str, *, conn=None) -> dict:
        row = await self._first(
            select(t.definitions).where(t.definitions.c.id == definition_id), conn=conn
        )
        if row is None:
            raise NotFound()
        return row

    async def locked_definition(
        self, definition_id: str, *, conn: AsyncConnection
    ) -> dict | None:
        return await self._first(
            select(t.definitions)
            .where(t.definitions.c.id == definition_id)
            .with_for_update(),
            conn=conn,
        )

    async def approved_review(
        self, resource_id: str, *, resource_type: str | None = None, conn=None
    ) -> dict | None:
        statement = select(t.reviews).where(
            t.reviews.c.resource_id == resource_id,
            t.reviews.c.is_current,
            t.reviews.c.review_status_type == "approved",
        )
        if resource_type is not None:
            statement = statement.where(t.reviews.c.resource_type == resource_type)
        return await self._first(statement, conn=conn)

    async def read_catalog(
        self, definition_type="agent", *, conn=None
    ) -> tuple[list[dict], dict]:
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.read_catalog(definition_type, conn=connection)
        rows = [
            dict(row)
            for row in (
                await conn.execute(
                    select(t.definitions)
                    .where(t.definitions.c.definition_type == definition_type)
                    .order_by(t.definitions.c.created_at.desc())
                )
            ).mappings()
        ]
        reviews = (
            await conn.execute(
                select(t.reviews.c.resource_id, t.reviews.c.review_status_type)
                .order_by(t.reviews.c.submitted_at)
            )
        ).all()
        return rows, dict(reviews)

    async def ensure_personal(self, values: dict, *, actor: str, conn: AsyncConnection):
        await conn.execute(
            insert(t.definitions)
            .values(**values, created_by=actor, updated_by=actor)
            .on_conflict_do_nothing()
        )

    async def insert_definition(
        self, values: dict, *, actor: str, conn: AsyncConnection
    ) -> dict:
        row = (
            await conn.execute(
                insert(t.definitions)
                .values(**values, created_by=actor, updated_by=actor)
                .returning(t.definitions)
            )
        ).mappings().one()
        return dict(row)

    async def update_definition(
        self, definition_id: str, values: dict, *, actor: str, conn: AsyncConnection
    ) -> dict:
        row = (
            await conn.execute(
                update(t.definitions)
                .where(t.definitions.c.id == definition_id)
                .values(**values, updated_by=actor, updated_at=utcnow())
                .returning(t.definitions)
            )
        ).mappings().one()
        return dict(row)

    async def disable_definition(
        self, definition_id: str, *, actor: str, conn: AsyncConnection
    ) -> None:
        await conn.execute(
            update(t.definitions)
            .where(t.definitions.c.id == definition_id)
            .values(is_enabled=False, updated_by=actor, updated_at=utcnow())
        )
