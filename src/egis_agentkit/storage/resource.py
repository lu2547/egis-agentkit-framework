from sqlalchemy import or_, select, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.models import Asset
from egis_agentkit.storage import tables as t


def _asset_identity(resource_type: str, resource_id: str):
    return (
        t.assets.c.resource_type == resource_type,
        t.assets.c.resource_id == resource_id,
    )


class ResourceAssetStorage:
    """Persist catalog identities and immutable resource versions."""

    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def _first(
        self, statement, *, conn: AsyncConnection | None = None
    ) -> dict | None:
        if conn is None:
            async with self.engine.connect() as connection:
                return await self._first(statement, conn=connection)
        row = (await conn.execute(statement)).mappings().first()
        return dict(row) if row is not None else None

    async def find_asset(
        self,
        resource_type: str,
        resource_id: str,
        *,
        enabled_only: bool = False,
        conn: AsyncConnection | None = None,
    ) -> dict | None:
        statement = select(t.assets).where(*_asset_identity(resource_type, resource_id))
        if enabled_only:
            statement = statement.where(t.assets.c.is_enabled)
        return await self._first(statement, conn=conn)

    async def get(
        self,
        resource_type: str,
        resource_id: str,
        version: str,
        *,
        conn: AsyncConnection | None = None,
    ) -> dict | None:
        return await self._first(
            select(t.assets).where(
                *_asset_identity(resource_type, resource_id),
                t.assets.c.resource_version == version,
            ),
            conn=conn,
        )

    async def list_versions(
        self,
        resource_type: str,
        *,
        resource_ids: list[str] | set[str] | None = None,
        conn: AsyncConnection | None = None,
    ) -> list[dict]:
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.list_versions(
                    resource_type, resource_ids=resource_ids, conn=connection
                )
        statement = select(t.assets).where(t.assets.c.resource_type == resource_type)
        if resource_ids is not None:
            statement = statement.where(t.assets.c.resource_id.in_(resource_ids))
        rows = await conn.execute(statement)
        return [dict(row) for row in rows.mappings()]

    async def list_enabled_by_source_name(
        self, resource_type: str, name: str
    ) -> list[dict]:
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                select(t.assets).where(
                    t.assets.c.resource_type == resource_type,
                    t.assets.c.is_enabled,
                    or_(
                        t.assets.c.resource_id == name,
                        t.assets.c.payload["nacos_name"].astext == name,
                    ),
                )
            )
            return [dict(row) for row in rows.mappings()]

    async def page_versions(
        self, resource_type: str, after: str, *, limit: int
    ) -> tuple[list[str], list[dict]]:
        kind = t.assets.c.resource_type == resource_type
        async with self.engine.connect() as conn:
            identifiers = list(
                (
                    await conn.execute(
                        select(t.assets.c.resource_id)
                        .where(kind, t.assets.c.resource_id > after)
                        .distinct()
                        .order_by(t.assets.c.resource_id)
                        .limit(limit)
                    )
                ).scalars()
            )
            if not identifiers:
                return [], []
            rows = await conn.execute(
                select(t.assets)
                .where(kind, t.assets.c.resource_id.in_(identifiers))
                .order_by(t.assets.c.resource_id, t.assets.c.resource_version)
            )
            return identifiers, [dict(row) for row in rows.mappings()]

    async def lock_resource(
        self, resource_type: str, resource_id: str, *, conn: AsyncConnection
    ) -> None:
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:identity,0))"),
            {"identity": resource_type + ":" + resource_id},
        )

    async def insert_asset(
        self, asset: Asset, *, actor: str, conn: AsyncConnection
    ) -> None:
        await conn.execute(
            t.assets.insert().values(
                **asset.model_dump(), created_by=actor, updated_by=actor
            )
        )

    async def locked_version(
        self,
        resource_type: str,
        resource_id: str,
        version: str | None,
        *,
        conn: AsyncConnection,
    ) -> dict | None:
        return await self._first(
            select(t.assets)
            .where(
                *_asset_identity(resource_type, resource_id),
                t.assets.c.resource_version == version,
            )
            .with_for_update(),
            conn=conn,
        )

    async def version_is_enabled(
        self, resource_type: str, resource_id: str, version: str
    ) -> bool:
        async with self.engine.connect() as conn:
            enabled = await conn.scalar(
                select(t.assets.c.is_enabled).where(
                    *_asset_identity(resource_type, resource_id),
                    t.assets.c.resource_version == version,
                )
            )
        return bool(enabled)
