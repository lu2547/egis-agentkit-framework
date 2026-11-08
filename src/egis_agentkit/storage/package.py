from sqlalchemy import and_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine
from sqlalchemy.sql.elements import ColumnElement

from egis_agentkit.models import Asset, utcnow
from egis_agentkit.storage.tables import assets


def _release_key(resource_id: str, version: str) -> ColumnElement[bool]:
    return and_(
        assets.c.resource_type == "assembly",
        assets.c.resource_id == resource_id,
        assets.c.resource_version == version,
    )


class PackageStorage:
    """Persist Agent package release descriptors. Callers own write transactions."""

    def __init__(self, engine: AsyncEngine):
        self.engine = engine

    async def register_release(
        self, asset: Asset, *, actor: str, conn: AsyncConnection
    ) -> dict:
        await conn.execute(
            insert(assets)
            .values(**asset.model_dump(), created_by=actor, updated_by=actor)
            .on_conflict_do_nothing()
        )
        row = (
            (
                await conn.execute(
                    select(assets)
                    .where(_release_key(asset.resource_id, asset.resource_version))
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        return dict(row)

    async def list_releases(self) -> list[dict]:
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                select(assets).where(assets.c.resource_type == "assembly")
            )
            return [dict(row) for row in rows.mappings()]

    async def get_release(self, resource_id: str, version: str) -> dict | None:
        async with self.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        select(assets).where(_release_key(resource_id, version))
                    )
                )
                .mappings()
                .first()
            )
        return dict(row) if row is not None else None

    async def disable_release(
        self, resource_id: str, version: str, *, actor: str, conn: AsyncConnection
    ) -> bool:
        result = await conn.execute(
            update(assets)
            .where(_release_key(resource_id, version))
            .values(is_enabled=False, updated_by=actor, updated_at=utcnow())
        )
        return bool(result.rowcount)

    async def enabled_release_keys(self) -> list[tuple[str, str]]:
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                select(assets.c.resource_id, assets.c.resource_version).where(
                    assets.c.resource_type == "assembly", assets.c.is_enabled
                )
            )
            return [(resource_id, version) for resource_id, version in rows]
