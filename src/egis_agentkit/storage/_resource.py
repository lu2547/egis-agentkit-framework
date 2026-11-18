"""保存不可变资源版本、附件与 MCP 连接凭据。"""

from contextlib import asynccontextmanager

from sqlalchemy import func, select, text
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.client._postgres import PostgresTransaction, transaction_connection
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset, utcnow
from egis_agentkit.storage.model._resource import (
    MCP_STORAGE_VERSION,
    asset_version_order,
    is_managed_connector,
    require_skill_publication_revision,
)


def _asset_identity(resource_type: str, resource_id: str):
    return (
        t.assets.c.resource_type == resource_type,
        t.assets.c.resource_id == resource_id,
    )


def _asset_record(row) -> dict:
    if row["resource_type"] == "skill":
        require_skill_publication_revision(row["publication_revision"])
    return dict(row)


class ResourceAssetStorage:
    """持久化目录资源及其版本。"""

    def __init__(self, engine: AsyncEngine | PostgresTransaction):
        self.engine = engine

    async def _first(
        self, statement, *, conn: AsyncConnection | None = None
    ) -> dict | None:
        if conn is None:
            async with self.engine.connect() as connection:
                return await self._first(statement, conn=connection)
        row = (await conn.execute(statement)).mappings().first()
        return _asset_record(row) if row is not None else None

    async def find_asset(
        self,
        resource_type: str,
        resource_id: str,
        *,
        enabled_only: bool = False,
        conn: AsyncConnection | None = None,
    ) -> dict | None:
        """读取资源当前版本，连接器直接使用当前配置的存储键。"""
        if resource_type == "mcp":
            row = await self.get(
                resource_type, resource_id, MCP_STORAGE_VERSION, conn=conn
            )
            if (
                row
                and is_managed_connector(row)
                and (row["is_enabled"] or not enabled_only)
            ):
                return row
            return None
        rows = await self.list_versions(
            resource_type, resource_ids=[resource_id], conn=conn
        )
        return max(
            (row for row in rows if not enabled_only or row["is_enabled"]),
            key=asset_version_order,
            default=None,
        )

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
        return [_asset_record(row) for row in rows.mappings()]

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
            return identifiers, [_asset_record(row) for row in rows.mappings()]

    async def lock_resource(
        self,
        resource_type: str,
        resource_id: str,
        *,
        conn: AsyncConnection | None = None,
    ) -> None:
        """串行化同一资源的发布和启停。"""
        conn = transaction_connection(self.engine, conn)
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:identity,0))"),
            {"identity": resource_type + ":" + resource_id},
        )

    async def next_publication_revision(self, resource_type, resource_id) -> int:
        """在持有资源写锁时计算下一个发布序号。"""
        conn = transaction_connection(self.engine, None)
        current = await conn.scalar(
            select(func.max(t.assets.c.publication_revision)).where(
                *_asset_identity(resource_type, resource_id)
            )
        )
        return (current or 0) + 1

    async def insert_asset(
        self, asset: Asset, *, actor: str, conn: AsyncConnection | None = None
    ) -> None:
        """写入新的资源版本。"""
        conn = transaction_connection(self.engine, conn)
        await conn.execute(
            t.assets.insert().values(
                **asset.model_dump(), created_by=actor, updated_by=actor
            )
        )

    async def update_connector(self, asset: Asset, *, actor: str) -> dict:
        """Replace connection configuration, preserving status and creation audit."""
        conn = transaction_connection(self.engine, None)
        row = await conn.execute(
            t.assets.update()
            .where(
                *_asset_identity(asset.resource_type, asset.resource_id),
                t.assets.c.resource_version == asset.resource_version,
            )
            .values(
                owner_type=asset.owner_type,
                owner_id=asset.owner_id,
                payload=asset.payload,
                checksum_sha256=asset.checksum_sha256,
                publication_revision=asset.publication_revision,
                is_shared_current=False,
                updated_by=actor,
                updated_at=utcnow(),
            )
            .returning(t.assets)
        )
        return dict(row.mappings().one())

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

    async def set_enabled(self, kind, rid, version, enabled, *, actor, conn=None):
        """切换指定版本的可用状态。"""
        conn = transaction_connection(self.engine, conn)
        result = await conn.execute(
            t.assets.update()
            .where(*_asset_identity(kind, rid), t.assets.c.resource_version == version)
            .values(is_enabled=enabled, updated_by=actor, updated_at=utcnow())
        )
        return bool(result.rowcount)


class MCPCredentialStorage:
    """Isolate credentials by user and immutable MCP connection version."""

    def __init__(self, engine: AsyncEngine | PostgresTransaction):
        self.engine = engine

    @asynccontextmanager
    async def write(self):
        """在同一事务内读取锁定凭据并提交修改。"""
        async with self.engine.begin() as connection:
            yield MCPCredentialStorage(PostgresTransaction(connection))

    def _match(self, user_id, connection_id):
        return (
            t.mcp_credentials.c.user_id == user_id,
            t.mcp_credentials.c.connection_id == connection_id,
        )

    async def lock(self, user_id, connection_id):
        conn = transaction_connection(self.engine, None)
        await conn.execute(
            insert(t.mcp_credentials)
            .values(user_id=user_id, connection_id=connection_id)
            .on_conflict_do_nothing()
        )
        return (
            (
                await conn.execute(
                    select(t.mcp_credentials)
                    .where(*self._match(user_id, connection_id))
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )

    async def update(self, user_id, connection_id, **values):
        conn = transaction_connection(self.engine, None)
        await conn.execute(
            t.mcp_credentials.update()
            .where(*self._match(user_id, connection_id))
            .values(**values)
        )

    async def read(self, user_id, connection_id):
        async with self.engine.connect() as conn:
            return (
                (
                    await conn.execute(
                        select(t.mcp_credentials).where(
                            *self._match(user_id, connection_id)
                        )
                    )
                )
                .mappings()
                .first()
            )
