"""保存不可变资源版本、附件与 MCP 连接凭据。"""

from contextlib import asynccontextmanager

from sqlalchemy import and_, func, or_, select, text, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.client._postgres import PostgresTransaction, transaction_connection
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset, utcnow
from egis_agentkit.storage.model._resource import (
    MCP_MANAGED_FLAG,
    MCP_STORAGE_VERSION,
    SYSTEM_DEFAULT_TOOL_SOURCES,
    asset_version_order,
    is_managed_connector,
)

from .skill._definition import SkillDefinitionStorage


def _asset_identity(resource_type: str, resource_id: str):
    return (
        t.assets.c.resource_type == resource_type,
        t.assets.c.resource_id == resource_id,
    )


class ResourceAssetStorage:
    """提供目录资源的统一持久化入口，技能使用独立定义表。"""

    def __init__(self, engine: AsyncEngine | PostgresTransaction):
        self.engine = engine
        self.skills = SkillDefinitionStorage(engine)

    async def delete_skill(self, skill_id: str, *, actor: str) -> None:
        conn = transaction_connection(self.engine, None)
        await self.skills.delete(skill_id, actor=actor, conn=conn)

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
        """读取资源当前版本，连接器直接使用当前配置的存储键。"""
        if resource_type == "skill":
            return await self.skills.find_asset(
                resource_id, enabled_only=enabled_only, conn=conn
            )
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
        if resource_type == "skill":
            return await self.skills.get(resource_id, version, conn=conn)
        return await self._first(
            select(t.assets).where(
                *_asset_identity(resource_type, resource_id),
                t.assets.c.resource_version == version,
            ),
            conn=conn,
        )

    async def owner_display_names(self, owner_ids):
        if not owner_ids:
            return {}
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                select(t.users.c.user_id, t.users.c.username).where(
                    t.users.c.user_id.in_(owner_ids)
                )
            )
            return dict(rows.all())

    async def get_many(self, resource_type, identities, *, conn=None):
        """Read exact versions using only the identities required by this operation."""
        identities = set(identities)
        if not identities:
            return []
        if resource_type == "skill":
            return await self.skills.get_many(identities, conn=conn)
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.get_many(resource_type, identities, conn=connection)
        rows = await conn.execute(
            select(t.assets).where(
                t.assets.c.resource_type == resource_type,
                tuple_(t.assets.c.resource_id, t.assets.c.resource_version).in_(
                    identities
                ),
            )
        )
        return [dict(row) for row in rows.mappings()]

    async def lock_resources(self, resource_type, resource_ids):
        """Acquire the same resource locks in one round trip and stable order."""
        identities = sorted({resource_type + ":" + rid for rid in resource_ids})
        if not identities:
            return
        conn = transaction_connection(self.engine, None)
        await conn.execute(
            text(
                "SELECT pg_advisory_xact_lock(hashtextextended(identity,0)) "
                "FROM (SELECT unnest(CAST(:identities AS text[])) AS identity "
                "ORDER BY identity) AS ordered_resources"
            ),
            {"identities": identities},
        )

    async def insert_assets(self, assets, *, actor):
        """Insert an already-validated immutable tool batch in one statement."""
        if not assets:
            return
        conn = transaction_connection(self.engine, None)
        await conn.execute(
            t.assets.insert(),
            [
                dict(**asset.model_dump(), created_by=actor, updated_by=actor)
                for asset in assets
            ],
        )

    async def enable_versions(self, resource_type, identities, *, actor):
        if not identities:
            return
        conn = transaction_connection(self.engine, None)
        await conn.execute(
            t.assets.update()
            .where(
                t.assets.c.resource_type == resource_type,
                tuple_(t.assets.c.resource_id, t.assets.c.resource_version).in_(
                    identities
                ),
            )
            .values(is_enabled=True, updated_by=actor, updated_at=utcnow())
        )

    async def list_versions(
        self,
        resource_type: str,
        *,
        resource_ids: list[str] | set[str] | None = None,
        conn: AsyncConnection | None = None,
    ) -> list[dict]:
        if resource_type == "skill":
            return await self.skills.list_versions(resource_ids=resource_ids, conn=conn)
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

    async def list_current_skill_versions(
        self,
        *,
        resource_ids: list[str] | set[str] | None = None,
        conn: AsyncConnection | None = None,
    ) -> list[dict]:
        """仅返回 owner 和 shared 指针选中的技能版本。"""
        return await self.skills.list_versions(
            resource_ids=resource_ids, current_only=True, conn=conn
        )

    async def page_versions(
        self, resource_type: str, after: str, *, limit: int
    ) -> tuple[list[str], list[dict]]:
        if resource_type == "skill":
            return await self.skills.page_versions(after, limit=limit)
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

    async def page_connector_catalog(self, *, company_group_id, after, limit, query):
        """Read one page of enabled managed connectors with an active company grant."""
        assets, grants = t.assets, t.resource_permission
        active_company = select(t.groups.c.group_id).where(
            t.groups.c.group_id == company_group_id, t.groups.c.is_enabled
        )
        company_grant = (
            select(grants.c.id)
            .where(
                grants.c.resource_type == "mcp",
                grants.c.resource_id == assets.c.resource_id,
                grants.c.principal_type == "group",
                grants.c.principal_id.in_(active_company),
                grants.c.resource_view_type == "system",
                grants.c.resource_owner_type == "system",
                grants.c.resource_owner_id == "system",
                grants.c.grant_source_type == "manual",
                grants.c.root_grant_id.is_(None),
                grants.c.revoked_at.is_(None),
            )
            .exists()
        )
        statement = select(assets).where(
            assets.c.resource_type == "mcp",
            assets.c.resource_version == MCP_STORAGE_VERSION,
            assets.c.owner_type == "system",
            assets.c.owner_id == "system",
            assets.c.payload.contains({MCP_MANAGED_FLAG: True}),
            assets.c.is_enabled,
            company_grant,
        )
        if after is not None:
            statement = statement.where(assets.c.resource_id > after)
        if query:
            name = func.coalesce(
                assets.c.payload["display_name"].astext, assets.c.resource_id
            )
            description = func.coalesce(assets.c.payload["description"].astext, "")
            statement = statement.where(
                func.concat(name, " ", description).icontains(query, autoescape=True)
            )
        statement = statement.order_by(assets.c.resource_id).limit(limit)
        async with self.engine.connect() as conn:
            return [dict(row) for row in (await conn.execute(statement)).mappings()]

    async def catalog_tool_versions(self, *, after, batch_size, default_ids, deployed):
        """Stream eligible tool versions in bounded batches, grouped by resource ID."""
        assets = t.assets
        platform = and_(
            assets.c.owner_type == "system",
            assets.c.payload["exposure"].astext == "model",
            assets.c.payload["source"].astext.in_(SYSTEM_DEFAULT_TOOL_SOURCES),
        )
        if default_ids is not None:
            platform = and_(platform, assets.c.resource_id.in_(default_ids))
        eligible = platform
        if deployed:
            eligible = or_(
                platform,
                tuple_(
                    assets.c.resource_id,
                    assets.c.resource_version,
                    assets.c.checksum_sha256,
                ).in_(deployed),
            )
        statement = select(assets).where(
            assets.c.resource_type == "tool", assets.c.is_enabled, eligible
        )
        if after is not None:
            statement = statement.where(assets.c.resource_id > after)
        statement = statement.order_by(assets.c.resource_id, assets.c.resource_version)
        async with self.engine.connect() as conn:
            async with conn.stream(statement) as rows:
                async for row in rows.mappings().yield_per(batch_size):
                    yield dict(row)

    async def page_admin_versions(self, kind, *, offset, limit):
        """按管理表的版本顺序分页，先过滤已受管连接器。"""
        if kind == "skill":
            return await self.skills.page_admin_versions(offset=offset, limit=limit)
        assets = t.assets
        statement = select(assets).where(assets.c.resource_type == kind)
        if kind == "mcp":
            statement = statement.where(
                assets.c.owner_type == "system",
                assets.c.owner_id == "system",
                assets.c.resource_version == MCP_STORAGE_VERSION,
                assets.c.payload.contains({MCP_MANAGED_FLAG: True}),
            )
        statement = (
            statement.order_by(
                assets.c.resource_id,
                assets.c.publication_revision,
                assets.c.resource_version,
            )
            .offset(offset)
            .limit(limit)
        )
        async with self.engine.connect() as conn:
            return [dict(row) for row in (await conn.execute(statement)).mappings()]

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
        if resource_type == "skill":
            return await self.skills.next_publication_revision(resource_id, conn=conn)
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
        if asset.resource_type == "skill":
            await self.skills.insert_asset(asset, actor=actor, conn=conn)
            return
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
        if resource_type == "skill":
            return await self.skills.get(
                resource_id, version, conn=conn, for_update=True
            )
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
        if resource_type == "skill":
            asset = await self.skills.get(resource_id, version)
            return bool(asset and asset["is_enabled"])
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
        if kind == "skill":
            return await self.skills.set_enabled(
                rid, version, enabled, actor=actor, conn=conn
            )
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

    async def read_many(self, user_id, connection_ids):
        """Read status rows for one user and only this page's connection hashes."""
        if not connection_ids:
            return {}
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                select(t.mcp_credentials).where(
                    t.mcp_credentials.c.user_id == user_id,
                    t.mcp_credentials.c.connection_id.in_(connection_ids),
                )
            )
            return {row["connection_id"]: dict(row) for row in rows.mappings()}

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
