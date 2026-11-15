"""保存资源授权、当前共享版本和共享审核。

核心能力
- 按接收人读取直接授权及其派生授权。
- 使用同一事务写入授权树，并按来源撤销指定分支。
- 保存共享申请及审批结果。
"""

from sqlalchemy import and_, or_, select, text, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.client._postgres import PostgresTransaction, transaction_connection
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import utcnow

GRANT_KEY = (
    "principal_type",
    "principal_id",
    "resource_type",
    "resource_id",
    "resource_view_type",
    "resource_owner_type",
    "resource_owner_id",
    "grant_source_type",
    "origin_key",
)


class AccessStorage:
    """Enterprise grant and review persistence in caller-owned write transactions."""

    def __init__(self, engine: AsyncEngine | PostgresTransaction):
        self.engine = engine

    async def lock_writes(self, *, conn=None):
        conn = transaction_connection(self.engine, conn)
        await conn.execute(text("SELECT pg_advisory_xact_lock(727140202)"))

    async def list_grants(self) -> list[dict]:
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                select(t.resource_permission).order_by(
                    t.resource_permission.c.granted_at.desc(), t.resource_permission.c.id
                )
            )
            return [dict(row) for row in rows.mappings()]

    async def direct_grants(self, kind, identifier, *, conn=None):
        """查询同一资源当前有效的所有直接授权。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.direct_grants(kind, identifier, conn=connection)
        rows = await conn.execute(
            select(t.resource_permission).where(
                t.resource_permission.c.resource_type == kind,
                t.resource_permission.c.resource_id == identifier,
                t.resource_permission.c.grant_source_type.in_(("manual", "share")),
                t.resource_permission.c.revoked_at.is_(None),
            )
        )
        return [dict(row) for row in rows.mappings()]

    async def shared_version(self, kind, identifier, *, conn=None):
        """定位资源的唯一共享版，独立于所有者最新配置。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.shared_version(kind, identifier, conn=connection)
        table = self._get_definition_table(kind)
        if table is not None:
            version = await conn.scalar(
                select(table.c.shared_version).where(table.c.id == identifier)
            )
            return str(version) if version is not None else None
        return await conn.scalar(
            select(t.assets.c.resource_version).where(
                t.assets.c.resource_type == kind,
                t.assets.c.resource_id == identifier,
                t.assets.c.is_shared_current,
            )
        )

    @staticmethod
    def _get_definition_table(kind):
        return {
            "agent": t.agent_definition_summary,
            "team": t.team_definition_summary,
        }.get(kind)

    async def set_shared_version(self, kind, identifier, version, *, conn=None):
        """切换共享配置，同时结束其他发布版本的当前审核状态。"""
        conn = transaction_connection(self.engine, conn)
        table = self._get_definition_table(kind)
        if table is not None:
            await conn.execute(
                update(table)
                .where(table.c.id == identifier)
                .values(shared_version=int(version) if version is not None else None)
            )
        else:
            identity = (
                t.assets.c.resource_type == kind,
                t.assets.c.resource_id == identifier,
            )
            await conn.execute(
                update(t.assets)
                .where(*identity, t.assets.c.is_shared_current)
                .values(is_shared_current=False)
            )
            if version is not None:
                await conn.execute(
                    update(t.assets)
                    .where(*identity, t.assets.c.resource_version == version)
                    .values(is_shared_current=True)
                )
        current = update(t.approvals).where(
            t.approvals.c.resource_type == kind,
            t.approvals.c.resource_id == identifier,
            t.approvals.c.is_current,
        )
        if version is not None:
            current = current.where(t.approvals.c.resource_version != str(version))
        await conn.execute(current.values(is_current=False))
        if version is None:
            await conn.execute(
                update(t.approvals)
                .where(
                    t.approvals.c.resource_type == kind,
                    t.approvals.c.resource_id == identifier,
                    t.approvals.c.review_type == "update",
                    t.approvals.c.review_status_type == "pending",
                )
                .values(review_status_type="cancelled")
            )

    async def has_pending_update(self, kind, identifier, *, conn=None):
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.has_pending_update(kind, identifier, conn=connection)
        return bool(
            await conn.scalar(
                select(t.approvals.c.review_id)
                .where(
                    t.approvals.c.resource_type == kind,
                    t.approvals.c.resource_id == identifier,
                    t.approvals.c.review_type == "update",
                    t.approvals.c.review_status_type == "pending",
                )
                .limit(1)
            )
        )

    async def active_grants(
        self,
        user_id: str,
        group_ids: list[str],
        *,
        kind=None,
        resource_id=None,
        conn=None,
    ) -> list[dict]:
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.active_grants(
                    user_id,
                    group_ids,
                    kind=kind,
                    resource_id=resource_id,
                    conn=connection,
                )
        enabled_groups = select(t.groups.c.group_id).where(
            t.groups.c.group_id.in_(group_ids), t.groups.c.is_enabled
        )
        statement = (
            select(t.resource_permission)
            .where(
                t.resource_permission.c.revoked_at.is_(None),
                or_(
                    and_(
                        t.resource_permission.c.principal_type == "user",
                        t.resource_permission.c.principal_id == user_id,
                    ),
                    and_(
                        t.resource_permission.c.principal_type == "group",
                        t.resource_permission.c.principal_id.in_(enabled_groups),
                    ),
                ),
            )
            .order_by(t.resource_permission.c.granted_at.desc(), t.resource_permission.c.id)
        )
        if kind is not None:
            statement = statement.where(t.resource_permission.c.resource_type == kind)
        if resource_id is not None:
            statement = statement.where(t.resource_permission.c.resource_id == resource_id)
        rows = await conn.execute(statement)
        return [dict(row) for row in rows.mappings()]

    async def upsert_grant(
        self, values: dict, *, actor: str, conn: AsyncConnection | None = None
    ) -> dict:
        conn = transaction_connection(self.engine, conn)
        statement = insert(t.resource_permission).values(
            **values, granted_by=actor, granted_at=utcnow()
        )
        result = await conn.execute(
            statement.on_conflict_do_update(
                index_elements=[t.resource_permission.c[key] for key in GRANT_KEY],
                index_where=t.resource_permission.c.revoked_at.is_(None),
                set_={"origin_key": statement.excluded.origin_key},
            ).returning(t.resource_permission)
        )
        return dict(result.mappings().one())

    async def active_grant(self, grant_id, resource_id, *, conn=None):
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.active_grant(grant_id, resource_id, conn=connection)
        row = (
            (
                await conn.execute(
                    select(t.resource_permission).where(
                        t.resource_permission.c.id == grant_id,
                        t.resource_permission.c.resource_id == resource_id,
                        t.resource_permission.c.root_grant_id.is_(None),
                        t.resource_permission.c.revoked_at.is_(None),
                    )
                )
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None

    async def revoke_dependencies(self, root_id, retained_ids, *, actor, conn=None):
        """收回根授权不再使用的依赖路径。"""
        conn = transaction_connection(self.engine, conn)
        await conn.execute(
            update(t.resource_permission)
            .where(
                t.resource_permission.c.root_grant_id == root_id,
                t.resource_permission.c.revoked_at.is_(None),
                t.resource_permission.c.id.not_in(retained_ids),
            )
            .values(revoked_at=utcnow(), revoked_by=actor)
        )

    async def revoke_grant(
        self, grant_id: str, *, actor: str, conn: AsyncConnection | None = None
    ) -> dict | None:
        conn = transaction_connection(self.engine, conn)
        row = (
            (
                await conn.execute(
                    select(t.resource_permission)
                    .where(
                        t.resource_permission.c.id == grant_id,
                        t.resource_permission.c.grant_source_type.in_(("manual", "share")),
                        t.resource_permission.c.root_grant_id.is_(None),
                    )
                    .with_for_update()
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            return None
        if row["revoked_at"] is not None:
            return dict(row)
        now = utcnow()
        await conn.execute(
            update(t.resource_permission)
            .where(
                or_(
                    t.resource_permission.c.id == grant_id,
                    t.resource_permission.c.root_grant_id == grant_id,
                ),
                t.resource_permission.c.revoked_at.is_(None),
            )
            .values(revoked_at=now, revoked_by=actor)
        )
        if row["source_review_id"]:
            await self.update_review(
                row["source_review_id"], {"is_current": False}, conn=conn
            )
        kind, identifier = row["resource_type"], row["resource_id"]
        if row["resource_view_type"] == "shared" and not await self.direct_grants(
            kind, identifier, conn=conn
        ):
            await self.set_shared_version(kind, identifier, None, conn=conn)
        return dict(row)

    async def revoke_resource(
        self, kind, identifier, *, actor, version=None, conn=None
    ):
        """撤销停用资源的直接授权及包含该资源的派生分支。"""
        conn = transaction_connection(self.engine, conn)
        if version is not None:
            # 停用单个资产版本不影响仍引用其他版本的授权路径。
            if str(version) != await self.shared_version(kind, identifier, conn=conn):
                return
            await self.set_shared_version(kind, identifier, None, conn=conn)
            await conn.execute(
                update(t.resource_permission)
                .where(
                    t.resource_permission.c.resource_type == kind,
                    t.resource_permission.c.resource_id == identifier,
                    t.resource_permission.c.grant_source_type.in_(("manual", "share")),
                    t.resource_permission.c.revoked_at.is_(None),
                )
                .values(revoked_at=utcnow(), revoked_by=actor)
            )
            return
        await self.set_shared_version(kind, identifier, None, conn=conn)
        matching = select(t.resource_permission.c.id).where(
            t.resource_permission.c.resource_type == kind,
            t.resource_permission.c.resource_id == identifier,
            t.resource_permission.c.revoked_at.is_(None),
        )
        branch = matching.cte("revoked_branch", recursive=True)
        branch = branch.union(
            select(t.resource_permission.c.id)
            .join(branch, t.resource_permission.c.parent_grant_id == branch.c.id)
            .where(t.resource_permission.c.revoked_at.is_(None))
        )
        rows = await conn.execute(
            update(t.resource_permission)
            .where(t.resource_permission.c.id.in_(select(branch.c.id)))
            .values(revoked_at=utcnow(), revoked_by=actor)
            .returning(
                t.resource_permission.c.grant_source_type,
                t.resource_permission.c.source_review_id,
            )
        )
        reviews = [
            row.source_review_id for row in rows if row.grant_source_type == "share"
        ]
        if reviews:
            await conn.execute(
                update(t.approvals)
                .where(t.approvals.c.review_id.in_(reviews))
                .values(is_current=False)
            )

    async def get_approved_review(
        self,
        resource_id: str,
        *,
        resource_type: str | None = None,
        review_id: str | None = None,
        version: str | None = None,
        conn: AsyncConnection | None = None,
    ) -> dict | None:
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.get_approved_review(
                    resource_id,
                    resource_type=resource_type,
                    review_id=review_id,
                    version=version,
                    conn=connection,
                )
        statement = select(t.approvals).where(
            t.approvals.c.resource_id == resource_id,
            t.approvals.c.is_current,
            t.approvals.c.review_status_type == "approved",
        )
        if resource_type is not None:
            statement = statement.where(t.approvals.c.resource_type == resource_type)
        if review_id is not None:
            statement = statement.where(t.approvals.c.review_id == review_id)
        if version is not None:
            statement = statement.where(t.approvals.c.resource_version == str(version))
        row = (
            (
                await conn.execute(
                    statement.order_by(t.approvals.c.reviewed_at.desc()).limit(1)
                )
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None

    async def insert_review(
        self, values: dict, *, conn: AsyncConnection | None = None
    ) -> dict:
        conn = transaction_connection(self.engine, conn)
        result = await conn.execute(
            insert(t.approvals).values(**values).returning(t.approvals)
        )
        return dict(result.mappings().one())

    async def list_reviews(self, *, owner_id: str | None = None) -> list[dict]:
        statement = select(t.approvals).order_by(t.approvals.c.submitted_at.desc())
        if owner_id is not None:
            statement = statement.where(t.approvals.c.owner_id == owner_id)
        async with self.engine.connect() as conn:
            rows = await conn.execute(statement)
            return [dict(row) for row in rows.mappings()]

    async def locked_review(
        self, review_id: str, *, conn: AsyncConnection | None = None
    ) -> dict | None:
        conn = transaction_connection(self.engine, conn)
        row = (
            (
                await conn.execute(
                    select(t.approvals)
                    .where(t.approvals.c.review_id == review_id)
                    .with_for_update()
                )
            )
            .mappings()
            .first()
        )
        return dict(row) if row is not None else None

    async def update_review(
        self, review_id: str, values: dict, *, conn: AsyncConnection | None = None
    ) -> dict:
        conn = transaction_connection(self.engine, conn)
        result = await conn.execute(
            update(t.approvals)
            .where(t.approvals.c.review_id == review_id)
            .values(**values)
            .returning(t.approvals)
        )
        return dict(result.mappings().one())
