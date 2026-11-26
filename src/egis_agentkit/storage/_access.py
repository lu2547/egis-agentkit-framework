"""保存资源授权、当前共享版本和共享审核。

核心能力
- 按接收人读取直接授权及其派生授权。
- 使用同一事务写入授权树，并按来源撤销指定分支。
- 保存共享申请及审批结果。
"""

from collections.abc import Collection

from sqlalchemy import and_, func, or_, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.client._postgres import PostgresTransaction, transaction_connection
from egis_agentkit.errors import NotFound
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

    async def definition_listing_grants(self, user_id, group_ids, resources, grant_ids):
        """列表一次读取用户可用的直接授权，以及会话明确引用的授权状态。"""
        if not resources:
            return []
        grants = t.resource_permission
        enabled_groups = select(t.groups.c.group_id).where(
            t.groups.c.group_id.in_(group_ids), t.groups.c.is_enabled
        )
        recipient = or_(
            and_(grants.c.principal_type == "user", grants.c.principal_id == user_id),
            and_(
                grants.c.principal_type == "group",
                grants.c.principal_id.in_(enabled_groups),
            ),
        )
        query = (
            select(grants, recipient.label("is_for_user"))
            .where(
                grants.c.revoked_at.is_(None),
                grants.c.root_grant_id.is_(None),
                tuple_(grants.c.resource_type, grants.c.resource_id).in_(resources),
                or_(recipient, grants.c.id.in_(grant_ids)),
            )
            .order_by(grants.c.granted_at.desc(), grants.c.id)
        )
        async with self.engine.connect() as conn:
            return [dict(row) for row in (await conn.execute(query)).mappings()]

    async def active_root_grants(self, grant_ids):
        if not grant_ids:
            return []
        grants = t.resource_permission
        async with self.engine.connect() as conn:
            return [
                dict(row)
                for row in (
                    await conn.execute(
                        select(grants).where(
                            grants.c.id.in_(grant_ids),
                            grants.c.root_grant_id.is_(None),
                            grants.c.revoked_at.is_(None),
                        )
                    )
                ).mappings()
            ]

    async def authorization_grants(
        self, user_id, group_ids, resources, *, grant_ids=()
    ):
        """One scoped grant set, including dependencies and explicitly pinned sources."""
        if not resources:
            return []
        grants = t.resource_permission
        enabled_groups = select(t.groups.c.group_id).where(
            t.groups.c.group_id.in_(group_ids), t.groups.c.is_enabled
        )
        recipient = or_(
            and_(grants.c.principal_type == "user", grants.c.principal_id == user_id),
            and_(
                grants.c.principal_type == "group",
                grants.c.principal_id.in_(enabled_groups),
            ),
        )
        query = (
            select(grants, recipient.label("is_for_user"))
            .where(
                grants.c.revoked_at.is_(None),
                tuple_(grants.c.resource_type, grants.c.resource_id).in_(resources),
                or_(recipient, grants.c.id.in_(grant_ids)),
            )
            .order_by(grants.c.granted_at.desc(), grants.c.id)
        )
        async with self.engine.connect() as conn:
            return [dict(row) for row in (await conn.execute(query)).mappings()]

    async def pending_updates(self, resources):
        if not resources:
            return set()
        reviews = t.approvals
        query = (
            select(reviews.c.resource_type, reviews.c.resource_id)
            .where(
                tuple_(reviews.c.resource_type, reviews.c.resource_id).in_(resources),
                reviews.c.review_type == "update",
                reviews.c.review_status_type == "pending",
            )
            .distinct()
        )
        async with self.engine.connect() as conn:
            return set((await conn.execute(query)).all())

    async def approved_reviews(self, review_ids):
        """批量核对没有授权 ID 的显式审核引用。"""
        if not review_ids:
            return {}
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(t.approvals).where(
                        t.approvals.c.review_id.in_(review_ids),
                        t.approvals.c.is_current,
                        t.approvals.c.review_status_type == "approved",
                    )
                )
            ).mappings()
            return {row["review_id"]: dict(row) for row in rows}

    async def skill_write_state(
        self,
        identifier,
        *,
        target=None,
        version=None,
        owner_version=False,
        owner_id=None,
    ):
        """Join this locked item's version, recipient and sharing predicates."""
        from .skill._definition import _asset_record, _definition_query

        summary, detail = t.skill_summary, t.skill_detail
        shared = detail.alias("shared_skill")
        query = (
            _definition_query()
            .outerjoin(
                shared,
                and_(
                    shared.c.skill_id == summary.c.skill_id,
                    shared.c.version == summary.c.shared_version,
                ),
            )
            .add_columns(shared.c.resource_version.label("_shared_resource_version"))
        )
        query = query.where(summary.c.skill_id == identifier)
        if version is not None:
            query = query.where(detail.c.resource_version == str(version))
        else:
            selected = (
                summary.c.owner_version
                if owner_version
                else func.coalesce(summary.c.shared_version, summary.c.owner_version)
            )
            query = query.where(detail.c.version == selected)
        if target is not None:
            principal = t.users if target.principal_type == "user" else t.groups
            key = (
                principal.c.user_id
                if target.principal_type == "user"
                else principal.c.group_id
            )
            recipient = (
                select(key)
                .where(key == target.principal_id, principal.c.is_enabled)
                .exists()
            )
            grants = t.resource_permission
            direct = (
                select(grants.c.id)
                .where(
                    grants.c.resource_type == "skill",
                    grants.c.resource_id == identifier,
                    grants.c.grant_source_type == "share",
                    grants.c.revoked_at.is_(None),
                    grants.c.principal_type == target.principal_type,
                    grants.c.principal_id == target.principal_id,
                )
                .exists()
            )
            query = query.add_columns(
                recipient.label("_recipient_enabled"),
                direct.label("_has_share_recipient"),
            )
        pending = (
            select(t.approvals.c.review_id)
            .where(
                t.approvals.c.resource_type == "skill",
                t.approvals.c.resource_id == identifier,
                t.approvals.c.review_type == "update",
                t.approvals.c.review_status_type == "pending",
            )
            .exists()
        )
        query = query.add_columns(pending.label("_has_pending_update"))
        if owner_id is not None:
            query = query.add_columns(
                select(t.users.c.user_id)
                .where(t.users.c.user_id == owner_id)
                .exists()
                .label("_owner_exists"),
                select(t.users.c.user_id)
                .where(t.users.c.user_id == owner_id, t.users.c.is_enabled)
                .exists()
                .label("_owner_enabled"),
            )
        async with self.engine.connect() as conn:
            row = (await conn.execute(query)).mappings().first()
            return _asset_record(row) if row is not None else None

    async def lock_writes(self, *, conn=None):
        conn = transaction_connection(self.engine, conn)
        await conn.execute(text("SELECT pg_advisory_xact_lock(727140202)"))

    async def list_grants(
        self, *, offset: int = 0, limit: int | None = None
    ) -> list[dict]:
        table = t.resource_permission
        statement = select(table).order_by(table.c.granted_at.desc(), table.c.id)
        if limit is not None:
            grants = statement.offset(offset).limit(limit).subquery()
            parent = table.alias()
            statement = (
                select(
                    grants,
                    parent.c.resource_type.label("parent_resource_type"),
                    parent.c.resource_id.label("parent_resource_id"),
                    func.coalesce(t.users.c.username, t.groups.c.group_name).label(
                        "principal_display_name"
                    ),
                )
                .outerjoin(parent, grants.c.parent_grant_id == parent.c.id)
                .outerjoin(
                    t.users,
                    and_(
                        grants.c.principal_type == "user",
                        grants.c.principal_id == t.users.c.user_id,
                    ),
                )
                .outerjoin(
                    t.groups,
                    and_(
                        grants.c.principal_type == "group",
                        grants.c.principal_id == t.groups.c.group_id,
                    ),
                )
                .order_by(grants.c.granted_at.desc(), grants.c.id)
            )
        async with self.engine.connect() as conn:
            rows = await conn.execute(statement)
            return [dict(row) for row in rows.mappings()]

    async def active_grants_for_resources(
        self, kind: str, identifiers: Collection[str]
    ) -> list[dict]:
        if not identifiers:
            return []
        table = t.resource_permission
        statement = (
            select(table)
            .where(
                table.c.resource_type == kind,
                table.c.resource_id.in_(identifiers),
                table.c.revoked_at.is_(None),
            )
            .order_by(table.c.granted_at.desc(), table.c.id)
        )
        async with self.engine.connect() as conn:
            rows = await conn.execute(statement)
            return [dict(row) for row in rows.mappings()]

    async def owned_sharing_roots(self, kind, identifier, owner_id, *, conn=None):
        """Find current entry grants that expose an owner's resource or dependency."""
        conn = transaction_connection(self.engine, conn)
        grants = t.resource_permission
        root = grants.alias("sharing_root")
        rows = await conn.execute(select(root).join(
            grants, root.c.id == func.coalesce(grants.c.root_grant_id, grants.c.id)
        ).where(
            grants.c.resource_type == kind,
            grants.c.resource_id == identifier,
            grants.c.resource_owner_type == "user",
            grants.c.resource_owner_id == owner_id,
            grants.c.grant_source_type.in_(("share", "manual", "dependency")),
            grants.c.revoked_at.is_(None), root.c.revoked_at.is_(None),
        ).distinct())
        return [dict(row) for row in rows.mappings()]

    async def direct_grants(
        self, kind, identifier, *, include_revoked=False, conn=None
    ):
        """查询同一资源当前有效的所有直接授权。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.direct_grants(
                    kind, identifier, include_revoked=include_revoked, conn=connection
                )
        grants = t.resource_permission
        statement = select(grants).where(
            grants.c.resource_type == kind,
            grants.c.resource_id == identifier,
            grants.c.grant_source_type.in_(("manual", "share")),
        )
        if not include_revoked:
            statement = statement.where(grants.c.revoked_at.is_(None))
        return [dict(row) for row in (await conn.execute(statement)).mappings()]

    async def shared_version(self, kind, identifier, *, conn=None):
        """定位资源的唯一共享版，独立于所有者最新配置。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.shared_version(kind, identifier, conn=connection)
        if kind == "skill":
            summary, detail = t.skill_summary, t.skill_detail
            return await conn.scalar(
                select(detail.c.resource_version)
                .join(
                    summary,
                    and_(
                        detail.c.skill_id == summary.c.skill_id,
                        detail.c.version == summary.c.shared_version,
                    ),
                )
                .where(summary.c.skill_id == identifier)
            )
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
        if kind == "skill":
            await self._set_shared_skill_version(identifier, version, conn=conn)
        elif table is not None:
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

    @staticmethod
    async def _set_shared_skill_version(identifier, version, *, conn):
        """按包版本定位技能明细，只更新共享指针。"""
        summary, detail = t.skill_summary, t.skill_detail
        statement = update(summary).where(summary.c.skill_id == identifier)
        if version is None:
            await conn.execute(statement.values(shared_version=None))
            return
        result = await conn.execute(
            statement.where(
                detail.c.skill_id == summary.c.skill_id,
                detail.c.resource_version == version,
            ).values(shared_version=detail.c.version)
        )
        if not result.rowcount:
            raise NotFound("技能版本不存在")

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
            .order_by(
                t.resource_permission.c.granted_at.desc(), t.resource_permission.c.id
            )
        )
        if kind is not None:
            statement = statement.where(t.resource_permission.c.resource_type == kind)
        if resource_id is not None:
            statement = statement.where(
                t.resource_permission.c.resource_id == resource_id
            )
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

    async def upsert_grants(self, values, *, actor, conn=None):
        """Deduplicate a bounded grant layer and preserve existing source IDs."""
        conn = transaction_connection(self.engine, conn)
        unique = {tuple(value.get(key) for key in GRANT_KEY): value for value in values}
        if not unique:
            return []
        now = utcnow()
        rows = list(unique.values())
        result = []
        # asyncpg caps one statement at 32767 arguments. Each grant has at
        # most 19 columns; cap layers independently of the number of roots.
        for offset in range(0, len(rows), 1000):
            statement = insert(t.resource_permission).values(
                [
                    {**value, "granted_by": actor, "granted_at": now}
                    for value in rows[offset : offset + 1000]
                ]
            )
            batch = await conn.execute(
                statement.on_conflict_do_update(
                    index_elements=[t.resource_permission.c[key] for key in GRANT_KEY],
                    index_where=t.resource_permission.c.revoked_at.is_(None),
                    set_={"origin_key": statement.excluded.origin_key},
                ).returning(t.resource_permission)
            )
            result.extend(dict(row) for row in batch.mappings())
        return result

    async def revoke_dependencies_many(self, retained, *, actor, conn=None):
        conn = transaction_connection(self.engine, conn)
        if not retained:
            return
        grants = t.resource_permission
        predicates = [
            and_(grants.c.root_grant_id == root_id, grants.c.id.not_in(ids))
            for root_id, ids in retained.items()
        ]
        await conn.execute(
            update(grants)
            .where(grants.c.revoked_at.is_(None), or_(*predicates))
            .values(revoked_at=utcnow(), revoked_by=actor)
        )

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
                        t.resource_permission.c.grant_source_type.in_(
                            ("manual", "share")
                        ),
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

    async def cancel_pending_reviews(self, kind, identifier, *, conn=None):
        """资源删除后结束该资源的全部待审核申请。"""
        conn = transaction_connection(self.engine, conn)
        reviews = t.approvals
        await conn.execute(
            update(reviews)
            .where(
                reviews.c.resource_type == kind,
                reviews.c.resource_id == identifier,
                reviews.c.review_status_type == "pending",
            )
            .values(review_status_type="cancelled")
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

    async def list_reviews(
        self, *, owner_id: str | None = None, offset: int = 0, limit: int | None = None
    ) -> list[dict]:
        statement = select(*[column for column in t.approvals.c if column.name != "content_snapshot"]).order_by(t.approvals.c.submitted_at.desc())
        if owner_id is not None:
            statement = statement.where(t.approvals.c.owner_id == owner_id)
        if limit is not None:
            statement = (
                statement.order_by(t.approvals.c.review_id).offset(offset).limit(limit)
            )
        selected = statement.subquery("review_page")
        detail = t.skill_detail
        grants = t.resource_permission
        active_share = select(grants.c.id).where(
            grants.c.source_review_id == selected.c.review_id,
            grants.c.resource_type == selected.c.resource_type,
            grants.c.resource_id == selected.c.resource_id,
            grants.c.grant_source_type == "share",
            grants.c.root_grant_id.is_(None),
            grants.c.revoked_at.is_(None),
        ).correlate(selected).exists()
        statement = (
            select(
                selected, detail.c.version.label("_publication_revision"),
                active_share.label("has_active_share"),
            )
            .select_from(selected)
            .outerjoin(
                detail,
                and_(
                    selected.c.resource_type == "skill",
                    selected.c.resource_id == detail.c.skill_id,
                    selected.c.resource_version == detail.c.resource_version,
                    select(t.skill_summary.c.skill_id)
                    .where(
                        t.skill_summary.c.skill_id == detail.c.skill_id,
                        t.skill_summary.c.is_enabled,
                    )
                    .correlate(detail)
                    .exists(),
                ),
            )
            .order_by(selected.c.submitted_at.desc(), selected.c.review_id)
        )
        async with self.engine.connect() as conn:
            rows = await conn.execute(statement)
            return [dict(row) for row in rows.mappings()]

    async def get_review(self, review_id, *, conn=None):
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.get_review(review_id, conn=connection)
        row = (await conn.execute(
            select(t.approvals).where(t.approvals.c.review_id == review_id)
        )).mappings().first()
        return dict(row) if row is not None else None

    async def pending_revocation(self, original_review_id, *, conn=None):
        conn = transaction_connection(self.engine, conn)
        row = (await conn.execute(select(t.approvals).where(
            t.approvals.c.original_review_id == original_review_id,
            t.approvals.c.review_type == "revoke",
            t.approvals.c.review_status_type == "pending",
        ))).mappings().first()
        return dict(row) if row is not None else None

    async def locked_review(
        self, review_id: str, *, conn: AsyncConnection | None = None
    ) -> dict | None:
        conn = transaction_connection(self.engine, conn)
        row = (
            (
                await conn.execute(
                    select(
                        t.approvals,
                        t.skill_detail.c.version.label("_publication_revision"),
                    )
                    .select_from(t.approvals)
                    .outerjoin(
                        t.skill_detail,
                        and_(
                            t.approvals.c.resource_type == "skill",
                            t.approvals.c.resource_id == t.skill_detail.c.skill_id,
                            t.approvals.c.resource_version
                            == t.skill_detail.c.resource_version,
                            select(t.skill_summary.c.skill_id)
                            .where(
                                t.skill_summary.c.skill_id == t.skill_detail.c.skill_id,
                                t.skill_summary.c.is_enabled,
                            )
                            .correlate(t.skill_detail)
                            .exists(),
                        ),
                    )
                    .where(t.approvals.c.review_id == review_id)
                    .with_for_update(of=t.approvals)
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
