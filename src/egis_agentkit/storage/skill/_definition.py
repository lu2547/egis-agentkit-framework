"""持久化技能定义和正式版本。

核心能力
- 用稳定 ID 记录技能归属、来源和 owner/shared 版本。
- 发布时追加明细，在同一事务内推进 owner 版本。
- 按当前指针读取目录候选，按明确包版本读取历史明细。
"""

from sqlalchemy import and_, case, func, or_, select, tuple_
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.client._postgres import PostgresTransaction
from egis_agentkit.errors import Conflict, NotFound
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset, utcnow
from egis_agentkit.storage.model._resource import require_skill_publication_revision

_DETAIL_FIELDS = (
    "display_name",
    "description",
    "category",
    "avatar",
    "avatar_checksum",
    "files",
    "import_job_id",
)


def _definition_query(*, current_only=False):
    summary, detail = t.skill_summary, t.skill_detail
    statement = (
        select(
            detail,
            summary.c.owner_type,
            summary.c.owner_id,
            summary.c.source_user_id,
            summary.c.owner_version,
            summary.c.shared_version,
            summary.c.is_enabled.label("summary_enabled"),
        )
        .select_from(summary.join(detail, summary.c.skill_id == detail.c.skill_id))
        .where(summary.c.is_enabled)
    )
    if current_only:
        statement = statement.where(
            detail.c.version.in_((summary.c.owner_version, summary.c.shared_version))
        )
    return statement


def _catalog_skill_candidates(user):
    """Drive catalog reads from owned IDs and recipient-scoped direct grants."""
    summary, grant = t.skill_summary, t.resource_permission
    own = select(summary.c.skill_id).where(
        summary.c.owner_type == "user",
        summary.c.owner_id == user.user_id,
        summary.c.is_enabled,
    )
    groups = select(t.groups.c.group_id).where(
        t.groups.c.group_id.in_(user.group_ids), t.groups.c.is_enabled
    )
    granted = (
        select(grant.c.resource_id.label("skill_id"))
        .select_from(grant.join(summary, grant.c.resource_id == summary.c.skill_id))
        .where(
            grant.c.resource_type == "skill",
            grant.c.revoked_at.is_(None),
            grant.c.root_grant_id.is_(None),
            grant.c.grant_source_type != "owner",
            grant.c.resource_view_type
            == case((summary.c.owner_type == "system", "system"), else_="shared"),
            summary.c.is_enabled,
            or_(
                and_(
                    grant.c.principal_type == "user",
                    grant.c.principal_id == user.user_id,
                ),
                and_(
                    grant.c.principal_type == "group", grant.c.principal_id.in_(groups)
                ),
            ),
        )
    )
    branches = [granted]
    if user.role_type == "admin":
        branches.append(
            select(summary.c.skill_id).where(
                summary.c.owner_type == "system", summary.c.is_enabled
            )
        )
    # UNION removes overlapping user/group grants and owned+granted identities
    # before joining version payloads, so permissions never multiply page rows.
    return own.union(*branches).subquery("catalog_skill_ids")


def _catalog_skill_visibility(user):
    """Select the current pointer after joining the scoped candidate ID set."""
    summary, detail = t.skill_summary, t.skill_detail
    system = summary.c.owner_type == "system"
    own = and_(summary.c.owner_type == "user", summary.c.owner_id == user.user_id)
    selected = detail.c.version == case(
        (or_(system, own), summary.c.owner_version), else_=summary.c.shared_version
    )
    visible = or_(own, detail.c.is_enabled)
    if user.role_type == "admin":
        visible = or_(system, visible)
    return summary.c.is_enabled, selected, visible


def _catalog_skill_category():
    detail = t.skill_detail
    return func.coalesce(detail.c.category, detail.c.payload["category"].astext)


def _asset_record(row) -> dict:
    """为目录、授权与固定绑定保留现有资源字段。"""
    result = dict(row)
    payload = dict(result["payload"])
    for field in _DETAIL_FIELDS:
        value = result.pop(field)
        if value is not None:
            payload[field] = value
    revision = result.pop("version")
    summary_enabled = result.pop("summary_enabled")
    result.update(
        resource_type="skill",
        resource_id=result.pop("skill_id"),
        publication_revision=revision,
        is_enabled=summary_enabled and result["is_enabled"],
        payload=payload,
    )
    return result


class SkillDefinitionStorage:
    """分别按当前指针和明确版本读取技能，发布时仅移动 owner 指针。"""

    def __init__(self, engine: AsyncEngine | PostgresTransaction) -> None:
        self.engine = engine

    async def _rows(
        self, statement, *, conn: AsyncConnection | None = None
    ) -> list[dict]:
        if conn is None:
            async with self.engine.connect() as connection:
                return await self._rows(statement, conn=connection)
        return [
            _asset_record(row) for row in (await conn.execute(statement)).mappings()
        ]

    async def find_asset(
        self,
        resource_id: str,
        *,
        enabled_only: bool = False,
        conn: AsyncConnection | None = None,
    ) -> dict | None:
        """按 owner 指针查找当前版本。"""
        summary, detail = t.skill_summary, t.skill_detail
        statement = _definition_query().where(
            summary.c.skill_id == resource_id,
            summary.c.owner_version == detail.c.version,
        )
        if enabled_only:
            statement = statement.where(summary.c.is_enabled, detail.c.is_enabled)
        rows = await self._rows(statement, conn=conn)
        return rows[0] if rows else None

    async def get(
        self,
        resource_id: str,
        version: str | None,
        *,
        conn: AsyncConnection | None = None,
        for_update: bool = False,
    ) -> dict | None:
        """按包版本查找明细，发布历史和固定绑定使用此入口。"""
        detail = t.skill_detail
        statement = _definition_query().where(
            detail.c.skill_id == resource_id, detail.c.resource_version == version
        )
        if for_update:
            statement = statement.with_for_update(of=detail)
        rows = await self._rows(statement, conn=conn)
        return rows[0] if rows else None

    async def get_many(self, identities, *, conn=None):
        """Read a bounded set of pinned skill package versions."""
        if not identities:
            return []
        detail = t.skill_detail
        return await self._rows(
            _definition_query().where(
                tuple_(detail.c.skill_id, detail.c.resource_version).in_(identities)
            ),
            conn=conn,
        )

    async def list_versions(
        self,
        *,
        resource_ids: list[str] | set[str] | None = None,
        current_only: bool = False,
        conn: AsyncConnection | None = None,
    ) -> list[dict]:
        summary = t.skill_summary
        statement = _definition_query(current_only=current_only)
        if resource_ids is not None:
            statement = statement.where(summary.c.skill_id.in_(resource_ids))
        return await self._rows(statement, conn=conn)

    async def page_versions(
        self, after: str, *, limit: int
    ) -> tuple[list[str], list[dict]]:
        """先选一页稳定 ID，再读取这些技能的 owner/shared 版本。"""
        summary, detail = t.skill_summary, t.skill_detail
        page = (
            select(summary.c.skill_id)
            .where(summary.c.skill_id > after, summary.c.is_enabled)
            .order_by(summary.c.skill_id)
            .limit(limit)
            .subquery()
        )
        rows = await self._rows(
            _definition_query(current_only=True)
            .join(page, summary.c.skill_id == page.c.skill_id)
            .order_by(summary.c.skill_id, detail.c.resource_version)
        )
        return list(dict.fromkeys(row["resource_id"] for row in rows)), rows

    async def page_catalog(self, user, *, after, limit, query, category, mine):
        """Filter current skills and personal selections before keyset pagination."""
        summary, detail = t.skill_summary, t.skill_detail
        candidates = _catalog_skill_candidates(user)
        statement = (
            _definition_query()
            .join(candidates, summary.c.skill_id == candidates.c.skill_id)
            .where(*_catalog_skill_visibility(user))
        )
        if after is not None:
            statement = statement.where(summary.c.skill_id > after)
        if query:
            name = func.coalesce(
                detail.c.display_name,
                detail.c.payload["display_name"].astext,
                summary.c.skill_id,
            )
            description = func.coalesce(
                detail.c.description, detail.c.payload["description"].astext, ""
            )
            statement = statement.where(
                func.concat(name, " ", description).icontains(query, autoescape=True)
            )
        if category:
            statement = statement.where(_catalog_skill_category() == category)
        if mine:
            installation = t.agent_personal_resources
            statement = statement.where(
                select(installation.c.resource_id)
                .where(
                    installation.c.user_id == user.user_id,
                    installation.c.resource_type == "skill",
                    installation.c.resource_id == summary.c.skill_id,
                )
                .exists()
            )
        return await self._rows(statement.order_by(summary.c.skill_id).limit(limit))

    async def catalog_metadata(self, user):
        """Aggregate category tabs and the batch-share action for visible skills."""
        summary, detail = t.skill_summary, t.skill_detail
        category = _catalog_skill_category()
        joined = summary.join(detail, summary.c.skill_id == detail.c.skill_id)
        candidates = _catalog_skill_candidates(user)
        categories = (
            select(category)
            .select_from(
                joined.join(candidates, summary.c.skill_id == candidates.c.skill_id)
            )
            .where(*_catalog_skill_visibility(user), category != "")
            .group_by(category)
            .order_by(func.min(summary.c.skill_id))
        )
        shareable = select(
            select(summary.c.skill_id)
            .select_from(joined)
            .where(
                summary.c.owner_type == "user",
                summary.c.owner_id == user.user_id,
                summary.c.owner_version == detail.c.version,
                summary.c.shared_version.is_(None),
                summary.c.is_enabled,
                detail.c.is_enabled,
            )
            .exists()
        )
        async with self.engine.connect() as conn:
            return {
                "categories": list((await conn.execute(categories)).scalars()),
                "has_shareable": await conn.scalar(shareable),
            }

    async def page_admin_versions(self, *, offset: int, limit: int) -> list[dict]:
        detail = t.skill_detail
        return await self._rows(
            _definition_query()
            .order_by(detail.c.skill_id, detail.c.version)
            .offset(offset)
            .limit(limit)
        )

    async def next_publication_revision(
        self, resource_id: str, *, conn: AsyncConnection
    ) -> int:
        summary = t.skill_summary
        current = (
            await conn.execute(
                select(summary.c.owner_version, summary.c.is_enabled).where(
                    summary.c.skill_id == resource_id
                )
            )
        ).one_or_none()
        if current is None:
            return 1
        if not current.is_enabled:
            raise Conflict("此技能已删除，请使用新的技能 ID")
        return current.owner_version + 1

    async def insert_asset(
        self, asset: Asset, *, actor: str, conn: AsyncConnection
    ) -> None:
        """追加新明细并校验 owner 指针，首次发布记录不可变来源。"""
        summary, detail = t.skill_summary, t.skill_detail
        revision = require_skill_publication_revision(asset.publication_revision)
        now = utcnow()
        audit = {"updated_by": actor, "updated_at": now}
        if revision == 1:
            advance = (
                insert(summary)
                .values(
                    skill_id=asset.resource_id,
                    owner_type=asset.owner_type,
                    owner_id=asset.owner_id,
                    source_user_id=actor,
                    owner_version=revision,
                    created_by=actor,
                    created_at=now,
                    **audit,
                )
                .on_conflict_do_nothing(index_elements=[summary.c.skill_id])
            )
        else:
            advance = (
                summary.update()
                .where(
                    summary.c.skill_id == asset.resource_id,
                    summary.c.owner_type == asset.owner_type,
                    summary.c.owner_id == asset.owner_id,
                    summary.c.owner_version == revision - 1,
                    summary.c.is_enabled,
                )
                .values(owner_version=revision, **audit)
            )
        result = await conn.execute(advance)
        if result.rowcount != 1:
            raise Conflict("技能归属或发布版本已发生变化")

        payload = dict(asset.payload)
        fields = {
            field: payload.pop(field)
            for field in _DETAIL_FIELDS
            if payload.get(field) is not None
        }
        await conn.execute(
            detail.insert().values(
                skill_id=asset.resource_id,
                version=revision,
                resource_version=asset.resource_version,
                checksum_sha256=asset.checksum_sha256,
                entrypoint=asset.entrypoint,
                is_enabled=asset.is_enabled,
                payload=payload,
                created_by=actor,
                created_at=now,
                **audit,
                **fields,
            )
        )

    async def set_enabled(
        self,
        resource_id: str,
        version: str,
        enabled: bool,
        *,
        actor: str,
        conn: AsyncConnection,
    ) -> bool:
        """更新指定版本的可用状态，其他版本和指针保持原值。"""
        detail = t.skill_detail
        result = await conn.execute(
            detail.update()
            .where(
                detail.c.skill_id == resource_id, detail.c.resource_version == version
            )
            .values(is_enabled=enabled, updated_by=actor, updated_at=utcnow())
        )
        return bool(result.rowcount)

    async def delete(self, skill_id: str, *, actor: str, conn: AsyncConnection) -> None:
        """删除整项技能并移除选用，保留历史版本及发布文件。"""
        summary = t.skill_summary
        now = utcnow()
        result = await conn.execute(
            summary.update()
            .where(summary.c.skill_id == skill_id, summary.c.is_enabled)
            .values(is_enabled=False, updated_by=actor, updated_at=now)
        )
        if result.rowcount != 1:
            raise NotFound("技能不存在")
        selections = t.agent_personal_resources
        await conn.execute(
            selections.delete().where(
                selections.c.resource_type == "skill",
                selections.c.resource_id == skill_id,
            )
        )
