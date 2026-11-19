"""持久化 Agent 定义，并在数据库内完成权限和版本分页。

核心能力
- 汇总记录维护稳定身份、owner/shared 版本及删除状态。
- 配置明细只追加，使用 owner_version 检查并发修改。
- 使用有效直接授权的 EXISTS 条件过滤列表，再关联可见版本并分页。
"""

from __future__ import annotations

from datetime import datetime
from typing import Any, Sequence

from sqlalchemy import (
    String,
    and_,
    case,
    cast,
    exists,
    func,
    literal,
    or_,
    select,
    text,
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.client._postgres import PostgresTransaction, transaction_connection
from egis_agentkit.client.nacos._config import LivePolicies
from egis_agentkit.client.nacos._settings import AccessConfig
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import AgentType, User, utcnow
from egis_agentkit.storage.model._base import publication_label
from egis_agentkit.storage.model.agent._definition_detail import AgentDefinitionDetail


class AgentDefinitionStorage:
    """Agent 的唯一写入实现，同时提供受权限约束的列表查询。"""

    def __init__(self, engine: AsyncEngine | PostgresTransaction) -> None:
        self.engine = engine

    async def get_definition(
        self,
        definition_id: str,
        *,
        version: int | str | None = None,
        conn: AsyncConnection | None = None,
    ) -> dict[str, Any]:
        """读取所有者最新版或指定配置版本，保留汇总层的生命周期状态。"""
        from .._definition import (
            _build_definition_query,
            _build_definition_record,
            _validate_version_number,
        )

        if conn is None:
            async with self.engine.connect() as connection:
                return await self.get_definition(
                    definition_id, version=version, conn=connection
                )
        selected = _validate_version_number(version) if version is not None else None
        row = (
            (
                await conn.execute(
                    _build_definition_query(t.agent_definition_summary, selected).where(
                        t.agent_definition_summary.c.id == definition_id
                    )
                )
            )
            .mappings()
            .first()
        )
        if row is None:
            raise NotFound()
        return _build_definition_record(t.agent_definition_summary, row)

    async def get_locked_definition(
        self, definition_id: str, *, conn: AsyncConnection | None = None
    ) -> dict[str, Any] | None:
        """锁定定义身份，保证并发首次创建及跨 Agent/Team 的 ID 唯一。"""
        conn = transaction_connection(self.engine, conn)
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"ak:definition:{definition_id}"},
        )
        await conn.execute(
            select(t.agent_definition_summary.c.id)
            .where(t.agent_definition_summary.c.id == definition_id)
            .with_for_update()
        )
        try:
            return await self.get_definition(definition_id, conn=conn)
        except NotFound:
            return None

    async def get_definition_for_update(
        self, definition_id: str, *, conn: AsyncConnection | None = None
    ) -> dict[str, Any]:
        """取得可继续编辑的锁定定义，删除记录不可重新保存。"""
        row = await self.get_locked_definition(definition_id, conn=conn)
        if row is None:
            raise NotFound()
        if row["deleted_at"] is not None:
            raise Forbidden("Agent 已删除")
        return row

    async def get_or_add_personal(
        self,
        definition_values: dict[str, Any],
        *,
        actor: str,
        conn: AsyncConnection | None = None,
    ) -> dict[str, Any]:
        """每个用户原子创建或返回同一份个人助理。"""
        if conn is None and isinstance(self.engine, AsyncEngine):
            async with self.engine.begin() as connection:
                return await self.get_or_add_personal(
                    definition_values, actor=actor, conn=connection
                )
        conn = transaction_connection(self.engine, conn)
        previous = await self.get_locked_definition(definition_values["id"], conn=conn)
        if previous is not None:
            if (
                previous["agent_type"] != "personal"
                or previous["owner_id"] != definition_values["owner_id"]
            ):
                raise Conflict("个人助理 ID 已被占用")
            return previous
        return await self.add_definition(definition_values, actor=actor, conn=conn)

    async def add_definition(
        self,
        definition_values: dict[str, Any],
        *,
        actor: str,
        conn: AsyncConnection | None = None,
    ) -> dict[str, Any]:
        """在当前存储事务插入汇总和首版明细。"""
        conn = transaction_connection(self.engine, conn)
        if definition_values["owner_version"] != 1:
            raise Conflict("新 Agent 的 owner_version 必须为 1")
        previous = await self.get_locked_definition(definition_values["id"], conn=conn)
        team_exists = await conn.scalar(
            select(
                exists().where(
                    t.team_definition_summary.c.id == definition_values["id"]
                )
            )
        )
        if previous is not None or team_exists:
            raise Conflict("定义已存在")
        now = utcnow()
        await conn.execute(
            insert(t.agent_definition_summary).values(
                **{
                    key: definition_values[key]
                    for key in (
                        "id",
                        "owner_type",
                        "owner_id",
                        "agent_type",
                        "agent_name",
                    )
                },
                owner_version=1,
                created_by=actor,
                updated_by=actor,
                created_at=now,
                updated_at=now,
            )
        )
        await self._add_definition_detail(conn, definition_values, 1, actor, now)
        return await self.get_definition(definition_values["id"], conn=conn)

    async def _add_definition_detail(
        self,
        connection: AsyncConnection,
        definition_values: dict[str, Any],
        version: int,
        actor: str,
        now: datetime,
    ) -> None:
        """只追加明细，已有版本不提供更新入口。"""
        detail = AgentDefinitionDetail(
            definition_id=definition_values["id"],
            version=version,
            agent_name=definition_values["agent_name"],
            agent_display_name=definition_values["agent_display_name"],
            payload=definition_values["payload"],
        )
        await connection.execute(
            insert(t.agent_definition_detail).values(
                **detail.model_dump(mode="json"),
                created_by=actor,
                created_at=now,
            )
        )

    async def update_definition(
        self,
        definition_id: str,
        definition_values: dict[str, Any],
        *,
        actor: str,
        conn: AsyncConnection | None = None,
    ) -> dict[str, Any]:
        """比较 owner 版本并追加下一版，阻止身份变更和覆盖并发修改。"""
        conn = transaction_connection(self.engine, conn)
        expected = definition_values["owner_version"]
        now = utcnow()
        summary = t.agent_definition_summary
        result = await conn.execute(
            update(summary)
            .where(
                summary.c.id == definition_id,
                summary.c.id == definition_values["id"],
                summary.c.owner_version == expected,
                summary.c.deleted_at.is_(None),
                *(
                    summary.c[key] == definition_values[key]
                    for key in ("owner_type", "owner_id", "agent_type")
                ),
            )
            .values(
                agent_name=definition_values["agent_name"],
                owner_version=expected + 1,
                updated_by=actor,
                updated_at=now,
            )
        )
        if result.rowcount != 1:
            raise Conflict()
        await self._add_definition_detail(
            conn, definition_values, expected + 1, actor, now
        )
        return await self.get_definition(definition_id, conn=conn)

    async def delete_definition(
        self, definition_id: str, *, actor: str, conn: AsyncConnection | None = None
    ) -> None:
        """逻辑删除汇总，保留可供历史引用的版本明细。"""
        conn = transaction_connection(self.engine, conn)
        now = utcnow()
        result = await conn.execute(
            update(t.agent_definition_summary)
            .where(
                t.agent_definition_summary.c.id == definition_id,
                t.agent_definition_summary.c.deleted_at.is_(None),
            )
            .values(deleted_at=now, updated_by=actor, updated_at=now)
        )
        if result.rowcount != 1:
            raise NotFound()

    async def list_versions(self, definition_id: str) -> list[str]:
        """读取一个 Agent 已发布的明细版本。"""
        async with self.engine.connect() as connection:
            return [
                str(value)
                for value in (
                    await connection.execute(
                        select(t.agent_definition_detail.c.version)
                        .where(
                            t.agent_definition_detail.c.definition_id == definition_id
                        )
                        .order_by(t.agent_definition_detail.c.version.desc())
                    )
                ).scalars()
            ]

    @staticmethod
    def _select_visible_definitions(
        user: User,
        types: Sequence[AgentType],
        *,
        editable_only: bool = False,
        owner_only: bool = False,
    ):
        """为目录卡片、分类和管理页选出可见版本。"""
        summary, detail, access = (
            t.agent_definition_summary,
            t.agent_definition_detail,
            t.resource_permission,
        )
        own = and_(summary.c.owner_type == "user", summary.c.owner_id == user.user_id)
        system = summary.c.owner_type == "system"
        view = case((system, "system"), (own, "private"), else_="shared")
        selected = case(
            (or_(own, system), summary.c.owner_version), else_=summary.c.shared_version
        )
        enabled_groups = select(t.groups.c.group_id).where(
            t.groups.c.group_id.in_(user.group_ids), t.groups.c.is_enabled
        )
        direct = exists(
            select(literal(1)).where(
                access.c.resource_type == "agent",
                access.c.resource_id == summary.c.id,
                access.c.resource_view_type == view,
                access.c.revoked_at.is_(None),
                access.c.root_grant_id.is_(None),
                access.c.grant_source_type.in_(("manual", "share")),
                or_(
                    and_(
                        access.c.principal_type == "user",
                        access.c.principal_id == user.user_id,
                    ),
                    and_(
                        access.c.principal_type == "group",
                        access.c.principal_id.in_(enabled_groups),
                    ),
                ),
            )
        )
        pending = exists(
            select(literal(1)).where(
                t.approvals.c.resource_type == "agent",
                t.approvals.c.resource_id == summary.c.id,
                t.approvals.c.review_type == "update",
                t.approvals.c.review_status_type == "pending",
            )
        )
        latest_review = (
            select(t.approvals.c.review_status_type)
            .where(
                t.approvals.c.resource_type == "agent",
                t.approvals.c.resource_id == summary.c.id,
                t.approvals.c.resource_version == cast(selected, String),
            )
            .order_by(t.approvals.c.submitted_at.desc(), t.approvals.c.review_id.desc())
            .limit(1)
            .scalar_subquery()
        )
        fields = ("description", "category", "tags", "avatar", "starter_prompts", "model_key")
        card = [value for key in fields for value in (key, detail.c.payload[key])]
        if editable_only:
            card.extend((
                "remote_config",
                func.jsonb_build_object(
                    "transport", detail.c.payload["remote_config"]["transport"]
                ),
            ))
        visible = own if owner_only else or_(own, system if editable_only else direct)
        return (
            select(
                summary.c.id,
                summary.c.owner_type,
                summary.c.owner_id,
                summary.c.agent_type,
                summary.c.owner_version,
                summary.c.shared_version,
                summary.c.deleted_at,
                summary.c.created_at,
                summary.c.updated_at,
                detail.c.version,
                detail.c.agent_name,
                detail.c.agent_display_name,
                func.jsonb_build_object(*card).label("payload"),
                view.label("resource_view_type"),
                own.label("is_owner"),
                direct.label("has_access"),
                pending.label("has_pending_update"),
                latest_review.label("review_status"),
            )
            .select_from(
                summary.join(
                    detail,
                    and_(
                        detail.c.definition_id == summary.c.id,
                        detail.c.version == selected,
                    ),
                )
            )
            .where(
                summary.c.deleted_at.is_(None),
                summary.c.agent_type.in_(types),
                visible,
            )
        )

    async def list_visible_categories(
        self, user: User, types: Sequence[AgentType]
    ) -> list[str]:
        """分类只来自该用户可见的定义版本。"""
        category = t.agent_definition_detail.c.payload["category"].astext
        statement = (
            self._select_visible_definitions(user, types)
            .with_only_columns(category)
            .where(category != "")
            .distinct()
            .order_by(category)
        )
        async with self.engine.connect() as connection:
            return list((await connection.scalars(statement)).all())

    async def page_visible_definitions(
        self,
        user: User,
        types: Sequence[AgentType],
        *,
        after: tuple[datetime, str] | str | None,
        limit: int,
        query: str = "",
        category: str = "",
        owner_only: bool = False,
        policies: AccessConfig | LivePolicies | None = None,
        order_by_id: bool = False,
        offset: int = 0,
        editable_only: bool = False,
    ) -> list[dict[str, Any]]:
        """先过滤权限、分类和搜索结果，再返回一页卡片。"""
        summary, detail = t.agent_definition_summary, t.agent_definition_detail
        statement = self._select_visible_definitions(
            user, types, editable_only=editable_only, owner_only=owner_only
        )
        if category:
            statement = statement.where(detail.c.payload["category"].astext == category)
        if after is not None:
            statement = (
                statement.where(summary.c.id > after)
                if order_by_id
                else statement.where(
                    tuple_(summary.c.created_at, summary.c.id) < tuple_(*after)
                )
            )
        if query:
            term = (
                "%"
                + query.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
                + "%"
            )
            statement = statement.where(
                or_(
                    detail.c.agent_name.ilike(term, escape="\\"),
                    detail.c.agent_display_name.ilike(term, escape="\\"),
                    detail.c.payload["description"].astext.ilike(term, escape="\\"),
                    detail.c.payload["tags"].astext.ilike(term, escape="\\"),
                )
            )
        statement = (
            statement.order_by(summary.c.id)
            if order_by_id
            else statement.order_by(summary.c.created_at.desc(), summary.c.id.desc())
        )
        statement = statement.offset(offset).limit(limit)
        async with self.engine.connect() as connection:
            rows = (await connection.execute(statement)).mappings().all()
        result = []
        for row in rows:
            value = dict(row)
            own = value.pop("is_owner")
            granted = value.pop("has_access")
            pending = value.pop("has_pending_update")
            review = value.pop("review_status")
            maintainable = own or (
                user.role_type == "admin" and row["resource_view_type"] == "system"
            )
            editable = maintainable and (
                row["agent_type"] != "remote" or user.role_type == "admin"
            )
            personal = row["agent_type"] == "personal"
            can_use_own = getattr(
                policies,
                "allow_personal_self_use" if personal else "allow_private_self_use",
                True,
            )
            result.append(
                {
                    **value,
                    "definition_type": "agent",
                    "resource_version": str(row["version"]),
                    "version_label": publication_label(row["version"]),
                    "is_editable": editable,
                    "is_deletable": editable,
                    "is_usable": granted or (own and can_use_own),
                    "has_shared_version": row["shared_version"] is not None,
                    "can_share": own and row["shared_version"] is None and not personal,
                    "can_share_update": own
                    and row["shared_version"] is not None
                    and row["owner_version"] != row["shared_version"]
                    and not pending
                    and not personal,
                    "share_status_type": "approved"
                    if row["resource_view_type"] == "shared"
                    else review or "private",
                }
            )
        return result

    async def current_bindings(self, references):
        """按一页资源的引用筛选当前 Agent 版本，只读取摘要与绑定。"""
        if not references:
            return []
        summary, detail = t.agent_definition_summary, t.agent_definition_detail
        statement = (
            select(summary.c.id, detail.c.agent_display_name, detail.c.payload["bindings"].label("bindings"))
            .select_from(
                summary.join(
                    detail,
                    and_(
                        summary.c.id == detail.c.definition_id,
                        summary.c.owner_version == detail.c.version,
                    ),
                )
            )
            .where(
                summary.c.deleted_at.is_(None),
                or_(*(detail.c.payload.contains({"bindings": [ref]}) for ref in references)),
            )
            .order_by(summary.c.created_at.desc(), summary.c.id)
        )
        async with self.engine.connect() as conn:
            return [dict(row) for row in (await conn.execute(statement)).mappings()]
