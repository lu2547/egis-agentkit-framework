"""保存 Agent 和 Team 的当前状态，并按版本读取不可变配置。"""

from sqlalchemy import and_, func, or_, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.client._postgres import PostgresTransaction, transaction_connection
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.storage import tables as t
from egis_agentkit.storage._access import AccessStorage
from egis_agentkit.storage.model import AgentDefinition, TeamDefinitionRecord, utcnow
from egis_agentkit.storage.model._base import publication_label, resource_available
from egis_agentkit.storage.model._definition import build_definition_options


def build_definition_data(row: dict) -> dict:
    """按业务模型返回定义，统一提供资源引用使用的版本字段。"""
    model = (
        AgentDefinition if row["definition_type"] == "agent" else TeamDefinitionRecord
    )
    data = model.model_validate(
        {key: row[key] for key in model.model_fields}
    ).model_dump(mode="json")
    version = data["version"]
    return {
        **data,
        "resource_version": str(version),
        "version_label": publication_label(version),
    }


def _get_definition_table(definition_type):
    """按业务类型选择汇总表。"""
    return {"agent": t.agent_definition_summary, "team": t.team_definition_summary}[
        definition_type
    ]


def _build_definition_record(table, row):
    """为跨类型的资源读取补充统一引用字段。"""
    if table is t.agent_definition_summary:
        return {
            **row,
            "definition_type": "agent",
            "resource_version": str(row["version"]),
            "version_label": publication_label(row["version"]),
        }
    return {
        **row,
        "definition_type": "team",
        "agent_type": None,
        "agent_name": None,
        "agent_display_name": None,
        "resource_version": str(row["version"]),
        "version_label": publication_label(row["version"]),
    }


def _get_version_table(table):
    return (
        t.agent_definition_detail
        if table is t.agent_definition_summary
        else t.team_definition_detail
    )


def _build_definition_query(table, version=None):
    """在数据库内关联汇总记录和指定版本明细。"""
    details = _get_version_table(table)
    agent = table is t.agent_definition_summary
    identity = (
        table.c.id,
        table.c.owner_type,
        table.c.owner_id,
        table.c.created_by,
        table.c.created_at,
        table.c.deleted_at,
        table.c.owner_version,
        table.c.shared_version,
    )
    if agent:
        identity += (table.c.agent_type,)
    selected = table.c.owner_version if version is None else version
    version_column = details.c.version
    kind = "agent" if agent else "team"
    return select(
        *identity,
        details.c[f"{kind}_name"],
        details.c[f"{kind}_display_name"],
        version_column,
        details.c.payload,
        (details.c.created_by if version is not None else table.c.updated_by).label(
            "updated_by"
        ),
        (details.c.created_at if version is not None else table.c.updated_at).label(
            "updated_at"
        ),
    ).select_from(
        table.join(
            details,
            and_(details.c.definition_id == table.c.id, version_column == selected),
        )
    )


def _validate_version_number(version):
    """把公开引用的十进制字符串转换为有效的数据库版本号。"""
    value = str(version)
    if (
        not value.isascii()
        or not value.isdecimal()
        or value.startswith("0")
        or len(value) > 19
        or int(value) > 2**63 - 1
    ):
        raise NotFound("资源版本不存在")
    return int(value)


def definition_view_selection(
    row,
    view,
    *,
    version=None,
    grant_id=None,
    review_id=None,
    grant=None,
    review=None,
    allow_pinned=False,
    dependency=False,
):
    """单条和批量读取共用的定义视图、固定版本和来源检查。"""
    if not resource_available(row):
        raise Forbidden("资源已停用")
    if (view == "system") != (row["owner_type"] == "system"):
        raise Forbidden("资源视图不匹配")
    metadata = {}
    selected = version
    if grant_id is not None:
        if grant is None or (
            grant["resource_id"],
            grant["resource_type"],
            grant["resource_view_type"],
        ) != (row["id"], row["definition_type"], view):
            raise Forbidden("授权已撤销或视图不匹配")
        if review_id is not None and review_id != grant["source_review_id"]:
            raise Forbidden("共享审核与授权不匹配")
        metadata["access_grant_id"] = grant_id
        if grant["source_review_id"] is not None:
            metadata["shared_review_id"] = grant["source_review_id"]
    elif review_id is not None:
        if view != "shared":
            raise Forbidden("资源视图不匹配")
        if review is None or (review["resource_id"], review["resource_type"]) != (
            row["id"],
            row["definition_type"],
        ):
            raise Forbidden("资源尚未通过共享审核")
        metadata["shared_review_id"] = review_id
    if view == "shared" and not dependency:
        shared = row["shared_version"]
        if shared is None:
            raise Forbidden("资源尚未共享")
        if not (allow_pinned and grant_id is not None and version is not None):
            selected = shared
    if view == "system" and grant_id is not None and not allow_pinned:
        selected = row["owner_version"]
    if version is not None and str(version) != str(selected):
        raise Forbidden("资源版本与授权不匹配")
    return selected, metadata


class DefinitionStorage:
    """持久化定义和版本，调用方负责鉴权和管理写事务。"""

    def __init__(self, engine: AsyncEngine | PostgresTransaction):
        """使用连接池或调用方已经打开的事务。"""
        self.engine = engine
        from .agent._definition import AgentDefinitionStorage

        self.agents = AgentDefinitionStorage(engine)

    async def _get_first_record(self, statement, *, conn=None):
        """读取查询返回的第一条记录。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self._get_first_record(statement, conn=connection)
        row = (await conn.execute(statement)).mappings().first()
        return dict(row) if row is not None else None

    async def get_definition(
        self,
        definition_id: str,
        *,
        version: int | str | None = None,
        conn=None,
        definition_type=None,
    ) -> dict:
        """查询当前或指定版本定义，包含当前启停状态。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.get_definition(
                    definition_id,
                    version=version,
                    conn=connection,
                    definition_type=definition_type,
                )
        row = await self._find_definition(
            definition_id,
            version=_validate_version_number(version) if version is not None else None,
            conn=conn,
            definition_type=definition_type,
        )
        if row is None:
            raise NotFound()
        return row

    async def get_many(self, references):
        """按类型和固定版本批量读取，汇总表仍提供当前生命周期状态。"""
        grouped = {kind: set() for kind in ("agent", "team")}
        for kind, identifier, version in references:
            if kind not in grouped:
                continue
            try:
                grouped[kind].add((identifier, _validate_version_number(version)))
            except NotFound:
                continue
        result = {}
        async with self.engine.connect() as conn:
            for kind, identities in grouped.items():
                if not identities:
                    continue
                table = _get_definition_table(kind)
                detail = _get_version_table(table)
                query = _build_definition_query(table, detail.c.version).where(
                    tuple_(table.c.id, detail.c.version).in_(identities)
                )
                for row in (await conn.execute(query)).mappings():
                    value = _build_definition_record(table, row)
                    result[kind, row["id"], str(row["version"])] = value
        return result

    async def get_locked_definition(
        self, definition_id: str, *, conn: AsyncConnection | None = None
    ) -> dict | None:
        """在当前事务锁定定义 ID 与已有主记录。"""
        conn = transaction_connection(self.engine, conn)
        # 分表后同一 ID 仍只能属于一个业务类型，包含记录尚不存在的情况。
        await conn.execute(
            text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
            {"key": f"ak:definition:{definition_id}"},
        )
        return await self._find_definition(definition_id, conn=conn, locked=True)

    async def get_definition_for_update(
        self, definition_id: str, *, conn: AsyncConnection | None = None
    ) -> dict:
        """在当前写事务内锁定已有定义，缺失时终止更新。"""
        row = await self.get_locked_definition(definition_id, conn=conn)
        if row is None:
            raise NotFound()
        return row

    async def _find_definition(
        self, definition_id, *, conn, version=None, locked=False, definition_type=None
    ):
        """跨两张主表查找定义，写入时只锁定主记录。"""
        tables = (
            (_get_definition_table(definition_type),)
            if definition_type
            else (t.agent_definition_summary, t.team_definition_summary)
        )
        for table in tables:
            query = _build_definition_query(table, version).where(
                table.c.id == definition_id
            )
            row = await self._get_first_record(
                query.with_for_update(of=table) if locked else query, conn=conn
            )
            if row is not None:
                return _build_definition_record(table, row)
        return None

    async def get_approved_review(
        self, resource_id: str, *, resource_type: str | None = None, conn=None
    ) -> dict | None:
        """查询资源当前通过的共享审核。"""
        return await AccessStorage(self.engine).get_approved_review(
            resource_id, resource_type=resource_type, conn=conn
        )

    async def get_definition_by_reference(self, reference: dict) -> dict:
        """读取已有会话保留的固定定义。"""
        return await self.get_definition_view(
            reference["definition_id"],
            reference["resource_view_type"],
            **build_definition_options(reference),
        )

    async def get_definition_view(
        self,
        definition_id: str,
        view: str,
        *,
        version: int | str | None = None,
        conn=None,
        review_id=None,
        grant_id=None,
        allow_pinned=False,
        dependency=False,
    ) -> dict:
        """按显式版本、授权或审批读取定义，调用方负责校验使用权。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.get_definition_view(
                    definition_id,
                    view,
                    version=version,
                    review_id=review_id,
                    grant_id=grant_id,
                    allow_pinned=allow_pinned,
                    dependency=dependency,
                    conn=connection,
                )
        row = await self.get_definition(definition_id, conn=conn)
        access = AccessStorage(self.engine)
        grant = (
            await access.active_grant(grant_id, definition_id, conn=conn)
            if grant_id is not None
            else None
        )
        review = (
            await access.get_approved_review(
                definition_id,
                resource_type=row["definition_type"],
                review_id=review_id,
                conn=conn,
            )
            if review_id is not None and grant_id is None
            else None
        )
        selected, metadata = definition_view_selection(
            row,
            view,
            version=version,
            grant_id=grant_id,
            review_id=review_id,
            grant=grant,
            review=review,
            allow_pinned=allow_pinned,
            dependency=dependency,
        )
        if selected is not None and str(row["resource_version"]) != str(selected):
            row = await self.get_definition(definition_id, version=selected, conn=conn)
        if not resource_available(row):
            raise Forbidden("资源已停用")
        return {**build_definition_data(row), "resource_view_type": view, **metadata}

    async def list_versions(
        self, definition_id: str, *, conn=None, definition_type=None
    ) -> list[str]:
        """按版本从新到旧列出定义的版本号，调用方决定可见范围。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.list_versions(
                    definition_id, conn=connection, definition_type=definition_type
                )
        if definition_type is None:
            definition_type = (await self.get_definition(definition_id, conn=conn))[
                "definition_type"
            ]
        versions = _get_version_table(_get_definition_table(definition_type))
        number = versions.c.version
        rows = await conn.execute(
            select(number)
            .where(versions.c.definition_id == definition_id)
            .order_by(number.desc())
        )
        return [str(version) for version in rows.scalars()]

    async def list_grantable_versions(self, kind, *, excluded_agent_types):
        """读取每项可独立授权的当前共享版本或所有者版本。"""
        identity = _get_definition_table(kind)
        owner = identity.c.owner_version
        selected = func.coalesce(identity.c.shared_version, owner)
        query = _build_definition_query(identity, selected).where(
            identity.c.deleted_at.is_(None), identity.c.owner_type == "system"
        )
        if kind == "agent":
            query = query.where(
                identity.c.agent_type.not_in(excluded_agent_types),
            )
        async with self.engine.connect() as connection:
            rows = (
                await connection.execute(query.order_by(identity.c.created_at.desc()))
            ).mappings()
            return [_build_definition_record(identity, row) for row in rows]

    async def list_definitions_and_reviews(
        self, definition_type="agent", *, conn=None, user=None
    ) -> tuple[list[dict], dict[tuple[str, str], str]]:
        """读取当前定义及目录候选的各版本审核状态。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.list_definitions_and_reviews(
                    definition_type, conn=connection, user=user
                )
        table = _get_definition_table(definition_type)
        query = _build_definition_query(table)
        if user is not None:
            query = query.where(table.c.deleted_at.is_(None))
            if user.role_type != "admin":
                grants = t.resource_permission
                groups = select(t.groups.c.group_id).where(
                    t.groups.c.group_id.in_(user.group_ids), t.groups.c.is_enabled
                )
                owned = select(table.c.id).where(
                    table.c.owner_type == "user", table.c.owner_id == user.user_id
                )
                granted = select(grants.c.resource_id.label("id")).where(
                    grants.c.resource_type == definition_type,
                    grants.c.revoked_at.is_(None),
                    grants.c.root_grant_id.is_(None),
                    grants.c.grant_source_type.in_(("manual", "share")),
                    or_(
                        and_(
                            grants.c.principal_type == "user",
                            grants.c.principal_id == user.user_id,
                        ),
                        and_(
                            grants.c.principal_type == "group",
                            grants.c.principal_id.in_(groups),
                        ),
                    ),
                )
                eligible = owned.union(granted).cte("visible_definition_ids")
                query = query.join(eligible, eligible.c.id == table.c.id)
        rows = [
            _build_definition_record(table, row)
            for row in (
                await conn.execute(query.order_by(table.c.created_at.desc()))
            ).mappings()
        ]
        states = await self._get_review_states(definition_type, rows, conn=conn)
        return rows, states

    async def _get_review_states(self, kind, records, *, conn):
        """每个资源版本只取最近一条共享审核状态。"""
        if not records:
            return {}
        reviews = t.approvals
        recent = (
            select(
                reviews.c.resource_id,
                reviews.c.resource_version,
                reviews.c.review_status_type,
                func.row_number()
                .over(
                    partition_by=(reviews.c.resource_id, reviews.c.resource_version),
                    order_by=(
                        reviews.c.submitted_at.desc(),
                        reviews.c.review_id.desc(),
                    ),
                )
                .label("position"),
            )
            .where(
                reviews.c.resource_type == kind,
                reviews.c.resource_id.in_([row["id"] for row in records]),
            )
            .subquery()
        )
        rows = await conn.execute(
            select(
                recent.c.resource_id,
                recent.c.resource_version,
                recent.c.review_status_type,
            ).where(recent.c.position == 1)
        )
        return {(identifier, version): status for identifier, version, status in rows}

    async def get_or_add_personal(self, values, *, actor, conn=None):
        return await self.agents.get_or_add_personal(values, actor=actor, conn=conn)

    async def add_definition(
        self, values: dict, *, actor: str, conn: AsyncConnection | None = None
    ) -> dict:
        """插入首版定义，在同一事务内保证 ID 全局唯一。"""
        if values["definition_type"] == "agent":
            return await self.agents.add_definition(values, actor=actor, conn=conn)
        conn = transaction_connection(self.engine, conn)
        if await self.get_locked_definition(values["id"], conn=conn) is not None:
            raise Conflict("定义已存在")
        return await self._add_first_version(values, actor=actor, conn=conn)

    async def _add_first_version(self, values, *, actor, conn) -> dict:
        """持有定义锁时，在同一事务中写入主记录和首版。"""
        if values["owner_version"] != 1:
            raise Conflict("新资源的 owner_version 必须为 1")
        table = _get_definition_table(values["definition_type"])
        fields = {key: value for key, value in values.items() if key in table.c}
        if table is t.team_definition_summary:
            fields["orchestrator_id"] = values["payload"]["orchestrator"][
                "definition_id"
            ]
        now = utcnow()
        await conn.execute(
            insert(table).values(
                **fields,
                created_by=actor,
                updated_by=actor,
                created_at=now,
                updated_at=now,
            )
        )
        await self._add_version(table, values, 1, actor=actor, now=now, conn=conn)
        return await self.get_definition(values["id"], conn=conn)

    async def _add_version(self, table, values, version, *, actor, now, conn):
        """只追加版本记录，已有版本没有更新入口。"""
        versions = _get_version_table(table)
        fields = {
            key: values[key]
            for key in (
                "agent_name",
                "agent_display_name",
                "team_name",
                "team_display_name",
                "payload",
            )
            if key in versions.c
        }
        await conn.execute(
            insert(versions).values(
                definition_id=values["id"],
                version=version,
                **fields,
                created_by=actor,
                created_at=now,
            )
        )

    async def update_definition(
        self,
        definition_id: str,
        values: dict,
        *,
        actor: str,
        conn: AsyncConnection | None = None,
    ) -> dict:
        """匹配当前版本后推进指针，并追加下一版定义配置。"""
        if values["definition_type"] == "agent":
            return await self.agents.update_definition(
                definition_id, values, actor=actor, conn=conn
            )
        conn = transaction_connection(self.engine, conn)
        expected = values["owner_version"]
        kind = values["definition_type"]
        table = _get_definition_table(kind)
        name = f"{kind}_name"
        identity = {"owner_type": values["owner_type"], "owner_id": values["owner_id"]}
        identity["orchestrator_id"] = values["payload"]["orchestrator"]["definition_id"]
        now = utcnow()
        result = await conn.execute(
            update(table)
            .where(
                table.c.id == definition_id,
                table.c.id == values["id"],
                table.c.owner_version == expected,
                table.c.deleted_at.is_(None),
                *(table.c[key] == value for key, value in identity.items()),
            )
            .values(
                **{name: values[name]},
                owner_version=expected + 1,
                updated_by=actor,
                updated_at=now,
            )
        )
        if result.rowcount != 1:
            raise Conflict()
        await self._add_version(
            table, values, expected + 1, actor=actor, now=now, conn=conn
        )
        return await self.get_definition(definition_id, conn=conn)

    async def delete_definition(
        self, definition_id: str, *, actor: str, conn: AsyncConnection | None = None
    ) -> None:
        """标记定义删除并保留所有已发布配置。"""
        conn = transaction_connection(self.engine, conn)
        record = await self.get_locked_definition(definition_id, conn=conn)
        if record is None:
            raise NotFound()
        if record["definition_type"] == "agent":
            return await self.agents.delete_definition(
                definition_id, actor=actor, conn=conn
            )
        table = _get_definition_table(record["definition_type"])
        await conn.execute(
            update(table)
            .where(table.c.id == definition_id, table.c.deleted_at.is_(None))
            .values(deleted_at=utcnow(), updated_by=actor, updated_at=utcnow())
        )
