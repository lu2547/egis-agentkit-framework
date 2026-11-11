"""保存 Agent 和 Team 的当前状态，并按版本读取不可变配置。"""

from sqlalchemy import and_, func, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.backend._postgres import PostgresTransaction, transaction_connection
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.storage import tables as t
from egis_agentkit.storage._access import AccessStorage
from egis_agentkit.storage.model import Definition, utcnow
from egis_agentkit.storage.model._base import publication_label
from egis_agentkit.storage.model._definition import definition_options


def definition_data(row: dict) -> dict:
    """将存储记录转换为定义视图，并附上供引用使用的版本。"""
    data = Definition.model_validate(
        {k: row[k] for k in Definition.model_fields}
    ).model_dump(mode="json")
    return {
        **data,
        "resource_version": str(data["config_version"]),
        "version_label": publication_label(data["config_version"]),
    }


def _definition_table(definition_type):
    """按业务类型选择主表。"""
    if definition_type == "agent":
        return t.agent_definitions
    if definition_type == "team":
        return t.team_definitions
    raise ValueError(f"Unknown definition type: {definition_type}")


def _definition_record(table, row):
    """补齐 Agent 和 Team 共用的字段。"""
    values = {
        **row,
        "resource_version": str(row["config_version"]),
        "version_label": publication_label(row["config_version"]),
    }
    if table is t.agent_definitions:
        return dict(
            values, definition_type="agent", team_name=None, team_display_name=None
        )
    return dict(
        values,
        definition_type="team",
        agent_type=None,
        agent_name=None,
        agent_display_name=None,
    )


def _version_table(table):
    """找到 Agent 或 Team 主表对应的不可变版本表。"""
    return (
        t.agent_definition_versions
        if table is t.agent_definitions
        else t.team_definition_versions
    )


def _select_definition(table, version=None):
    """从主表读取归属和状态，从版本表读取指定配置。"""
    versions = _version_table(table)
    kind = "agent" if table is t.agent_definitions else "team"
    identity = (
        table.c.id,
        table.c.owner_type,
        table.c.owner_id,
        table.c.is_enabled,
        table.c.created_by,
        table.c.created_at,
    )
    if kind == "agent":
        identity += (table.c.agent_type,)
    position = table.c.config_version if version is None else version
    updated_by, updated_at = table.c.updated_by, table.c.updated_at
    if version is not None:
        updated_by = versions.c.created_by.label("updated_by")
        updated_at = versions.c.created_at.label("updated_at")
    return select(
        *identity,
        versions.c[f"{kind}_name"],
        versions.c[f"{kind}_display_name"],
        versions.c.config_version,
        versions.c.payload,
        updated_by,
        updated_at,
    ).select_from(
        table.join(
            versions,
            and_(
                versions.c.definition_id == table.c.id,
                versions.c.config_version == position,
            ),
        )
    )


def _version_number(version):
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


class DefinitionStorage:
    """持久化定义和版本，调用方负责鉴权和管理写事务。"""

    def __init__(self, engine: AsyncEngine | PostgresTransaction):
        """使用连接池或调用方已经打开的事务。"""
        self.engine = engine

    async def _first(self, statement, *, conn=None):
        """读取查询返回的第一条记录。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self._first(statement, conn=connection)
        row = (await conn.execute(statement)).mappings().first()
        return dict(row) if row is not None else None

    async def get_definition(
        self, definition_id: str, *, version: int | str | None = None, conn=None
    ) -> dict:
        """查询当前或指定版本定义，包含当前启停状态。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.get_definition(
                    definition_id, version=version, conn=connection
                )
        row = await self._find_definition(
            definition_id,
            version=_version_number(version) if version is not None else None,
            conn=conn,
        )
        if row is None:
            raise NotFound()
        return row

    async def locked_definition(
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
        row = await self.locked_definition(definition_id, conn=conn)
        if row is None:
            raise NotFound()
        return row

    async def _find_definition(
        self, definition_id, *, conn, version=None, locked=False
    ):
        """跨两张主表查找定义，写入时只锁定主记录。"""
        for table in (t.agent_definitions, t.team_definitions):
            query = _select_definition(table, version).where(
                table.c.id == definition_id
            )
            row = await self._first(
                query.with_for_update(of=table) if locked else query, conn=conn
            )
            if row is not None:
                return _definition_record(table, row)
        return None

    async def approved_review(
        self, resource_id: str, *, resource_type: str | None = None, conn=None
    ) -> dict | None:
        """查询资源当前通过的共享审核。"""
        return await AccessStorage(self.engine).approved_review(
            resource_id, resource_type=resource_type, conn=conn
        )

    async def read_reference(self, reference: dict) -> dict:
        """读取已有会话保留的固定定义。"""
        return await self.read_view(
            reference["definition_id"],
            reference["resource_view_type"],
            **definition_options(reference),
        )

    async def read_view(
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
                return await self.read_view(
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
        if not row["is_enabled"]:
            raise Forbidden("资源已停用")
        if (view == "system") != (row["owner_type"] == "system"):
            raise Forbidden("资源视图不匹配")
        # 普通共享入口解析唯一当前版本，已持久化引用可读取原版本。
        access = AccessStorage(self.engine)
        metadata = {}
        selected = version
        if grant_id is not None:
            grant = await access.active_grant(grant_id, definition_id, conn=conn)
            if grant is None or (
                grant["resource_type"],
                grant["resource_view_type"],
            ) != (row["definition_type"], view):
                raise Forbidden("授权已撤销或视图不匹配")
            metadata["access_grant_id"] = grant_id
            source_review_id = grant["source_review_id"]
            if review_id is not None and review_id != source_review_id:
                raise Forbidden("共享审核与授权不匹配")
            if source_review_id is not None:
                metadata["shared_review_id"] = source_review_id
        elif review_id is not None:
            if view != "shared":
                raise Forbidden("资源视图不匹配")
            review = await access.approved_review(
                definition_id,
                resource_type=row["definition_type"],
                review_id=review_id,
                conn=conn,
            )
            if review is None:
                raise Forbidden("资源尚未通过共享审核")
            metadata["shared_review_id"] = review["review_id"]
        if view == "shared" and not dependency:
            shared = await access.shared_version(row["definition_type"], definition_id, conn=conn)
            if shared is None:
                raise Forbidden("资源尚未共享")
            if not (allow_pinned and grant_id is not None and version is not None):
                selected = shared
        if view == "system" and grant_id is not None and not allow_pinned:
            selected = row["config_version"]
        if version is not None and str(version) != str(selected):
            raise Forbidden("资源版本与授权不匹配")
        if selected is not None and str(row["config_version"]) != str(selected):
            row = await self.get_definition(definition_id, version=selected, conn=conn)
        if not row["is_enabled"]:
            raise Forbidden("资源已停用")
        return {**definition_data(row), "resource_view_type": view, **metadata}

    async def list_versions(self, definition_id: str, *, conn=None) -> list[str]:
        """按版本从新到旧列出定义的版本号，调用方决定可见范围。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.list_versions(definition_id, conn=connection)
        definition = await self.get_definition(definition_id, conn=conn)
        versions = _version_table(_definition_table(definition["definition_type"]))
        rows = await conn.execute(
            select(versions.c.config_version)
            .where(versions.c.definition_id == definition_id)
            .order_by(versions.c.config_version.desc())
        )
        return [str(version) for version in rows.scalars()]

    async def grantable_versions(self, kind, *, excluded_agent_types):
        """一次读取每项可独立授权的已启用定义。"""
        identity = _definition_table(kind)
        versions = _version_table(identity)
        statement = (
            select(
                identity.c.id,
                identity.c.owner_type,
                identity.c.owner_id,
                versions.c.config_version,
                versions.c[f"{kind}_display_name"],
            )
            .join(versions, and_(
                versions.c.definition_id == identity.c.id,
                versions.c.config_version == func.coalesce(
                    identity.c.shared_version, identity.c.config_version
                ),
            ))
            .where(identity.c.is_enabled)
            .order_by(identity.c.created_at.desc(), versions.c.config_version.desc())
        )
        if kind == "agent":
            statement = statement.where(
                identity.c.agent_type.not_in(excluded_agent_types)
            )
        async with self.engine.connect() as conn:
            rows = await conn.execute(statement)
            return [
                {**row, "resource_version": str(row["config_version"])}
                for row in rows.mappings()
            ]

    async def read_catalog(
        self, definition_type="agent", *, conn=None
    ) -> tuple[list[dict], dict[tuple[str, str], str]]:
        """读取当前定义及目录候选的各版本审核状态。"""
        if conn is None:
            async with self.engine.connect() as connection:
                return await self.read_catalog(definition_type, conn=connection)
        table = _definition_table(definition_type)
        rows = [
            _definition_record(table, row)
            for row in (
                await conn.execute(
                    _select_definition(table).order_by(table.c.created_at.desc())
                )
            ).mappings()
        ]
        states = await self._review_states(definition_type, rows, conn=conn)
        return rows, states

    async def _review_states(self, kind, records, *, conn):
        """每个资源版本只取最近一条共享审核状态。"""
        if not records:
            return {}
        reviews = t.reviews
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

    async def page_agents(self, types, *, after, limit):
        """按创建时间和 ID 读取一批候选定义及各版本的审核状态。"""
        table = t.agent_definitions
        position = tuple_(table.c.created_at, table.c.id)
        query = _select_definition(table).where(
            table.c.is_enabled, table.c.agent_type.in_(types)
        )
        if after is not None:
            query = query.where(position < tuple_(*after))
        async with self.engine.connect() as conn:
            records = (
                (
                    await conn.execute(
                        query.order_by(
                            table.c.created_at.desc(), table.c.id.desc()
                        ).limit(limit)
                    )
                )
                .mappings()
                .all()
            )
            states = await self._review_states("agent", records, conn=conn)
            return [_definition_record(table, row) for row in records], states

    async def list_enabled_experts_after(self, after: str, *, limit: int) -> list[dict]:
        """按 ID 扫描已启用专家的当前定义。"""
        table = t.agent_definitions
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                _select_definition(table)
                .where(
                    table.c.agent_type.in_(("expert", "remote")),
                    table.c.is_enabled,
                    table.c.id > after,
                )
                .order_by(table.c.id)
                .limit(limit)
            )
            return [_definition_record(table, row) for row in rows.mappings()]

    async def ensure_personal(
        self, values: dict, *, actor: str, conn: AsyncConnection | None = None
    ):
        """创建或读取个人助理，只锁当前定义。"""
        if conn is None and isinstance(self.engine, AsyncEngine):
            async with self.engine.begin() as connection:
                return await self.ensure_personal(values, actor=actor, conn=connection)
        conn = transaction_connection(self.engine, conn)
        previous = await self.locked_definition(values["id"], conn=conn)
        if previous is not None and (
            previous["agent_type"] != "personal"
            or previous["owner_id"] != values["owner_id"]
        ):
            raise Conflict("个人助理 ID 已被占用")
        if previous is not None:
            return previous
        return await self._insert_first_version(values, actor=actor, conn=conn)

    async def insert_definition(
        self, values: dict, *, actor: str, conn: AsyncConnection | None = None
    ) -> dict:
        """插入首版定义，在同一事务内保证 ID 全局唯一。"""
        conn = transaction_connection(self.engine, conn)
        if await self.locked_definition(values["id"], conn=conn) is not None:
            raise Conflict("定义已存在")
        return await self._insert_first_version(values, actor=actor, conn=conn)

    async def _insert_first_version(self, values, *, actor, conn) -> dict:
        """持有定义锁时，在同一事务中写入主记录和首版。"""
        if values["config_version"] != 1:
            raise Conflict("新资源的 config_version 必须为 1")
        table = _definition_table(values["definition_type"])
        fields = {key: value for key, value in values.items() if key in table.c}
        if table is t.team_definitions:
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
        await self._insert_version(table, values, 1, actor=actor, now=now, conn=conn)
        return await self.get_definition(values["id"], conn=conn)

    async def _insert_version(self, table, values, version, *, actor, now, conn):
        """只追加版本记录，已有版本没有更新入口。"""
        versions = _version_table(table)
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
                config_version=version,
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
        conn = transaction_connection(self.engine, conn)
        expected = values["config_version"]
        kind = values["definition_type"]
        table = _definition_table(kind)
        name = f"{kind}_name"
        identity = {"owner_type": values["owner_type"], "owner_id": values["owner_id"]}
        if kind == "agent":
            identity["agent_type"] = values["agent_type"]
        else:
            identity["orchestrator_id"] = values["payload"]["orchestrator"][
                "definition_id"
            ]
        now = utcnow()
        result = await conn.execute(
            update(table)
            .where(
                table.c.id == definition_id,
                table.c.id == values["id"],
                table.c.config_version == expected,
                *(table.c[key] == value for key, value in identity.items()),
            )
            .values(
                **{name: values[name]},
                is_enabled=values["is_enabled"],
                config_version=expected + 1,
                updated_by=actor,
                updated_at=now,
            )
        )
        if result.rowcount != 1:
            raise Conflict()
        await self._insert_version(
            table, values, expected + 1, actor=actor, now=now, conn=conn
        )
        return await self.get_definition(definition_id, conn=conn)

    async def disable_definition(
        self, definition_id: str, *, actor: str, conn: AsyncConnection | None = None
    ) -> None:
        """停用主记录并保留所有已发布配置。"""
        conn = transaction_connection(self.engine, conn)
        record = await self.locked_definition(definition_id, conn=conn)
        if record is None:
            raise NotFound()
        table = _definition_table(record["definition_type"])
        await conn.execute(
            update(table)
            .where(table.c.id == definition_id)
            .values(is_enabled=False, updated_by=actor, updated_at=utcnow())
        )
