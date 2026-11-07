from hashlib import sha256

from sqlalchemy import select
from sqlalchemy.exc import IntegrityError

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.models import Definition, User
from egis_agentkit.service.agent.views import (
    DefinitionViews,
    definition_data,
    require_current_shared_asset,
)
from egis_agentkit.service.user.service import require_admin
from egis_agentkit.storage import AgentKitStorage
from egis_agentkit.storage import tables as t


class AgentDefinitionManager:
    def __init__(self, storage: AgentKitStorage, policies=None):
        self.storage, self.policies = storage, policies
        self.views = DefinitionViews(storage)
        self.repository = self.views.repository

    async def personal(self, user: User) -> dict:
        identifier = sha256(("personal:" + user.user_id).encode()).hexdigest()[:32]
        record = Definition(
            id=identifier,
            owner_id=user.user_id,
            agent_type="personal",
            agent_name="personal",
            agent_display_name="个人助理",
            payload={"description": "你的工作助理", "avatar": "spark"},
        )
        async with self.storage.transaction() as conn:
            await self.repository.ensure_personal(
                record.model_dump(mode="json"), actor=user.user_id, conn=conn
            )
        return await self.repository.get_definition(identifier)

    async def save(
        self, user: User, data: Definition, *, conn=None, allow_team_role=False
    ) -> dict:
        if not user.is_enabled:
            raise Forbidden()
        if data.owner_type == "system":
            require_admin(user)
        elif data.owner_id != user.user_id and user.role_type != "admin":
            raise Forbidden()
        if data.definition_type == "team" or data.agent_type == "orchestrator":
            if not allow_team_role:
                raise Forbidden("编排智能体和专家团必须通过 TeamService 管理")
        if data.agent_type == "personal":
            raise Forbidden("个人助理由系统初始化，请通过偏好设置维护")
        if user.role_type != "admin":
            if data.owner_type != "user" or data.payload.get("implementation"):
                raise Forbidden("轻量专家不能上传或指定 Python 装配")
            if any(
                ref["resource_type"] in ("tool", "middleware")
                for ref in data.payload.get("bindings", [])
            ):
                raise Forbidden("轻量专家只允许绑定已授权的 Skill 和 MCP")
        if conn is None:
            try:
                async with self.storage.transaction() as connection:
                    return await self.save(
                        user, data, conn=connection, allow_team_role=allow_team_role
                    )
            except IntegrityError as exc:
                raise Conflict("英文名已存在，或配置与现有资源冲突") from exc
        previous = await self.repository.locked_definition(data.id, conn=conn)
        if previous:
            if (
                previous["owner_type"],
                previous["owner_id"],
                previous["definition_type"],
                previous["agent_type"],
            ) != (
                data.owner_type,
                data.owner_id,
                data.definition_type,
                data.agent_type,
            ):
                raise Forbidden("不能通过保存改变资源归属或类型")
            if previous["config_version"] != data.config_version:
                raise Conflict()
            if (
                previous["owner_type"] == "user"
                and previous["owner_id"] != user.user_id
                and user.role_type != "admin"
            ):
                raise Forbidden()
        await self._validate_bindings(user, data, conn)
        values = data.model_dump(mode="json")
        if previous:
            values["config_version"] += 1
            return await self.repository.update_definition(
                data.id, values, actor=user.user_id, conn=conn
            )
        if data.config_version != 1:
            raise Conflict("新资源的 config_version 必须为 1")
        return await self.repository.insert_definition(
            values, actor=user.user_id, conn=conn
        )

    async def _validate_bindings(self, user, data, conn):
        from egis_agentkit.service.access.authorization import PermissionService

        for ref in data.payload.get("bindings", []):
            asset = (
                (
                    await conn.execute(
                        select(t.assets).where(
                            t.assets.c.resource_type == ref["resource_type"],
                            t.assets.c.resource_id == ref["resource_id"],
                            t.assets.c.resource_version == ref["resource_version"],
                            t.assets.c.is_enabled,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if asset is None:
                raise NotFound("绑定资源或指定版本不存在")
            if ref["resource_view_type"] == "shared":
                await require_current_shared_asset(conn, asset)
            if data.owner_type == "system" and asset["owner_type"] != "system":
                raise Forbidden("系统定义不能引用用户私有资源")
            if (
                user.role_type != "admin"
                and not (
                    await PermissionService(self.storage, self.policies).explain(
                        user,
                        ref["resource_type"],
                        ref["resource_id"],
                        ref["resource_view_type"],
                    )
                ).is_allowed
            ):
                raise Forbidden("只能绑定本人或已授权的资源")

    async def raw_view(self, definition_id: str, view: str, *, conn=None) -> dict:
        return await self.views.raw_view(definition_id, view, conn=conn)

    async def resolve(self, user: User, definition_id: str, view: str) -> dict:
        from egis_agentkit.service.access.authorization import PermissionService

        if not (
            await PermissionService(self.storage, self.policies).explain(
                user, "agent", definition_id, view
            )
        ).is_allowed:
            raise Forbidden("未获得此资源的使用权限")
        return await self.raw_view(definition_id, view)

    async def list(self, user: User, *, definition_type="agent") -> list[dict]:
        from egis_agentkit.service.access.authorization import PermissionService

        permissions = PermissionService(self.storage, self.policies)
        rows, states = await self.repository.read_catalog(definition_type)
        result = []
        for row in rows:
            if row["agent_type"] in ("personal", "orchestrator"):
                continue
            own = row["owner_type"] == "user" and row["owner_id"] == user.user_id
            view = (
                "private"
                if own
                else "system"
                if row["owner_type"] == "system"
                else "shared"
            )
            try:
                value = await self.raw_view(row["id"], view)
            except Forbidden:
                if user.role_type != "admin":
                    continue
                value, view = (
                    definition_data(row),
                    "private" if row["owner_type"] == "user" else "system",
                )
            decision = await permissions.explain(user, definition_type, row["id"], view)
            # System catalog metadata is visible; invocation still defaults denied.
            result.append(
                {
                    **value,
                    "resource_view_type": view,
                    "is_usable": decision.is_allowed,
                    "is_editable": own or user.role_type == "admin",
                    "is_deletable": own or user.role_type == "admin",
                    "share_status_type": states.get(row["id"], "private"),
                }
            )
        return result

    async def delete(self, user: User, definition_id: str) -> None:
        async with self.storage.transaction() as conn:
            row = await self.repository.get_definition(definition_id, conn=conn)
            if row["agent_type"] in ("personal", "orchestrator"):
                raise Forbidden("不能单独删除个人助理或编排智能体")
            if user.role_type != "admin" and (
                row["owner_type"] != "user" or row["owner_id"] != user.user_id
            ):
                raise Forbidden()
            # Tombstone keeps sessions and share audit references resolvable.
            await self.repository.disable_definition(
                definition_id, actor=user.user_id, conn=conn
            )


# Compatibility for callers that still import the former definition service.
AgentService = AgentDefinitionManager
