from copy import deepcopy
from hashlib import sha256

from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.exc import IntegrityError

from ..errors import Conflict, Forbidden, NotFound
from ..models import Asset, Definition, User, utcnow
from ..storage import AgentKitStorage
from ..storage import tables as t
from .users import require_admin


def definition_data(row: dict) -> dict:
    return Definition.model_validate(
        {k: row[k] for k in Definition.model_fields}
    ).model_dump(mode="json")


async def require_current_shared_asset(conn, asset):
    """Validate a new shared edge against its exact current approved asset."""
    if asset["owner_type"] != "user":
        raise Forbidden("共享资源归属不匹配")
    review = (
        (
            await conn.execute(
                select(t.reviews).where(
                    t.reviews.c.resource_type == asset["resource_type"],
                    t.reviews.c.resource_id == asset["resource_id"],
                    t.reviews.c.is_current,
                    t.reviews.c.review_status_type == "approved",
                )
            )
        )
        .mappings()
        .first()
    )
    approved = review["snapshot"].get("asset") if review else None
    if (
        review is None
        or review["owner_id"] != asset["owner_id"]
        or approved != {key: asset[key] for key in Asset.model_fields}
    ):
        raise Forbidden("此资源版本未通过当前共享审核")


class AgentService:
    def __init__(self, storage: AgentKitStorage, policies=None):
        self.storage, self.policies = storage, policies

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
            await conn.execute(
                insert(t.definitions)
                .values(
                    **record.model_dump(mode="json"),
                    created_by=user.user_id,
                    updated_by=user.user_id,
                )
                .on_conflict_do_nothing()
            )
        return await self.storage.get_definition(identifier)

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
        previous = (
            (
                await conn.execute(
                    select(t.definitions)
                    .where(t.definitions.c.id == data.id)
                    .with_for_update()
                )
            )
            .mappings()
            .first()
        )
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
            row = (
                (
                    await conn.execute(
                        update(t.definitions)
                        .where(t.definitions.c.id == data.id)
                        .values(**values, updated_by=user.user_id, updated_at=utcnow())
                        .returning(t.definitions)
                    )
                )
                .mappings()
                .one()
            )
        else:
            if data.config_version != 1:
                raise Conflict("新资源的 config_version 必须为 1")
            row = (
                (
                    await conn.execute(
                        insert(t.definitions)
                        .values(
                            **values, created_by=user.user_id, updated_by=user.user_id
                        )
                        .returning(t.definitions)
                    )
                )
                .mappings()
                .one()
            )
        return dict(row)

    async def _validate_bindings(self, user, data, conn):
        from .permissions import PermissionService

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
        row = await self.storage.get_definition(definition_id, conn=conn)
        if not row["is_enabled"]:
            raise Forbidden("资源已停用")
        if view == "shared":
            statement = select(t.reviews).where(
                t.reviews.c.resource_id == definition_id,
                t.reviews.c.is_current,
                t.reviews.c.review_status_type == "approved",
            )
            if conn is None:
                async with self.storage.engine.connect() as connection:
                    review = (await connection.execute(statement)).mappings().first()
            else:
                review = (await conn.execute(statement)).mappings().first()
            if review is None:
                raise Forbidden("资源尚未通过共享审核")
            root = deepcopy(review["snapshot"]["root"])
            root["shared_version"] = review["shared_version"]
            root["shared_review_id"] = review["review_id"]
            return root
        if (
            view == "system"
            and row["owner_type"] != "system"
            or view == "private"
            and row["owner_type"] != "user"
        ):
            raise Forbidden("资源视图不匹配")
        return definition_data(row)

    async def resolve(self, user: User, definition_id: str, view: str) -> dict:
        from .permissions import PermissionService

        if not (
            await PermissionService(self.storage, self.policies).explain(
                user, "agent", definition_id, view
            )
        ).is_allowed:
            raise Forbidden("未获得此资源的使用权限")
        return await self.raw_view(definition_id, view)

    async def list(self, user: User, *, definition_type="agent") -> list[dict]:
        from .permissions import PermissionService

        permissions = PermissionService(self.storage, self.policies)
        async with self.storage.engine.connect() as conn:
            rows = [
                dict(r)
                for r in (
                    await conn.execute(
                        select(t.definitions)
                        .where(t.definitions.c.definition_type == definition_type)
                        .order_by(t.definitions.c.created_at.desc())
                    )
                ).mappings()
            ]
            review_rows = (
                await conn.execute(
                    select(
                        t.reviews.c.resource_id, t.reviews.c.review_status_type
                    ).order_by(t.reviews.c.submitted_at)
                )
            ).all()
        states = dict(review_rows)
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
            row = await self.storage.get_definition(definition_id, conn=conn)
            if row["agent_type"] in ("personal", "orchestrator"):
                raise Forbidden("不能单独删除个人助理或编排智能体")
            if user.role_type != "admin" and (
                row["owner_type"] != "user" or row["owner_id"] != user.user_id
            ):
                raise Forbidden()
            # Tombstone keeps sessions and share audit references resolvable.
            await conn.execute(
                update(t.definitions)
                .where(t.definitions.c.id == definition_id)
                .values(is_enabled=False, updated_by=user.user_id, updated_at=utcnow())
            )
