from sqlalchemy import select, update
from sqlalchemy.dialects.postgresql import insert

from ..errors import Forbidden, NotFound
from ..models import Group, User, utcnow
from ..storage import AgentKitStorage
from ..storage import tables as t


def require_admin(user: User) -> None:
    if not user.is_enabled or user.role_type != "admin":
        raise Forbidden("只有管理员可以执行此操作")


class UserService:
    def __init__(self, storage: AgentKitStorage):
        self.storage = storage

    async def get(self, user_id: str) -> User:
        row = await self.storage.get_user(user_id)
        if row is None:
            raise NotFound("用户不存在")
        user = User.model_validate({k: row[k] for k in User.model_fields})
        if not user.is_enabled:
            raise Forbidden("用户已停用")
        return user

    async def sync_identity(self, user_id: str, username: str) -> User:
        identity = User(user_id=user_id, username=username)
        async with self.storage.transaction() as conn:
            # Login never updates roles, membership or enablement.
            statement = insert(t.users).values(
                **identity.model_dump(), created_by="identity", updated_by="identity"
            )
            await conn.execute(
                statement.on_conflict_do_update(
                    index_elements=[t.users.c.user_id],
                    set_={
                        "username": identity.username,
                        "updated_at": utcnow(),
                        "updated_by": "identity",
                    },
                )
            )
        return await self.get(user_id)

    async def provision_admin(self, user_id: str, *, actor: str) -> None:
        """Explicit trusted CLI bootstrap; never exposed as a login operation."""
        async with self.storage.transaction() as conn:
            result = await conn.execute(
                update(t.users)
                .where(t.users.c.user_id == user_id)
                .values(role_type="admin", updated_by=actor, updated_at=utcnow())
            )
            if result.rowcount != 1:
                raise NotFound("先创建可信用户，再设置初始管理员")

    async def list_users(self, admin: User) -> list[dict]:
        require_admin(admin)
        async with self.storage.engine.connect() as conn:
            return [
                dict(r)
                for r in (
                    await conn.execute(select(t.users).order_by(t.users.c.user_id))
                ).mappings()
            ]

    async def update_user(self, admin: User, data: User) -> dict:
        require_admin(admin)
        data.group_ids = list(dict.fromkeys(data.group_ids))
        async with self.storage.transaction() as conn:
            if data.group_ids:
                valid = set(
                    (
                        await conn.execute(
                            select(t.groups.c.group_id).where(
                                t.groups.c.group_id.in_(data.group_ids),
                                t.groups.c.is_enabled,
                            )
                        )
                    ).scalars()
                )
                if valid != set(data.group_ids):
                    raise NotFound("分组不存在或已停用")
            # Lock admin rows to serialize concurrent removal of the last admin.
            admins = list(
                (
                    await conn.execute(
                        select(t.users.c.user_id)
                        .where(t.users.c.role_type == "admin", t.users.c.is_enabled)
                        .order_by(t.users.c.user_id)
                        .with_for_update()
                    )
                ).scalars()
            )
            if (
                data.user_id in admins
                and (data.role_type != "admin" or not data.is_enabled)
                and len(admins) == 1
            ):
                raise Forbidden("不能停用或降级最后一位管理员")
            row = (
                (
                    await conn.execute(
                        update(t.users)
                        .where(t.users.c.user_id == data.user_id)
                        .values(
                            **data.model_dump(),
                            updated_by=admin.user_id,
                            updated_at=utcnow(),
                        )
                        .returning(t.users)
                    )
                )
                .mappings()
                .first()
            )
            if row is None:
                raise NotFound()
            return dict(row)

    async def list_groups(self, admin: User) -> list[dict]:
        require_admin(admin)
        async with self.storage.engine.connect() as conn:
            rows = [
                dict(r)
                for r in (
                    await conn.execute(select(t.groups).order_by(t.groups.c.group_name))
                ).mappings()
            ]
            members = (
                (
                    await conn.execute(
                        select(
                            t.users.c.user_id, t.users.c.username, t.users.c.group_ids
                        )
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            row["users"] = [
                {"user_id": m["user_id"], "username": m["username"]}
                for m in members
                if row["group_id"] in m["group_ids"]
            ]
        return rows

    async def save_group(self, admin: User, data: Group) -> dict:
        require_admin(admin)
        async with self.storage.transaction() as conn:
            statement = insert(t.groups).values(
                **data.model_dump(), created_by=admin.user_id, updated_by=admin.user_id
            )
            row = (
                (
                    await conn.execute(
                        statement.on_conflict_do_update(
                            index_elements=[t.groups.c.group_id],
                            set_={
                                **data.model_dump(),
                                "updated_by": admin.user_id,
                                "updated_at": utcnow(),
                            },
                        ).returning(t.groups)
                    )
                )
                .mappings()
                .one()
            )
        return dict(row)
