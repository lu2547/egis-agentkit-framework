from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.models import Group, User
from egis_agentkit.storage import AgentKitStorage


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
            await self.storage.users.upsert_identity(identity, conn=conn)
        return await self.get(user_id)

    async def provision_admin(self, user_id: str, *, actor: str) -> None:
        """Explicit trusted CLI bootstrap; never exposed as a login operation."""
        async with self.storage.transaction() as conn:
            if not await self.storage.users.set_admin(user_id, actor=actor, conn=conn):
                raise NotFound("先创建可信用户，再设置初始管理员")

    async def list_users(self, admin: User) -> list[dict]:
        require_admin(admin)
        return await self.storage.users.list_users()

    async def update_user(self, admin: User, data: User) -> dict:
        require_admin(admin)
        data.group_ids = list(dict.fromkeys(data.group_ids))
        async with self.storage.transaction() as conn:
            if data.group_ids:
                valid = await self.storage.users.enabled_group_ids(
                    data.group_ids, conn=conn
                )
                if valid != set(data.group_ids):
                    raise NotFound("分组不存在或已停用")
            # Serialize concurrent attempts to remove the last enabled admin.
            admins = await self.storage.users.lock_enabled_admins(conn=conn)
            if (
                data.user_id in admins
                and (data.role_type != "admin" or not data.is_enabled)
                and len(admins) == 1
            ):
                raise Forbidden("不能停用或降级最后一位管理员")
            row = await self.storage.users.update_user(
                data, actor=admin.user_id, conn=conn
            )
            if row is None:
                raise NotFound()
            return row

    async def list_groups(self, admin: User) -> list[dict]:
        require_admin(admin)
        rows, members = await self.storage.users.list_groups_with_members()
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
            return await self.storage.users.save_group(
                data, actor=admin.user_id, conn=conn
            )
