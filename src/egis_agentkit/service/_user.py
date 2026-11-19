"""同步用户身份并管理角色与组织组。"""

from egis_agentkit.client.nacos._settings import AccessConfig
from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.storage import AgentKitStorage
from egis_agentkit.storage.model import Group, User
from egis_agentkit.storage.model._base import new_id

from ._admin_page import AdminPage

VIRTUAL_USER_PREFIX = "skill-system."


def require_admin(user: User) -> None:
    """确认当前有效用户具有管理员角色。"""
    if not user.is_enabled or user.role_type != "admin":
        raise Forbidden("只有管理员可以执行此操作")


class UserService:
    """读取有效身份，管理用户角色、组织组和成员关系。"""

    def __init__(self, storage: AgentKitStorage, policies=None):
        self.storage = storage
        self.policies = policies if policies is not None else AccessConfig()

    def company_group(self) -> Group:
        return Group(
            group_id=self.policies.company_group_id,
            group_name=self.policies.company_group_name,
        )

    async def get(self, user_id: str) -> User:
        """重新读取数据库身份，使角色和组成员变更立即生效。"""
        row = await self.storage.get_user(user_id)
        if row is None:
            raise NotFound("用户不存在")
        user = User.model_validate({key: row[key] for key in User.model_fields})
        if not user.is_enabled:
            raise Forbidden("用户已停用")
        return user

    async def sync_identity(self, user_id: str, username: str) -> User:
        if user_id.startswith(VIRTUAL_USER_PREFIX):
            raise Forbidden("此身份标识保留给系统虚拟用户")
        company = self.company_group()
        identity = User(user_id=user_id, username=username, group_ids=[company.group_id])
        async with self.storage.users.write() as users:
            if await users.get_user(user_id) is None:
                await users.ensure_group(company, actor="identity")
            await users.upsert_identity(identity)
        return await self.get(user_id)

    async def provision_admin(self, user_id: str, *, actor: str) -> None:
        """Explicit trusted CLI bootstrap; never exposed as a login operation."""
        async with self.storage.users.write() as users:
            if not await users.set_admin(user_id, actor=actor):
                raise NotFound("先创建可信用户，再设置初始管理员")

    async def list_users(self, admin: User) -> list[dict]:
        require_admin(admin)
        return await self.storage.users.list_users()

    async def list_users_page(
        self, admin: User, page: AdminPage, *, group_id: str | None = None
    ) -> dict:
        require_admin(admin)
        rows = await self.storage.users.list_users(
            group_id=group_id, offset=page.offset, limit=page.fetch_limit
        )
        return page.response(rows)

    async def update_user(self, admin: User, data: User) -> dict:
        require_admin(admin)
        data.group_ids = list(dict.fromkeys(data.group_ids))
        async with self.storage.users.write() as users:
            if data.group_ids:
                valid = await users.enabled_group_ids(data.group_ids)
                if valid != set(data.group_ids):
                    raise NotFound("分组不存在或已停用")
            # Serialize concurrent attempts to remove the last enabled admin.
            admins = await users.lock_enabled_admins()
            if (
                data.user_id in admins
                and (data.role_type != "admin" or not data.is_enabled)
                and len(admins) == 1
            ):
                raise Forbidden("不能停用或降级最后一位管理员")
            row = await users.update_user(data, actor=admin.user_id)
            if row is None:
                raise NotFound()
            return row

    async def list_groups(self, admin: User, *, include_members: bool = True) -> list[dict]:
        require_admin(admin)
        if not include_members:
            return await self.storage.users.list_group_options()
        rows, members = await self.storage.users.list_groups_with_members()
        for row in rows:
            row["users"] = [
                {"user_id": m["user_id"], "username": m["username"]}
                for m in members
                if row["group_id"] in m["group_ids"]
            ]
        return rows

    async def list_groups_page(self, admin: User, page: AdminPage) -> dict:
        require_admin(admin)
        rows = await self.storage.users.list_groups_page(
            offset=page.offset, limit=page.fetch_limit
        )
        return page.response(rows)

    async def save_group(self, admin: User, data: Group) -> dict:
        require_admin(admin)
        async with self.storage.users.write() as users:
            return await users.save_group(data, actor=admin.user_id)

    async def create_group(self, admin: User, name: str, *, is_enabled: bool) -> dict:
        require_admin(admin)
        group = Group(group_id=new_id(), group_name=name, is_enabled=is_enabled)
        async with self.storage.users.write() as users:
            return await users.create_group(group, actor=admin.user_id)

    async def update_group(self, admin: User, data: Group) -> dict:
        require_admin(admin)
        async with self.storage.users.write() as users:
            group = await users.update_group(data, actor=admin.user_id)
            if group is None:
                raise NotFound("用户组不存在")
            return group
