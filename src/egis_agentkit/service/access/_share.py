"""维护资源首次共享及共享新版本的申请与审核。

核心能力
- 提交时固定当前发布版本及首次共享的接收对象。
- 审批通过时发放授权或切换已有受众的共享版。
"""

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service._user import UserService, require_admin
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import (
    DEFINITION_KINDS,
    INTERNAL_AGENT_TYPES,
    PermissionService,
)
from egis_agentkit.storage.model import Grant, ShareReview, ShareTarget, User, utcnow
from egis_agentkit.storage.model._base import publication_label, resource_available


class ShareService:
    """记录并审核所有权限资源的共享申请。"""

    def __init__(self, storage):
        self.storage = storage
        self.grants = GrantService(storage)

    async def recipients(self, user: User):
        """列出有效个人和组织组供申请人选择。"""
        if not user.is_enabled:
            raise Forbidden()
        users = await self.storage.users.list_users()
        groups, _ = await self.storage.users.list_groups_with_members()
        return [
            *(
                {
                    "principal_type": "user",
                    "principal_id": row["user_id"],
                    "display_name": row["username"],
                }
                for row in users
                if row["is_enabled"]
            ),
            *(
                {
                    "principal_type": "group",
                    "principal_id": row["group_id"],
                    "display_name": row["group_name"],
                }
                for row in groups
                if row["is_enabled"]
            ),
        ]

    async def submit(
        self, user, resource_type, resource_id, *, target: ShareTarget | None = None
    ):
        """选择发布版本，记录新接收方或一次共享升级。"""
        if resource_type not in (*DEFINITION_KINDS, "skill"):
            raise Conflict("此资源类型不支持共享")
        if not user.is_enabled:
            raise Forbidden()
        async with self.storage.definition_write() as write:
            shared = await write.access.shared_version(resource_type, resource_id)
            resource = await self.grants.read_version(
                resource_type, resource_id, shared if target else None, write=write
            )
            self._validate_resource(user, resource)
            version = resource["resource_version"]
            if target:
                await self.grants.require_recipient(target, write=write)
                await self._check_new_recipient(
                    resource_type, resource_id, target, write=write
                )
            else:
                if not shared or shared == version:
                    raise Conflict("没有可提交的共享新版本")
                if await write.access.has_pending_update(resource_type, resource_id):
                    raise Conflict("已有待审核的共享版本")
            await self._authorize_dependencies(
                user, resource_type, resource, write=write
            )
            review = ShareReview(
                resource_type=resource_type,
                resource_id=resource_id,
                resource_version=version,
                review_type="share" if target else "update",
                owner_id=user.user_id,
                share_scope_type=target.principal_type if target else None,
                principal_id=target.principal_id if target else None,
                submitted_by=user.user_id,
            )
            row = await write.access.insert_review(review.model_dump())
            return await self._with_publication_label(row, storage=write)

    @staticmethod
    async def _check_new_recipient(kind, identifier, target, *, write):
        roots = await write.access.direct_grants(kind, identifier)
        if any(
            row["grant_source_type"] == "share"
            and (row["principal_type"], row["principal_id"])
            == (target.principal_type, target.principal_id)
            for row in roots
        ):
            raise Conflict("该接收方已有共享授权")

    @staticmethod
    async def _authorize_dependencies(user, kind, resource, *, write):
        if kind in DEFINITION_KINDS:
            await PermissionService(write).require_execution(
                user, {**resource, "resource_view_type": "private"}
            )

    @staticmethod
    def _validate_resource(user, resource):
        if resource is None:
            raise NotFound()
        if (resource["owner_type"], resource["owner_id"]) != ("user", user.user_id):
            raise Forbidden("只能申请共享自己的资源")
        if not resource_available(resource):
            raise Conflict("源资源已经停用")
        if resource.get("agent_type") in INTERNAL_AGENT_TYPES:
            raise Forbidden("该资源不能单独共享")

    async def list(self, user, *, admin=False):
        """读取本人申请，或由管理员读取审核队列。"""
        if admin:
            require_admin(user)
        rows = await self.storage.access.list_reviews(
            owner_id=None if admin else user.user_id
        )
        return [
            await self._with_publication_label(row, storage=self.storage)
            for row in rows
        ]

    @staticmethod
    async def _with_publication_label(row, *, storage):
        kind, version = row["resource_type"], row["resource_version"]
        if kind in DEFINITION_KINDS:
            revision = int(version)
        else:
            asset = await storage.assets.get(kind, row["resource_id"], version)
            revision = asset["publication_revision"] if asset is not None else None
        return {
            **row,
            "version_label": publication_label(revision) if revision else version,
        }

    async def decide(self, admin, review_id, is_approved, reason):
        """锁定申请并提交审核结果，允许重复提交相同决定。"""
        require_admin(admin)
        async with self.storage.definition_write() as write:
            review = await write.access.locked_review(review_id)
            if review is None:
                raise NotFound()
            desired = "approved" if is_approved else "rejected"
            if review["review_status_type"] == desired:
                return await self._with_publication_label(review, storage=write)
            if review["review_status_type"] != "pending":
                raise Conflict("申请已处理")
            if is_approved:
                await self._issue_approved_grant(admin, review, write=write)
            updated = await write.access.update_review(
                review_id,
                {
                    "review_status_type": desired,
                    "reviewed_by": admin.user_id,
                    "reviewed_at": utcnow(),
                    "review_reason": reason,
                    "is_current": is_approved,
                },
            )
            return await self._with_publication_label(updated, storage=write)

    async def _issue_approved_grant(self, admin, review, *, write):
        """重新校验申请版本，原子更新共享入口及其依赖。"""
        owner = await UserService(write).get(review["owner_id"])
        kind, identifier, version = (
            review["resource_type"],
            review["resource_id"],
            review["resource_version"],
        )
        if kind == "mcp":
            raise Conflict("连接器由管理员统一上架")
        resource = await self.grants.read_version(
            kind, identifier, version, write=write
        )
        self._validate_resource(owner, resource)
        shared = await write.access.shared_version(kind, identifier)
        await self._authorize_dependencies(owner, kind, resource, write=write)
        if review["review_type"] == "update":
            if not shared or shared == version:
                raise Conflict("没有可更新的共享版本")
            await write.access.set_shared_version(kind, identifier, version)
            if kind in DEFINITION_KINDS:
                await self.grants.refresh_dependencies(
                    resource, actor=admin.user_id, write=write
                )
            return
        if shared is not None and shared != version:
            raise Conflict("共享版本已变更，请重新申请")
        target = ShareTarget(
            principal_type=review["share_scope_type"],
            principal_id=review["principal_id"],
        )
        await self._check_new_recipient(kind, identifier, target, write=write)
        await self.grants.issue(
            admin,
            Grant(
                **target.model_dump(),
                resource_type=kind,
                resource_id=identifier,
                resource_view_type="shared",
                resource_owner_type="user",
                resource_owner_id=review["owner_id"],
                grant_source_type="share",
                source_review_id=review["review_id"],
            ),
            resource,
            write=write,
        )
