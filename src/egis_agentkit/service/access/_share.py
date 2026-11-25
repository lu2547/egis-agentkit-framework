"""维护资源首次共享及共享新版本的申请与审核。

核心能力
- 提交时固定当前发布版本及首次共享的接收对象。
- 审批通过时发放授权或切换已有受众的共享版。
"""

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service._admin_page import AdminPage
from egis_agentkit.service._user import UserService, require_admin
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import (
    DEFINITION_KINDS,
    INTERNAL_AGENT_TYPES,
    PermissionService,
)
from egis_agentkit.service.access._publication import same_publication_content
from egis_agentkit.storage.model import Grant, ShareReview, ShareTarget, User, utcnow
from egis_agentkit.storage.model._base import publication_label, resource_available


class ShareService:
    """记录并审核所有权限资源的共享申请。"""

    def __init__(self, storage, *, publish_skill=None):
        self.storage = storage
        self.grants = GrantService(storage)
        self._publish_skill = publish_skill

    async def recipients(self, user: User):
        """列出有效个人和组织组供申请人选择。"""
        if not user.is_enabled:
            raise Forbidden()
        users = await self.storage.users.list_users()
        groups = await self.storage.users.list_group_options()
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

    async def recipients_page(
        self, user: User, principal_type: str, query: str, page: AdminPage
    ) -> dict:
        """按类型搜索可接收共享或授权的对象。"""
        if not user.is_enabled:
            raise Forbidden()
        rows = await self.storage.users.search_recipients(
            principal_type, query.strip(), offset=page.offset, limit=page.fetch_limit
        )
        return page.response(
            [{**row, "principal_type": principal_type} for row in rows]
        )

    async def submit(
        self, user, resource_type, resource_id, *, target: ShareTarget | None = None
    ):
        """选择发布版本，记录新接收方或一次共享升级。"""
        if resource_type not in (*DEFINITION_KINDS, "skill"):
            raise Conflict("此资源类型不支持共享")
        if not user.is_enabled:
            raise Forbidden()
        async with self.storage.definition_write() as write:
            if resource_type == "skill":
                resource = await write.access.skill_write_state(
                    resource_id, target=target, owner_version=target is None
                )
                shared = (
                    resource["_shared_resource_version"]
                    if resource is not None
                    else None
                )
            else:
                shared = await write.access.shared_version(resource_type, resource_id)
                resource = await self.grants.read_version(
                    resource_type, resource_id, shared if target else None, write=write
                )
            self._validate_resource(user, resource)
            version = resource["resource_version"]
            if target:
                if "_recipient_enabled" in resource:
                    self.grants.require_loaded_recipient(
                        target, resource["_recipient_enabled"]
                    )
                    if resource["_has_share_recipient"]:
                        raise Conflict("该接收方已有共享授权")
                else:
                    await self.grants.require_recipient(target, write=write)
                    await self._check_new_recipient(
                        resource_type, resource_id, target, write=write
                    )
            else:
                if not shared or shared == version:
                    raise Conflict("没有可提交的共享新版本")
                previous = await self.grants.read_version(
                    resource_type, resource_id, shared, write=write
                )
                if same_publication_content(resource, previous):
                    raise Conflict("资源内容没有变化，无需共享新版本")
                pending = resource.get("_has_pending_update")
                if pending is None:
                    pending = await write.access.has_pending_update(
                        resource_type, resource_id
                    )
                if pending:
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
            return await self._with_publication_label(
                row, storage=write, resource=resource
            )

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

    async def list_reviews_page(self, admin: User, page: AdminPage) -> dict:
        require_admin(admin)
        rows = await self.storage.access.list_reviews(
            offset=page.offset, limit=page.fetch_limit
        )
        response = page.response(rows)
        response["items"] = [
            await self._with_publication_label(row, storage=self.storage)
            for row in response["items"]
        ]
        return response

    @staticmethod
    async def _with_publication_label(row, *, storage, resource=None):
        kind, version = row["resource_type"], row["resource_version"]
        if kind in DEFINITION_KINDS:
            revision = int(version)
        elif "_publication_revision" in row:
            revision = row["_publication_revision"]
        else:
            asset = (
                resource
                if resource is not None
                else await storage.assets.get(kind, row["resource_id"], version)
            )
            revision = asset["publication_revision"] if asset is not None else None
        return {
            **{
                key: value
                for key, value in row.items()
                if key != "_publication_revision"
            },
            "version_label": publication_label(revision) if revision else version,
        }

    async def decide(self, admin, review_id, is_approved, reason):
        """锁定申请并提交审核结果，允许重复提交相同决定。"""
        require_admin(admin)
        async with self.storage.definition_write() as write:
            review = await write.access.locked_review(review_id)
            if review is None:
                raise NotFound()
            if admin.user_id in {review["submitted_by"], review["owner_id"]}:
                raise Forbidden("不能审核自己的共享申请，请由其他管理员审核")
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
            if "_publication_revision" in review:
                updated["_publication_revision"] = review["_publication_revision"]
            return await self._with_publication_label(updated, storage=write)

    async def withdraw(self, user, review_id):
        """所有者取消申请或撤回单个受众，保留其他入口及历史发布文件。"""
        if not user.is_enabled:
            raise Forbidden("用户已停用")
        async with self.storage.definition_write() as write:
            review = await write.access.locked_review(review_id)
            if review is None:
                raise NotFound("共享申请不存在")
            if review["owner_id"] != user.user_id:
                raise Forbidden("只能撤回自己的共享申请")
            if review["review_status_type"] == "cancelled":
                return
            if review["review_status_type"] not in {"pending", "approved"}:
                raise Conflict("该共享申请已处理，不能撤回")
            if review["review_status_type"] == "approved":
                if review["review_type"] != "share":
                    raise Conflict("请在原共享申请中撤回接收方，已发布版本不能回退")
                # definition_write 的授权写锁串行化受众增加、撤回和版本切换。
                # 资源停用后仍允许所有者清理已发出的授权。
                for grant in await write.access.direct_grants(
                    review["resource_type"], review["resource_id"]
                ):
                    if grant["source_review_id"] == review_id:
                        await write.access.revoke_grant(grant["id"], actor=user.user_id)
            await write.access.update_review(review_id, {
                "review_status_type": "cancelled", "is_current": False,
            })

    async def _issue_approved_grant(self, admin, review, *, write):
        """重新校验申请版本，原子更新共享入口及其依赖。"""
        if review["resource_type"] == "skill":
            return await self._issue_approved_skill(admin, review, write=write)
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
            previous = await self.grants.read_version(kind, identifier, shared, write=write)
            if same_publication_content(resource, previous):
                raise Conflict("资源内容没有变化，无需共享新版本")
            await self._publish_dependencies(resource, write=write)
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
        await self._publish_dependencies(resource, write=write)
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

    async def _issue_approved_skill(self, admin, review, *, write):
        identifier, version = review["resource_id"], review["resource_version"]
        target = (
            ShareTarget(
                principal_type=review["share_scope_type"],
                principal_id=review["principal_id"],
            )
            if review["review_type"] == "share"
            else None
        )
        resource = await write.access.skill_write_state(
            identifier, version=version, target=target, owner_id=review["owner_id"]
        )
        if resource is None:
            await UserService(write).get(review["owner_id"])
            raise NotFound()
        if not resource["_owner_exists"]:
            raise NotFound("用户不存在")
        if not resource["_owner_enabled"]:
            raise Forbidden("用户已停用")
        owner = User(user_id=review["owner_id"], username=review["owner_id"])
        self._validate_resource(owner, resource)
        shared = resource["_shared_resource_version"]
        if review["review_type"] == "update":
            if not shared or shared == version:
                raise Conflict("没有可更新的共享版本")
            previous = await self.grants.read_version("skill", identifier, shared, write=write)
            if same_publication_content(resource, previous):
                raise Conflict("资源内容没有变化，无需共享新版本")
            await self._publish_personal_skill(resource)
            await write.access.set_shared_version("skill", identifier, version)
            return
        if shared is not None and shared != version:
            raise Conflict("共享版本已变更，请重新申请")
        if resource["_has_share_recipient"]:
            raise Conflict("该接收方已有共享授权")
        await self._publish_personal_skill(resource)
        await self.grants.issue(
            admin,
            Grant(
                **target.model_dump(),
                resource_type="skill",
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

    async def _publish_dependencies(self, definition, *, write):
        _, assets = await self.grants.definition_dependencies(definition, write=write)
        for asset in assets.values():
            await self._publish_personal_skill(asset)

    async def _publish_personal_skill(self, resource):
        if resource["owner_type"] != "user":
            return
        if self._publish_skill is None:
            from egis_agentkit.service.resource._catalog import ResourceService

            files = self.storage.history.files
            publisher = ResourceService(
                self.storage, files.root, config=files.config
            ).publish_shared_skill
        else:
            publisher = self._publish_skill
        await publisher(resource)
