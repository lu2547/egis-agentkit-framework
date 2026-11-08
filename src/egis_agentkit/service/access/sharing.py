from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.models import Asset, Grant, ShareReview, User, utcnow
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.access.snapshot import SnapshotService
from egis_agentkit.service.user.service import require_admin


class ShareService:
    def __init__(self, storage):
        self.storage = storage

    async def submit(
        self,
        user: User,
        resource_type: str,
        resource_id: str,
        resource_version: str | None = None,
    ):
        if resource_type not in ("agent", "team", "skill", "mcp"):
            raise Conflict("资源文件共享请先保存为不可变版本")
        async with self.storage.transaction() as conn:
            if resource_type in ("skill", "mcp"):
                if not resource_version:
                    raise Conflict("共享资源必须指定准确版本")
                row = await self.storage.assets.locked_version(
                    resource_type, resource_id, resource_version, conn=conn
                )
                if row is None:
                    raise NotFound()
                if row["owner_type"] != "user" or row["owner_id"] != user.user_id:
                    raise Forbidden("只能申请共享自己的资源")
                if not user.is_enabled or not row["is_enabled"]:
                    raise Conflict("源资源已经停用")
                snapshot = {"asset": {key: row[key] for key in Asset.model_fields}}
                source_versions = {resource_id: resource_version}
            else:
                row = await self.storage.get_definition(resource_id, conn=conn)
                if row["owner_type"] != "user" or row["owner_id"] != user.user_id:
                    raise Forbidden("只能申请共享自己的资源")
                if (
                    row["agent_type"] in ("personal", "orchestrator")
                    or row["definition_type"] != resource_type
                ):
                    raise Forbidden("该资源不能单独共享")
                snapshot = await SnapshotService(self.storage).capture(
                    resource_id, "private", conn=conn
                )
                for node in snapshot["nodes"].values():
                    data = node["data"]
                    if (
                        node["resource_view_type"] == "private"
                        and data.get("owner_id") != user.user_id
                    ):
                        raise Forbidden("共享闭包不能包含他人的私有资源")
                source_versions = snapshot["source_versions"]
            review = ShareReview(
                resource_type=resource_type,
                resource_id=resource_id,
                owner_id=user.user_id,
                source_versions=source_versions,
                snapshot=snapshot,
                submitted_by=user.user_id,
            )
            return await self.storage.access.insert_review(
                review.model_dump(), conn=conn
            )

    async def list(self, user, *, admin=False):
        if admin:
            require_admin(user)
        return await self.storage.access.list_reviews(
            owner_id=None if admin else user.user_id
        )

    async def decide(self, admin, review_id, is_approved, reason):
        require_admin(admin)
        async with self.storage.transaction() as conn:
            review = await self.storage.access.locked_review(review_id, conn=conn)
            if review is None:
                raise NotFound()
            desired = "approved" if is_approved else "rejected"
            if review["review_status_type"] == desired:
                return review  # Idempotent retry does not create a new version/grant.
            if review["review_status_type"] != "pending":
                raise Conflict("申请已处理")
            # Serialize approvals on the stable source row; review rows differ.
            resource_type, resource_id = review["resource_type"], review["resource_id"]
            is_asset = resource_type in ("skill", "mcp")
            if is_asset:
                version = review["source_versions"].get(resource_id)
                root = await self.storage.assets.locked_version(
                    resource_type, resource_id, version, conn=conn
                )
            else:
                root = await self.storage.definitions.locked_definition(
                    resource_id, conn=conn
                )
                if root is None:
                    raise NotFound()
            values = {
                "review_status_type": desired,
                "reviewed_by": admin.user_id,
                "reviewed_at": utcnow(),
                "review_reason": reason,
                "is_current": False,
            }
            if is_approved:
                if root is None or not root["is_enabled"]:
                    raise Conflict("源资源已经停用")
                if is_asset:
                    if {key: root[key] for key in Asset.model_fields} != review[
                        "snapshot"
                    ].get("asset"):
                        raise Conflict("源配置已修改，请重新提交共享申请")
                else:
                    for identifier, version in review["source_versions"].items():
                        row = await self.storage.definitions.locked_definition(
                            identifier, conn=conn
                        )
                        if (
                            row is None
                            or not row["is_enabled"]
                            or row["config_version"] != version
                        ):
                            raise Conflict("源配置已修改，请重新提交共享申请")
                latest = await self.storage.access.latest_shared_version(
                    resource_type, resource_id, conn=conn
                )
                await self.storage.access.clear_current_review(
                    resource_type, resource_id, conn=conn
                )
                values.update(is_current=True, shared_version=(latest or 0) + 1)
                await PermissionService(self.storage).grant(
                    admin,
                    Grant(
                        principal_type="all_users",
                        principal_id="*",
                        resource_type=resource_type,
                        resource_id=resource_id,
                        resource_view_type="shared",
                        resource_owner_type="user",
                        resource_owner_id=review["owner_id"],
                        grant_source_type="share",
                        source_review_id=review_id,
                    ),
                    conn=conn,
                )
            return await self.storage.access.update_review(review_id, values, conn=conn)
