from dataclasses import dataclass, field

from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.models import Grant, User
from egis_agentkit.service.agent.views import DefinitionViews
from egis_agentkit.service.user.service import require_admin


@dataclass
class PermissionDecision:
    is_allowed: bool
    paths: list[list[dict]] = field(default_factory=list)


class PermissionService:
    def __init__(self, storage, policies=None):
        self.storage, self.policies = storage, policies

    async def explain(
        self, user: User, resource_type: str, resource_id: str, view: str
    ) -> PermissionDecision:
        # Long-running turns must observe membership/disable changes at the
        # next protected action instead of trusting their initial User object.
        from egis_agentkit.service.user.service import UserService

        try:
            user = await UserService(self.storage).get(user.user_id)
        except (Forbidden, NotFound):
            return PermissionDecision(False)
        if resource_type in ("agent", "team"):
            try:
                target = await self.storage.get_definition(resource_id)
            except NotFound:
                return PermissionDecision(False)
        else:
            target = await self.storage.assets.find_asset(
                resource_type, resource_id, enabled_only=True
            )
            if target is None:
                return PermissionDecision(False)
        if not target["is_enabled"]:
            return PermissionDecision(False)
        node = {
            "resource_type": resource_type,
            "resource_id": resource_id,
            "resource_view_type": view,
        }
        if view == "private":
            own = target["owner_type"] == "user" and target["owner_id"] == user.user_id
            is_personal = target.get("agent_type") == "personal"
            enabled = getattr(
                self.policies,
                "allow_personal_self_use" if is_personal else "allow_private_self_use",
                True,
            )
            return PermissionDecision(
                bool(own and enabled),
                [[{"source": "private-self"}, node]] if own and enabled else [],
            )
        if (
            view == "system"
            and target["owner_type"] != "system"
            or view == "shared"
            and target["owner_type"] != "user"
        ):
            return PermissionDecision(False)
        grants = await self.storage.access.active_grants(user.user_id, user.group_ids)
        paths = []
        for grant in grants:
            if (
                grant["resource_type"] == resource_type
                and grant["resource_id"] == resource_id
                and grant["resource_view_type"] == view
            ):
                if await self._available(resource_type, resource_id, view):
                    paths.append([{"grant_id": grant["id"]}, node])
            elif grant["resource_type"] in ("agent", "team"):
                path = await self._find_path(
                    grant["resource_type"],
                    grant["resource_id"],
                    grant["resource_view_type"],
                    node,
                )
                if path:
                    paths.append([{"grant_id": grant["id"]}, *path])
        return PermissionDecision(bool(paths), paths)

    async def _available(self, resource_type, resource_id, view):
        if resource_type in ("agent", "team"):
            try:
                await DefinitionViews(self.storage).raw_view(resource_id, view)
            except (Forbidden, NotFound):
                return False
        elif view == "shared":
            return (
                await self.storage.access.approved_review(
                    resource_id, resource_type=resource_type
                )
                is not None
            )
        return True

    async def _find_path(self, root_type, root_id, root_view, target):
        from egis_agentkit.service.access.snapshot import SnapshotService

        try:
            snapshot = await SnapshotService(self.storage).capture(root_id, root_view)
        except (Forbidden, NotFound):
            return None
        return SnapshotService.path(snapshot, target)

    async def require(self, user, resource_type, resource_id, view):
        decision = await self.explain(user, resource_type, resource_id, view)
        if not decision.is_allowed:
            raise Forbidden("未获得此资源的使用权限")
        return decision

    async def validate_snapshot(self, user, snapshot):
        """Check current authority without changing pinned configuration.

        Self-use of a private definition is not a new grant to its external
        dependencies. Crossing from private configuration into a system/shared
        node therefore needs an independently valid USE path. Once a granted
        public node is entered, its pinned descendants inherit that authority.
        """
        nodes = snapshot.get("nodes", {})
        root = nodes.get(snapshot.get("root_key"))
        if root is None or not all(
            k in root for k in ("resource_type", "resource_id", "resource_view_type")
        ):
            raise Forbidden("执行快照缺少权限根")
        await self.require(
            user, root["resource_type"], root["resource_id"], root["resource_view_type"]
        )
        pending = [(snapshot["root_key"], root["resource_view_type"] != "private")]
        seen = set()
        while pending:
            key, inherited = pending.pop()
            if (key, inherited) in seen:
                continue
            seen.add((key, inherited))
            node = nodes.get(key)
            if node is None:
                raise Forbidden("执行快照引用不完整")
            kind, identifier, view = (
                node["resource_type"],
                node["resource_id"],
                node["resource_view_type"],
            )
            if not inherited:
                await self.require(user, kind, identifier, view)
                inherited = view != "private"
            if kind in ("agent", "team"):
                current = await self.storage.get_definition(identifier)
                if not current["is_enabled"]:
                    raise Forbidden("绑定智能体已停用")
            else:
                enabled = await self.storage.assets.version_is_enabled(
                    kind, identifier, node["data"]["resource_version"]
                )
                if not enabled:
                    raise Forbidden("绑定资源已停用")
            pending.extend((child, inherited) for child in node.get("edges", []))

    async def list_grants(self, admin):
        require_admin(admin)
        return await self.storage.access.list_grants()

    async def grant(self, admin: User, data: Grant, *, conn=None):
        require_admin(admin)
        if conn is None:
            async with self.storage.transaction() as connection:
                return await self.grant(admin, data, conn=connection)
        if data.principal_type == "user":
            principal = await self.storage.users.get_user(data.principal_id, conn=conn)
            if principal is None or not principal["is_enabled"]:
                raise NotFound("授权用户不存在或已停用")
        if data.principal_type == "group":
            enabled = await self.storage.users.enabled_group_ids(
                [data.principal_id], conn=conn
            )
            if not enabled:
                raise NotFound("授权分组不存在或已停用")
        if data.resource_type in ("agent", "team"):
            resource = await self.storage.get_definition(data.resource_id, conn=conn)
        else:
            resource = await self.storage.assets.find_asset(
                data.resource_type, data.resource_id, conn=conn
            )
            if resource is None:
                raise NotFound()
        if (resource["owner_type"], resource["owner_id"]) != (
            data.resource_owner_type,
            data.resource_owner_id,
        ):
            raise Forbidden("授权目标归属不匹配")
        if data.resource_view_type == "system" and resource["owner_type"] != "system":
            raise Forbidden("不能通过系统授权开放用户私有配置")
        return await self.storage.access.upsert_grant(
            data.model_dump(exclude={"revoked_at"}), actor=admin.user_id, conn=conn
        )

    async def revoke(self, admin, grant_id):
        require_admin(admin)
        async with self.storage.transaction() as conn:
            if not await self.storage.access.revoke_grant(
                grant_id, actor=admin.user_id, conn=conn
            ):
                raise NotFound()
