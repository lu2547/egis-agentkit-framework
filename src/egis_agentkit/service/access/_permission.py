"""按资源版本和调用场景判定使用权限。

核心能力
- 校验定义的维护和资源绑定权限。
- 区分目录入口与专家、专家团内部的授权路径。
- 校验执行时的入口与依赖授权。
"""

from dataclasses import dataclass, field

from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service._user import UserService, require_admin
from egis_agentkit.storage.model import AgentDefinition, User
from egis_agentkit.storage.model._base import resource_available
from egis_agentkit.storage.model._resource import (
    MCP_STORAGE_VERSION,
    is_managed_connector,
)

DEFINITION_KINDS = ("agent", "team")
INTERNAL_AGENT_TYPES = ("personal", "orchestrator")


def is_system_default_tool(asset):
    payload = asset["payload"]
    return (
        asset.get("resource_type") == "tool"
        and asset["owner_type"] == "system"
        and payload.get("exposure") == "model"
        and payload.get("source") in ("workspace", "toolkit", "middleware")
    )


@dataclass
class PermissionDecision:
    """单次权限查询返回的版本和生效路径。"""

    is_allowed: bool
    paths: list[list[dict]] = field(default_factory=list)
    review_id: str | None = None
    grant_id: str | None = None
    root_grant_id: str | None = None
    resource_version: str | None = None

    @property
    def source_options(self):
        return {
            key: value
            for key, value in (
                ("grant_id", self.grant_id),
                ("review_id", self.review_id),
            )
            if value is not None
        }


@dataclass(frozen=True)
class DefinitionAccess:
    """查询服务读取定义时使用的授权结果。"""

    view: str
    is_editable: bool
    decision: PermissionDecision


class PermissionService:
    """按用户、资源版本和调用上下文检查访问权限。"""

    def __init__(self, storage, policies=None):
        self.storage, self.policies = storage, policies

    @staticmethod
    def require_definition_write(user: User, *, owner_type: str, owner_id: str) -> None:
        """创建、更新和停用共用定义维护权限。"""
        if not user.is_enabled:
            raise Forbidden()
        if owner_type == "system":
            require_admin(user)
        elif user.role_type != "admin" and owner_id != user.user_id:
            raise Forbidden()

    async def require_agent_bindings(
        self, user: User, data: AgentDefinition
    ) -> list[dict]:
        """校验绑定资源的归属和使用权，返回通过校验的资源。"""
        assets = []
        for ref in data.payload.get("bindings", []):
            kind, identifier = ref["resource_type"], ref["resource_id"]
            version = MCP_STORAGE_VERSION if kind == "mcp" else ref["resource_version"]
            asset = await self.storage.assets.get(kind, identifier, version)
            if asset is None or not asset["is_enabled"]:
                raise NotFound("绑定资源或指定版本不存在")
            if kind == "mcp":
                if not is_managed_connector(asset):
                    raise NotFound("连接器未在管理控制台登记")
                if user.role_type != "admin":
                    await self.require(user, kind, identifier, "system")
                assets.append(asset)
                continue
            view = ref["resource_view_type"]
            if (view == "system") != (asset["owner_type"] == "system"):
                raise Forbidden("绑定资源视图与归属不匹配")
            if view == "private" and data.owner_id != asset["owner_id"]:
                raise Forbidden("只能绑定自己的私有资源")
            if data.owner_type == "system" and asset["owner_type"] != "system":
                raise Forbidden("系统定义不能引用用户私有资源")
            assets.append(asset)
            if ref["resource_type"] == "tool":
                continue
            if (user.role_type != "admin" or view == "shared") and not (
                await self.explain(
                    user,
                    ref["resource_type"],
                    ref["resource_id"],
                    ref["resource_view_type"],
                    version=ref["resource_version"],
                )
            ).is_allowed:
                raise Forbidden("只能绑定本人或已授权的资源")

        return assets

    async def definition_access(self, user, source, view=None, *, version=None):
        """确定查询可读的视图与版本，并标记维护权限。"""
        own = source["owner_type"] == "user" and source["owner_id"] == user.user_id
        editable = own or user.role_type == "admin"
        catalog = view is None
        if catalog:
            view = (
                "system"
                if source["owner_type"] == "system"
                else ("private" if own else "shared")
            )
        if not user.is_enabled or (view == "private" and not editable):
            raise Forbidden()
        decision = await self.explain(
            user, source["definition_type"], source["id"], view, version=version
        )
        if view == "shared" and not decision.is_allowed:
            if not catalog or not editable:
                raise Forbidden()
            view = "private"
        if view == "system" and not (editable or decision.is_allowed):
            raise Forbidden()
        current = version is None or str(
            source["owner_version"]
        ) == str(version)
        return DefinitionAccess(
            view, editable and current and view != "shared", decision
        )

    async def definition_versions(self, user, source):
        """列出可维护的历史或独立可用的当前版本引用。"""
        editable = user.role_type == "admin" or (
            source["owner_type"] == "user" and source["owner_id"] == user.user_id
        )
        if editable and user.is_enabled:
            view = "system" if source["owner_type"] == "system" else "private"
            versions = await self.storage.definitions.list_versions(source["id"])
            return [
                {"resource_version": str(version), "resource_view_type": view}
                for version in versions
            ]
        view = "system" if source["owner_type"] == "system" else "shared"
        decision = await self.explain(
            user, source["definition_type"], source["id"], view
        )
        if not decision.is_allowed:
            return []
        return [
            {"resource_version": decision.resource_version, "resource_view_type": view}
        ]

    async def share_availability(self, user, kind, identifier, *, version, view):
        """返回当前私有版本可提交的共享操作。"""
        result = {
            "can_share": False,
            "can_share_update": False,
            "has_shared_version": False,
        }
        if (
            not user.is_enabled
            or view != "private"
            or kind not in (*DEFINITION_KINDS, "skill")
        ):
            return result
        target = await self._resource(kind, identifier, None)
        if (
            target is None
            or target["owner_id"] != user.user_id
            or target["owner_type"] != "user"
            or target.get("agent_type") in INTERNAL_AGENT_TYPES
        ):
            return result
        shared = await self.storage.access.shared_version(kind, identifier)
        result["has_shared_version"] = shared is not None
        if resource_available(target) and target["resource_version"] == str(version):
            result["can_share"] = shared is None
            result["can_share_update"] = (
                shared is not None
                and shared != str(version)
                and not await self.storage.access.has_pending_update(kind, identifier)
            )
        return result

    async def _resource(self, kind, identifier, version):
        if kind in DEFINITION_KINDS:
            row = await self.storage.definitions.get_definition(
                identifier, version=version
            )
            if row["definition_type"] != kind:
                raise NotFound("资源类型不匹配")
            return row
        if version is not None:
            return await self.storage.assets.get(kind, identifier, str(version))
        return await self.storage.assets.find_asset(kind, identifier)

    async def explain(
        self,
        user,
        resource_type,
        resource_id,
        view,
        *,
        version=None,
        agent_id=None,
        team_id=None,
        review_id=None,
        grant_id=None,
        root_grant_id=None,
        allow_pinned=False,
        _parent_grant_id=None,
    ) -> PermissionDecision:
        """返回可用的授权路径，并选择此次使用的确切版本。"""
        if resource_type == "mcp":
            return await self._connector_access(user, resource_id, view)
        resolve_dependency = root_grant_id is not None and version is None
        try:
            user = await UserService(self.storage).get(user.user_id)
            if root_grant_id is None and view == "shared":
                current = await self.storage.access.shared_version(
                    resource_type, resource_id
                )
                if current is None or (
                    version is not None and str(version) != current and not allow_pinned
                ):
                    return PermissionDecision(False)
                version = version if allow_pinned and version is not None else current
            elif (
                root_grant_id is None
                and view == "system"
                and version is not None
                and not allow_pinned
            ):
                current = await self._resource(resource_type, resource_id, None)
                if current is None or current["resource_version"] != str(version):
                    return PermissionDecision(False)
            target = (
                None
                if resolve_dependency
                else await self._resource(resource_type, resource_id, version)
            )
        except (Forbidden, NotFound):
            return PermissionDecision(False)
        if not resolve_dependency and not self._valid_view(target, view):
            return PermissionDecision(False)

        # 先检查平台默认与所有者权限，再匹配指定授权来源。
        node = {
            "resource_type": resource_type,
            "resource_id": resource_id,
            "resource_view_type": view,
        }
        intrinsic = self._intrinsic_source(user, target, view) if target else None
        if intrinsic and (intrinsic != "private-self" or root_grant_id is None):
            return PermissionDecision(
                True,
                [[{"source": intrinsic}, node]],
                resource_version=target["resource_version"],
            )

        grants = await self.storage.access.active_grants(
            user.user_id, user.group_ids, kind=resource_type, resource_id=resource_id
        )
        matches = [
            row
            for row in grants
            if self._matches_grant(
                row,
                node,
                agent_id=agent_id,
                team_id=team_id,
                review_id=review_id,
                grant_id=grant_id,
                root_grant_id=root_grant_id,
                parent_grant_id=_parent_grant_id,
            )
        ]
        if not matches:
            return PermissionDecision(False)
        if root_grant_id is not None and _parent_grant_id is None:
            root = await self._dependency_root(matches[0])
            versions = await self._dependency_versions(root, matches[0])
            if version is None and len(versions) == 1:
                version = next(iter(versions))
                target = await self._resource(resource_type, resource_id, version)
            if str(version) not in versions:
                return PermissionDecision(False)
        if not self._valid_view(target, view):
            return PermissionDecision(False)
        selected = matches[0]
        paths = [
            [
                {
                    "grant_id": row["id"],
                    "root_grant_id": row["root_grant_id"],
                    "parent_grant_id": row["parent_grant_id"],
                    "agent_id": row["agent_id"],
                    "team_id": row["team_id"],
                },
                {**node, "resource_version": target["resource_version"]},
            ]
            for row in matches
        ]
        return PermissionDecision(
            True,
            paths,
            review_id=selected["source_review_id"],
            grant_id=selected["id"] if root_grant_id is None else None,
            root_grant_id=root_grant_id or selected["id"],
            resource_version=target["resource_version"],
        )

    async def _connector_access(self, user, identifier, view):
        users = UserService(self.storage, self.policies)
        try:
            user = await users.get(user.user_id)
        except (Forbidden, NotFound):
            return PermissionDecision(False)
        group_id = users.company_group().group_id
        if view != "system" or group_id not in user.group_ids:
            return PermissionDecision(False)
        asset = await self.storage.assets.get("mcp", identifier, MCP_STORAGE_VERSION)
        if asset is None or not is_managed_connector(asset) or not asset["is_enabled"]:
            return PermissionDecision(False)
        grants = await self.storage.access.active_grants(
            user.user_id, [group_id], kind="mcp", resource_id=identifier
        )
        allowed = any(
            row["grant_source_type"] == "manual"
            and row["principal_type"] == "group"
            and row["principal_id"] == group_id
            and row["resource_view_type"] == "system"
            and row["resource_owner_type"] == "system"
            and row["resource_owner_id"] == "system"
            and row["root_grant_id"] is None
            for row in grants
        )
        return PermissionDecision(allowed)

    @staticmethod
    def _valid_view(target, view):
        return (
            target is not None
            and resource_available(target)
            and (view == "system") == (target["owner_type"] == "system")
        )

    def _intrinsic_source(self, user, target, view):
        if is_system_default_tool(target):
            return "system-default"
        if target.get("resource_type") == "tool":
            return None
        if view != "private" or target["owner_id"] != user.user_id:
            return None
        personal = target.get("agent_type") == "personal"
        policy = "allow_personal_self_use" if personal else "allow_private_self_use"
        return "private-self" if getattr(self.policies, policy, True) else None

    @staticmethod
    def _matches_grant(
        row,
        node,
        *,
        agent_id,
        team_id,
        review_id,
        grant_id,
        root_grant_id,
        parent_grant_id,
    ):
        if any(row[key] != value for key, value in node.items()):
            return False
        if root_grant_id is not None:
            return (
                row["root_grant_id"] == root_grant_id
                and row["agent_id"] == agent_id
                and row["team_id"] == team_id
                and (
                    parent_grant_id is None or row["parent_grant_id"] == parent_grant_id
                )
            )
        return (
            row["root_grant_id"] is None
            and row["grant_source_type"] != "owner"
            and (review_id is None or row["source_review_id"] == review_id)
            and (grant_id is None or row["id"] == grant_id)
        )

    async def require(self, user, resource_type, resource_id, view, **context):
        """使用路径缺失时中止读取或执行。"""
        decision = await self.explain(user, resource_type, resource_id, view, **context)
        if not decision.is_allowed:
            raise Forbidden("未获得此资源的使用权限")
        return decision

    async def _dependency_root(self, dependency):
        root_id = dependency["root_grant_id"]
        identifier = dependency["team_id"] or dependency["agent_id"]
        root = await self.storage.access.active_grant(root_id, identifier)
        if root is None:
            return None
        try:
            return {
                **await self.storage.definitions.get_definition_view(
                    identifier, root["resource_view_type"], grant_id=root_id
                ),
                "resource_view_type": root["resource_view_type"],
            }
        except (Forbidden, NotFound):
            return None

    async def _dependency_versions(self, root, row):
        """从根定义的固定引用解析派生路径允许的资源版本。"""
        from egis_agentkit.service.team._members import team_member_definitions

        if root is None:
            return set()
        if root["definition_type"] == "team":
            try:
                member = (await team_member_definitions(self.storage, root)).get(
                    row["agent_id"]
                )
            except (Forbidden, NotFound):
                return set()
        else:
            member = root if root["id"] == row["agent_id"] else None
        if member is None:
            return set()
        if row["resource_type"] == "agent":
            return (
                {member["resource_version"]}
                if member["id"] == row["resource_id"]
                and member["resource_view_type"] == row["resource_view_type"]
                else set()
            )
        return {
            ref["resource_version"]
            for ref in member["payload"].get("bindings", [])
            if all(
                row[field] == ref[field]
                for field in ("resource_type", "resource_id", "resource_view_type")
            )
        }

    async def require_execution(self, user, root, *, allow_pinned=False):
        """校验根授权及定义中的固定成员和资源绑定。"""
        from egis_agentkit.service.team._members import team_member_definitions

        root_grant = await self.require(
            user,
            root["definition_type"],
            root["id"],
            root["resource_view_type"],
            version=root["resource_version"],
            grant_id=root.get("access_grant_id"),
            review_id=root.get("shared_review_id"),
            allow_pinned=allow_pinned,
        )
        if root["definition_type"] == "agent":
            await self._require_bindings(
                user, root, root_grant, allow_pinned=allow_pinned
            )
            return
        team_id = root["id"]
        for member in (await team_member_definitions(self.storage, root)).values():
            member_grant = await self.require(
                user,
                "agent",
                member["id"],
                member["resource_view_type"],
                version=member["resource_version"],
                allow_pinned=allow_pinned,
                root_grant_id=root_grant.root_grant_id,
                _parent_grant_id=root_grant.grant_id,
                agent_id=member["id"],
                team_id=team_id,
            )
            await self._require_bindings(
                user,
                member,
                member_grant,
                team_id=team_id if root_grant.root_grant_id else None,
                allow_pinned=allow_pinned,
            )

    async def _require_bindings(
        self, user, agent, grant, *, allow_pinned, team_id=None
    ):
        parent_id = grant.paths[0][0]["grant_id"] if grant.root_grant_id else None
        for binding in agent["payload"].get("bindings", []):
            if binding["resource_type"] == "skill":
                await self.require(
                    user,
                    binding["resource_type"],
                    binding["resource_id"],
                    binding["resource_view_type"],
                    version=binding["resource_version"],
                    allow_pinned=allow_pinned,
                    root_grant_id=grant.root_grant_id,
                    _parent_grant_id=parent_id,
                    agent_id=agent["id"],
                    team_id=team_id,
                )
