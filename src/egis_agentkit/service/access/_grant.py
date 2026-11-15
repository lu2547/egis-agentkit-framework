"""管理资源发布后的归属、直接授权和派生授权。

核心能力
- 记录资源的个人归属。
- 发放直接授权，并为每条入口生成独立的依赖授权。
- 按入口撤销授权，保留其他使用来源。
"""

from packaging.version import Version

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service._user import require_admin
from egis_agentkit.service.access._permission import (
    DEFINITION_KINDS,
    INTERNAL_AGENT_TYPES,
    MANAGED_ASSET_KINDS,
)
from egis_agentkit.storage.model import Grant, ShareTarget, User, new_id
from egis_agentkit.storage.model._base import publication_label, resource_available


class GrantService:
    """维护资源授权记录和来源关系。"""

    def __init__(self, storage):
        self.storage = storage

    async def require_recipient(self, target: ShareTarget, *, write):
        """确认指定接收用户或组织组仍有效。"""
        if target.principal_type == "user":
            principal = await write.users.get_user(target.principal_id)
            if principal is None or not principal["is_enabled"]:
                raise NotFound("授权用户不存在或已停用")
        elif not await write.users.enabled_group_ids([target.principal_id]):
            raise NotFound("授权分组不存在或已停用")

    async def list_grants(self, admin):
        """读取直接和派生授权的审计记录。"""
        require_admin(admin)
        return await self.storage.access.list_grants()

    async def grantable_resources(self, admin):
        """列出每项资源可直接授权的当前版本。"""
        require_admin(admin)
        resources = []
        for kind in DEFINITION_KINDS:
            rows = await self.storage.definitions.list_grantable_versions(
                kind, excluded_agent_types=INTERNAL_AGENT_TYPES
            )
            for row in rows:
                resources.append(
                    self._grantable_row(
                        kind, row["id"], row, row[f"{kind}_display_name"]
                    )
                )
        for kind in MANAGED_ASSET_KINDS:
            rows = await self.storage.assets.list_versions(kind)
            selected = {}
            for row in rows:
                identifier = row["resource_id"]
                previous = selected.get(identifier)
                if previous is None or self._publication_order(
                    row
                ) > self._publication_order(previous):
                    selected[identifier] = row
            for identifier, row in sorted(selected.items()):
                if row["is_enabled"]:
                    resources.append(
                        self._grantable_row(
                            kind,
                            identifier,
                            row,
                            row["payload"].get("display_name") or identifier,
                        )
                    )
        return resources

    @staticmethod
    def _publication_order(row):
        return (
            row["is_shared_current"],
            row["publication_revision"] or 0,
            Version(row["resource_version"]),
        )

    @staticmethod
    def _grantable_row(kind, identifier, row, name):
        revision = (
            row.get("version")
            or row.get("config_version")
            or row.get("publication_revision")
        )
        return {
            "resource_type": kind,
            "resource_id": identifier,
            "resource_version": row["resource_version"],
            "version_label": publication_label(revision)
            if revision
            else row["resource_version"],
            "resource_view_type": "system"
            if row["owner_type"] == "system"
            else "shared",
            "resource_owner_type": row["owner_type"],
            "resource_owner_id": row["owner_id"],
            "display_name": name,
        }

    async def grant(self, admin: User, data: Grant, *, write=None):
        """立即发放授权，现有共享版优先于私有最新配置。"""
        require_admin(admin)
        if write is None:
            async with self.storage.definition_write() as transaction:
                return await self.grant(admin, data, write=transaction)
        resource = await self.grant_version(
            data.resource_type, data.resource_id, write=write
        )
        return await self.issue(admin, data, resource, write=write)

    async def issue(self, admin, data, resource, *, write):
        """提交已选择的共享版本和该接收方的授权路径。"""
        await self.require_recipient(data, write=write)
        self._validate_grant_resource(data, resource)
        kind, identifier = data.resource_type, data.resource_id
        version = resource["resource_version"]
        if (
            data.resource_view_type == "shared"
            and not await write.access.shared_version(kind, identifier)
        ):
            await write.access.set_shared_version(kind, identifier, version)
        root = await write.access.upsert_grant(
            {**data.model_dump(), "origin_key": data.grant_source_type},
            actor=admin.user_id,
        )
        if kind in DEFINITION_KINDS:
            await self._grant_dependencies(
                root, resource, actor=admin.user_id, write=write
            )
        return root

    async def grant_version(self, kind, identifier, *, write):
        """选择已共享版本，首次授权选择所有者最新版本。"""
        shared = await write.access.shared_version(kind, identifier)
        return await self.read_version(kind, identifier, shared, write=write)

    async def read_version(self, kind, identifier, version, *, write):
        """从资源存储读取指定版本或所有者最新版本。"""
        if kind in DEFINITION_KINDS:
            row = await write.definitions.get_definition(identifier, version=version)
            if row["definition_type"] != kind:
                raise NotFound("资源类型不匹配")
            return row
        if version is None:
            return await write.assets.find_asset(kind, identifier)
        return await write.assets.get(kind, identifier, str(version))

    @staticmethod
    def _validate_grant_resource(data, resource):
        if resource is None:
            raise NotFound()
        if not resource_available(resource):
            raise Conflict("源资源已经停用")
        if (resource["owner_type"], resource["owner_id"]) != (
            data.resource_owner_type,
            data.resource_owner_id,
        ):
            raise Forbidden("授权目标归属不匹配")
        if (data.resource_view_type == "system") != (
            resource["owner_type"] == "system"
        ):
            raise Forbidden("授权资源视图与归属不匹配")
        if resource.get("agent_type") in INTERNAL_AGENT_TYPES:
            raise Forbidden("该资源不能单独授权")

    async def _grant_dependencies(self, root, definition, *, actor, write):
        from egis_agentkit.service.team._members import team_member_definitions

        team_id = definition["id"] if definition["definition_type"] == "team" else None
        members = (
            (await team_member_definitions(write, definition)).values()
            if team_id
            else [definition]
        )
        retained = set()
        for member in members:
            parent_id = root["id"]
            agent_id = member["id"]
            if team_id:
                parent_id = await self._grant_dependency(
                    root,
                    "agent",
                    agent_id,
                    member["resource_view_type"],
                    member,
                    parent_id=parent_id,
                    agent_id=agent_id,
                    team_id=team_id,
                    actor=actor,
                    write=write,
                )
                retained.add(parent_id)
            for binding in member["payload"].get("bindings", []):
                kind = binding["resource_type"]
                if kind == "tool":
                    continue
                identifier = binding["resource_id"]
                asset = await write.assets.get(
                    kind, identifier, binding["resource_version"]
                )
                if asset is None or not asset["is_enabled"]:
                    raise NotFound("绑定资源指定版本不存在或已停用")
                retained.add(
                    await self._grant_dependency(
                        root,
                        kind,
                        identifier,
                        binding["resource_view_type"],
                        asset,
                        parent_id=parent_id,
                        agent_id=agent_id,
                        team_id=team_id,
                        actor=actor,
                        write=write,
                    )
                )
        await write.access.revoke_dependencies(root["id"], retained, actor=actor)

    @staticmethod
    async def _grant_dependency(
        root,
        kind,
        identifier,
        view,
        data,
        *,
        parent_id,
        agent_id,
        team_id,
        actor,
        write,
    ):
        dependency = await write.access.upsert_grant(
            {
                "id": new_id(),
                "resource_type": kind,
                "resource_id": identifier,
                "resource_view_type": view,
                "resource_owner_type": data["owner_type"],
                "resource_owner_id": data["owner_id"],
                "agent_id": agent_id,
                "team_id": team_id,
                "grant_source_type": "dependency",
                "source_review_id": root["source_review_id"],
                "root_grant_id": root["id"],
                "parent_grant_id": parent_id,
                "origin_key": parent_id,
                "principal_type": root["principal_type"],
                "principal_id": root["principal_id"],
            },
            actor=actor,
        )
        return dependency["id"]

    async def refresh_dependencies(self, definition, *, actor, write):
        """按现有根授权更新派生路径，保持接收方和入口 ID。"""
        kind = definition["definition_type"]
        if kind not in DEFINITION_KINDS:
            return
        for root in await write.access.direct_grants(kind, definition["id"]):
            await self._grant_dependencies(root, definition, actor=actor, write=write)

    async def record_owner(self, kind, identifier, resource, *, actor):
        """登记资源的个人归属授权。"""
        if (
            resource["owner_type"] != "user"
            or resource.get("agent_type") in INTERNAL_AGENT_TYPES
        ):
            return
        owner_id = resource["owner_id"]
        await self.storage.access.upsert_grant(
            {
                "id": new_id(),
                "principal_type": "user",
                "principal_id": owner_id,
                "resource_type": kind,
                "resource_id": identifier,
                "resource_view_type": "private",
                "resource_owner_type": "user",
                "resource_owner_id": owner_id,
                "grant_source_type": "owner",
                "origin_key": "owner",
            },
            actor=actor,
        )

    async def record_definition_owner(self, definition, *, actor):
        """登记所有者或同步系统配置变更后的授权路径。"""
        if definition["owner_type"] == "system":
            await self.refresh_dependencies(definition, actor=actor, write=self.storage)
            return
        await self.record_owner(
            definition["definition_type"],
            definition["id"],
            definition,
            actor=actor,
        )

    async def retire_definition_grants(self, identifier, *, actor, previous):
        """撤销停用定义及其所有版本的授权。"""
        await self.storage.access.revoke_resource(
            previous["definition_type"], identifier, actor=actor
        )

    async def revoke(self, admin, grant_id):
        """撤销指定入口及其派生授权，保留其他授权路径。"""
        require_admin(admin)
        async with self.storage.definition_write() as write:
            root = await write.access.revoke_grant(grant_id, actor=admin.user_id)
            if root is None:
                raise NotFound()
