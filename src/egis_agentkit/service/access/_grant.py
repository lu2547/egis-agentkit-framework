"""管理资源发布后的归属、直接授权和派生授权。

核心能力
- 记录资源的个人归属。
- 发放直接授权，并为每条入口生成独立的依赖授权。
- 按入口撤销授权，保留其他使用来源。
"""

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service._admin_page import AdminPage
from egis_agentkit.service._user import UserService, require_admin
from egis_agentkit.service.access._permission import (
    DEFINITION_KINDS,
    INTERNAL_AGENT_TYPES,
)
from egis_agentkit.storage.model import Grant, ShareTarget, User, new_id
from egis_agentkit.storage.model._base import publication_label, resource_available
from egis_agentkit.storage.model._resource import (
    MCP_STORAGE_VERSION,
    is_managed_connector,
    require_skill_publication_revision,
)


class GrantService:
    """维护资源授权记录和来源关系。"""

    def __init__(self, storage, policies=None):
        self.storage = storage
        self.policies = policies

    async def require_recipient(self, target: ShareTarget, *, write):
        """确认指定接收用户或组织组仍有效。"""
        if target.principal_type == "user":
            principal = await write.users.get_user(target.principal_id)
            if principal is None or not principal["is_enabled"]:
                raise NotFound("授权用户不存在或已停用")
        elif not await write.users.enabled_group_ids([target.principal_id]):
            raise NotFound("授权分组不存在或已停用")

    @staticmethod
    def require_loaded_recipient(target, enabled):
        if not enabled:
            message = (
                "授权用户不存在或已停用"
                if target.principal_type == "user"
                else "授权分组不存在或已停用"
            )
            raise NotFound(message)

    async def list_grants(self, admin):
        """读取直接和派生授权的审计记录。"""
        require_admin(admin)
        return await self.storage.access.list_grants()

    async def list_grants_page(self, admin: User, page: AdminPage) -> dict:
        require_admin(admin)
        rows = await self.storage.access.list_grants(
            offset=page.offset, limit=page.fetch_limit
        )
        return page.response(rows)

    async def grantable_resources(self, admin, *, include_skills=False):
        """列出可直接授权的系统定义，技能选择使用分页目录。"""
        require_admin(admin)
        resources = []
        for kind in DEFINITION_KINDS:
            rows = await self.storage.definitions.list_grantable_versions(
                kind, excluded_agent_types=INTERNAL_AGENT_TYPES
            )
            for row in rows:
                if row["owner_type"] != "system":
                    continue
                resources.append(
                    self._grantable_row(
                        kind, row["id"], row, row[f"{kind}_display_name"]
                    )
                )
        skills = await self.storage.assets.list_current_skill_versions() if include_skills else []
        for row in sorted(skills, key=lambda skill: skill["resource_id"]):
            if row["owner_type"] != "system":
                continue
            version = row["shared_version"] or row["owner_version"]
            if row["publication_revision"] != version or not row["is_enabled"]:
                continue
            identifier = row["resource_id"]
            resources.append(
                self._grantable_row(
                    "skill",
                    identifier,
                    row,
                    row["payload"].get("display_name") or identifier,
                )
            )
        return resources

    @staticmethod
    def _grantable_row(kind, identifier, row, name):
        revision = (
            require_skill_publication_revision(row["publication_revision"])
            if kind == "skill"
            else row["version"]
        )
        return {
            "resource_type": kind,
            "resource_id": identifier,
            "resource_version": row["resource_version"],
            "version_label": publication_label(revision),
            "resource_view_type": "system"
            if row["owner_type"] == "system"
            else "shared",
            "resource_owner_type": row["owner_type"],
            "resource_owner_id": row["owner_id"],
            "display_name": name,
        }

    async def grant(self, admin: User, data: Grant, *, write=None):
        """立即发放系统资源授权，个人共享由审核入口发放。"""
        require_admin(admin)
        if data.resource_owner_type != "system" or data.resource_view_type != "system":
            raise Forbidden("个人资源请提交共享申请并由其他管理员审核")
        if write is None:
            async with self.storage.definition_write() as transaction:
                return await self.grant(admin, data, write=transaction)
        if data.resource_type == "mcp":
            company = UserService(self.storage, self.policies).company_group()
            if (
                data.principal_type != "group"
                or data.principal_id != company.group_id
                or data.grant_source_type != "manual"
                or data.resource_view_type != "system"
            ):
                raise Forbidden("MCP 只开放给公司组")
            await write.assets.lock_resource("mcp", data.resource_id)
            resource = await write.assets.get(
                "mcp", data.resource_id, MCP_STORAGE_VERSION
            )
            if resource is None or not is_managed_connector(resource):
                raise NotFound()
        elif data.resource_type == "skill":
            resource = await write.access.skill_write_state(
                data.resource_id, target=data
            )
        else:
            resource = await self.grant_version(
                data.resource_type, data.resource_id, write=write
            )
        return await self.issue(admin, data, resource, write=write)

    async def issue(self, admin, data, resource, *, write):
        """提交已选择的共享版本和该接收方的授权路径。"""
        if resource is not None and "_recipient_enabled" in resource:
            self.require_loaded_recipient(data, resource["_recipient_enabled"])
        else:
            await self.require_recipient(data, write=write)
        self._validate_grant_resource(data, resource)
        kind, identifier = data.resource_type, data.resource_id
        version = resource["resource_version"]
        shared = (
            (
                resource["_shared_resource_version"]
                if "_shared_resource_version" in resource
                else await write.access.shared_version(kind, identifier)
            )
            if data.resource_view_type == "shared"
            else None
        )
        if data.resource_view_type == "shared" and shared is None:
            await write.access.set_shared_version(kind, identifier, version)
        root = await self._record_direct(data, actor=admin.user_id, write=write)
        if kind in DEFINITION_KINDS:
            await self._grant_dependencies(
                root, resource, actor=admin.user_id, write=write
            )
        return root

    async def publish_connector(self, identifier, company, *, actor, write):
        """为管理员维护的 MCP 登记公司组入口，由组与连接器状态决定可见性。"""
        await write.users.ensure_group(company, actor=actor)
        data = Grant(
            principal_type="group",
            principal_id=company.group_id,
            resource_type="mcp",
            resource_id=identifier,
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="system",
        )
        return await self._record_direct(data, actor=actor, write=write)

    @staticmethod
    async def _record_direct(data, *, actor, write):
        return await write.access.upsert_grant(
            {**data.model_dump(), "origin_key": data.grant_source_type}, actor=actor
        )

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
        await self._grant_dependencies_many(
            [root], definition, actor=actor, write=write
        )

    @staticmethod
    def _dependency_values(
        root, kind, identifier, view, data, *, parent_id, agent_id, team_id
    ):
        return {
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
        }

    async def definition_dependencies(self, definition, *, write):
        """读取审批和派生授权共同使用的固定成员及技能版本。"""
        from egis_agentkit.storage._authorization import resource_rows
        from egis_agentkit.storage._definition import (
            build_definition_data,
            definition_view_selection,
        )

        team_id = definition["id"] if definition["definition_type"] == "team" else None
        members = [definition]
        if team_id:
            refs = (
                definition["payload"]["orchestrator"],
                *definition["payload"]["members"],
            )
            loaded = await write.definitions.get_many(
                {
                    ("agent", ref["definition_id"], ref["resource_version"])
                    for ref in refs
                }
            )
            members = []
            for ref in refs:
                row = loaded.get(
                    ("agent", ref["definition_id"], str(ref["resource_version"]))
                )
                if row is None:
                    raise NotFound("绑定专家版本不存在")
                definition_view_selection(
                    row,
                    ref["resource_view_type"],
                    version=ref["resource_version"],
                    dependency=True,
                )
                members.append(
                    {
                        **build_definition_data(row),
                        "resource_view_type": ref["resource_view_type"],
                    }
                )
        references = {
            ("skill", ref["resource_id"], ref["resource_version"])
            for member in members
            for ref in member["payload"].get("bindings", [])
            if ref["resource_type"] == "skill"
        }
        assets = {
            (row["resource_id"], row["resource_version"]): row
            for row in await resource_rows(write.engine, references)
        }
        for _, identifier, version in references:
            asset = assets.get((identifier, version))
            if asset is None or not asset["is_enabled"]:
                raise NotFound("绑定资源指定版本不存在或已停用")
        return members, assets

    async def _grant_dependencies_many(self, roots, definition, *, actor, write):
        if not roots:
            return
        team_id = definition["id"] if definition["definition_type"] == "team" else None
        members, assets = await self.definition_dependencies(definition, write=write)
        retained = {root["id"]: set() for root in roots}
        parents = {}
        if team_id:
            layer = [
                self._dependency_values(
                    root,
                    "agent",
                    member["id"],
                    member["resource_view_type"],
                    member,
                    parent_id=root["id"],
                    agent_id=member["id"],
                    team_id=team_id,
                )
                for root in roots
                for member in members
            ]
            for row in await write.access.upsert_grants(layer, actor=actor):
                parents[row["root_grant_id"], row["agent_id"]] = row["id"]
                retained[row["root_grant_id"]].add(row["id"])
        layer = []
        for root in roots:
            for member in members:
                parent_id = parents[root["id"], member["id"]] if team_id else root["id"]
                for ref in member["payload"].get("bindings", []):
                    if ref["resource_type"] != "skill":
                        continue
                    asset = assets[ref["resource_id"], ref["resource_version"]]
                    layer.append(
                        self._dependency_values(
                            root,
                            "skill",
                            ref["resource_id"],
                            ref["resource_view_type"],
                            asset,
                            parent_id=parent_id,
                            agent_id=member["id"],
                            team_id=team_id,
                        )
                    )
        for row in await write.access.upsert_grants(layer, actor=actor):
            retained[row["root_grant_id"]].add(row["id"])
        await write.access.revoke_dependencies_many(retained, actor=actor)

    async def refresh_dependencies(self, definition, *, actor, write):
        """按现有根授权更新派生路径，保持接收方和入口 ID。"""
        kind = definition["definition_type"]
        if kind not in DEFINITION_KINDS:
            return
        roots = await write.access.direct_grants(kind, definition["id"])
        await self._grant_dependencies_many(roots, definition, actor=actor, write=write)

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
