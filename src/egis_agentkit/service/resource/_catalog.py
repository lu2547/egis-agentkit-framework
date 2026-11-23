"""管理技能、连接器和工具资源，向会话提供已经授权的运行声明。

核心能力
- 维护资源目录、发布版本和个人安装开关。
- 读取技能版本文件，解析连接器配置和认证凭据。
- prepare_workspace 在工具集收集前装载授权结果，工具执行前再次刷新。
- 资源归属和授权仍由权限服务校验，Workspace 只接收装载结果。
"""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import logging
import re
from contextlib import aclosing
from functools import partial
from uuid import uuid4

from agentscope.app._service._mcp_render import render_mcp
from agentscope.skill import Skill

from egis_agentkit.client._mcp_config import _verify_mcp_card
from egis_agentkit.client.mcp._gateway_mcp_client_override import credential_fingerprint
from egis_agentkit.client.nacos._settings import (
    DEFAULT_SKILL_ARCHIVE_MAX_BYTES,
    CatalogPaginationConfig,
    SkillArchiveConfig,
)
from egis_agentkit.core.mcp import MCPAuthorization
from egis_agentkit.core.skill._archive import (
    build_skill_archive,
    calculate_files_digest,
    read_archive_files,
)
from egis_agentkit.core.skill._avatar import AUTO_AVATAR
from egis_agentkit.errors import (
    AgentKitError,
    BadRequest,
    Conflict,
    Forbidden,
    InvalidRequest,
    NotFound,
)
from egis_agentkit.service._user import UserService, require_admin
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import (
    PermissionService,
    is_system_default_tool,
)
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.storage.model import (
    Asset,
    ResourceRef,
    SkillActivation,
    User,
)
from egis_agentkit.storage.model._base import publication_label
from egis_agentkit.storage.model._resource import (
    MCP_STORAGE_VERSION,
    asset_version_order,
    is_managed_connector,
    require_skill_publication_revision,
    validate_resource_id,
)
from egis_agentkit.storage.skill._version import SkillVersionStorage

from ._connector import AuthorizedMCPClient, LocalMCPRegistry
from ._connector_manager import ConnectorManager
from ._skill import LocalSkillRegistry, _skill_front, decode_skill_markdown
from ._skill_avatar import generate_skill_avatar
from ._tool import FunctionToolRegistry

__all__ = ["SkillActivation"]
logger = logging.getLogger(__name__)

TOOL_CATEGORIES = ("系统默认", "可选工具", "专家工具")


EXPERT_ICON_NAMES = (
    "developer",
    "architect",
    "founder",
    "embedded",
    "workbench",
    "wechat",
    "academic",
    "research",
    "knowledge",
    "fullstack",
    "python",
    "godot",
)
SKILL_ICON_NAMES = (
    "tencent-finance",
    "minimax-xlsx",
    "ai-news",
    "ppt",
    "web-access",
    "legal-search",
    "kdocs",
    "deep-thinking",
    "wecom",
    "minimax-docx",
    "tencent-pdfkit",
    "neodata",
    "minimax-h3",
    "dingtalk",
    "browser-skill",
)
CATALOG_ICON_IDS = frozenset(
    [
        *("expert:" + name for name in EXPERT_ICON_NAMES),
        *("skill:" + name for name in SKILL_ICON_NAMES),
        "spark",
        "book",
        "team",
        "code",
    ]
)


def catalog_icon_id(value, *, default="book"):
    if value is None:
        return default
    if not isinstance(value, str) or value not in CATALOG_ICON_IDS:
        raise BadRequest("请选择标准图标库中的图标")
    return value


def normalize_skill_avatar(value):
    if value is None or value == AUTO_AVATAR:
        return AUTO_AVATAR
    return catalog_icon_id(value)


class ResourceService:
    """管理 Skill 和 MCP 的版本、使用权限和安装，以及工具目录。"""

    def __init__(
        self,
        storage,
        workspace_root,
        *,
        policies=None,
        credential_key=None,
        config=None,
    ):
        from egis_agentkit.service.resource._hubs import (
            LocalMCPHub,
            LocalSkillHub,
        )

        self.storage = storage
        self.policies = policies
        self.config = config
        self.permissions = PermissionService(storage, policies)
        self.grants = GrantService(storage, policies)
        self.skill_versions = SkillVersionStorage(
            workspace_root, config_provider=config
        )
        self.workspace_root = self.skill_versions.root
        self.skill_registry = LocalSkillRegistry(storage.assets, self.skill_versions)
        self.mcp_registry = LocalMCPRegistry(storage.assets)
        self.connectors = ConnectorManager(storage, policies)
        self.skill_hub = LocalSkillHub(self)
        self.mcp_hub = LocalMCPHub(self)
        self.mcp_authorization = MCPAuthorization(
            storage.mcp_credentials, key=credential_key, config=config
        )
        self.tool_registry = FunctionToolRegistry(self)
        self.personal = PersonalManager(
            storage, workspace_root=workspace_root, config=config
        )
        self.tool_registry.bind_default_configuration()

    async def prepare_workspace(self, user, scope, workspace):
        """在工具集收集前及每次工具执行前刷新当前会话的工具与连接器授权。"""
        if scope is None or (
            scope.user_id,
            scope.project_id,
            scope.agent_id,
            scope.session_id,
        ) != (
            workspace.egis_user_id,
            workspace.egis_project_id,
            workspace.egis_agent_id,
            workspace.egis_session_id,
        ):
            raise Forbidden("资源装配缺少一致的会话上下文")
        if user.user_id != scope.user_id:
            raise Forbidden("资源装配用户与会话不一致")
        user = await UserService(self.storage).get(user.user_id)
        # 1. 个人助理使用当前安装项，专家使用定义中的固定绑定。
        refs = scope.definition["payload"].get("bindings", [])
        installations = None
        if scope.definition["agent_type"] == "personal":
            installations = await self.personal.list_resources(user, "mcp")
            refs = [
                {"resource_type": "mcp", "resource_id": rid}
                for rid, row in installations.items()
                if row["is_enabled"]
            ]
        # 2. 重新解析连接器授权和凭据；不可用连接器不进入本轮工具集。
        mcps = await self.mcps_for_refs(
            user,
            refs,
            authorization_scope=scope,
            skip_unavailable=True,
            installations=installations,
            refresh_user=False,
        )
        # 3. 筛选默认工具后交给 Core 装载，Core 不承担权限查询。
        tools = await self.tool_registry.authorize_many(
            user, scope, await workspace.get_builtin_tools()
        )
        workspace.configure_workspace_resources(tools=tools, mcps=mcps)
        await workspace.list_mcps(agent_id=scope.agent_id, session_id=scope.session_id)

    async def selected_for_personal(
        self, user, resource_type, rows=None, *, enabled_only=False, resource_ids=None
    ):
        """将个人资源开关与当前独立授权结合，只在使用时解析资源版本。"""
        settings = await self.personal.list_resources(user, resource_type)
        identifiers = {
            identifier
            for identifier, row in settings.items()
            if (not enabled_only or row["is_enabled"])
            and (resource_ids is None or identifier in resource_ids)
        }
        if not identifiers:
            return {}
        if rows is None:
            rows = await self._current_versions(resource_type, resource_ids=identifiers)
        return await self._latest_visible_assets(
            user,
            resource_type,
            (row for row in rows if row["resource_id"] in identifiers),
            usable_only=True,
        )

    async def _personal_asset(self, user, kind, identifier, *, require_available=True):
        """Resolve and authorize the selected version once for this operation."""
        if kind not in ("skill", "mcp", "tool"):
            raise Forbidden("此资源不能由个人助理选择")
        versions = await self._current_versions(kind, resource_ids=[identifier])
        if kind == "tool":
            versions = [
                asset for asset in versions if self.tool_registry.is_optional_tool(asset)
            ]
        snapshot = await self._read_snapshot(user, versions)
        selected = await self._latest_visible_assets(
            snapshot.user, kind, versions, usable_only=True, snapshot=snapshot
        )
        asset = selected.get(identifier)
        if asset is None:
            raise Forbidden("未获得此资源的独立使用权限")
        if (
            kind == "tool"
            and require_available
            and not await self._usable(snapshot.user, asset, snapshot=snapshot)
        ):
            raise Forbidden("可选工具尚未开放或配置不完整")
        return asset, snapshot

    async def install(self, user, kind, identifier):
        """安装由资源管理协调，只保存个人启用开关，不复制原生安装记录。"""
        asset, snapshot = await self._personal_asset(user, kind, identifier)
        user = snapshot.user
        if kind == "mcp":
            self._verified_mcp_card(asset)
            await self.mcp_authorization.headers(user, asset)
        installation = await self.personal.add_resource(user, kind, identifier)
        return await self._view(
            user, asset, snapshot=snapshot, installations={identifier: installation}
        )

    async def update_installation(self, user, kind, identifier, enabled):
        """调整已有个人选择，组授权及其他用户的开关保持独立。"""
        asset, snapshot = await self._personal_asset(
            user, kind, identifier, require_available=enabled
        )
        user = snapshot.user
        if kind == "mcp" and enabled:
            self._verified_mcp_card(asset)
            await self.mcp_authorization.headers(user, asset)
        installation = await self.personal.update_resource_enabled(
            user, kind, identifier, enabled
        )
        return await self._view(
            user, asset, snapshot=snapshot, installations={identifier: installation}
        )

    async def remove_installation(self, user: User, kind: str, identifier: str) -> None:
        """取消个人选用；即使资源授权已经撤销，也允许清理自己的关联。"""
        await self.personal.remove_resource(user, kind, identifier)

    async def seed_builtin_tools(self):
        await self.tool_registry.seed()

    def _registry(self, kind):
        if kind == "skill":
            return self.skill_registry
        if kind == "mcp":
            return self.mcp_registry
        return self.tool_registry

    async def _versions(self, kind, **kwargs):
        rows = await self._registry(kind).list(**kwargs)
        return (
            [row for row in rows if is_managed_connector(row)]
            if kind == "mcp"
            else rows
        )

    async def _current_versions(self, kind, *, resource_ids=None):
        """只读取技能的所有者和共享版本，供目录及个人装配选择。"""
        if kind == "skill":
            return await self.storage.assets.list_current_skill_versions(
                resource_ids=resource_ids
            )
        return await self._versions(kind, resource_ids=resource_ids)

    async def hub_list(self, user_id, kind):
        user = await UserService(self.storage).get(user_id)
        rows = await self._current_versions(kind)
        latest = await self._latest_visible_assets(user, kind, rows, usable_only=True)
        return sorted(latest.values(), key=lambda row: row["resource_id"])

    async def hub_asset(self, user_id, kind, rid, version):
        user = await UserService(self.storage).get(user_id)
        if version is None:
            rows = await self._current_versions(kind, resource_ids=[rid])
            selected = await self._latest_visible_assets(
                user, kind, rows, usable_only=True
            )
            if rid not in selected:
                raise NotFound()
            return selected[rid]
        asset = await self._asset(kind, rid, version)
        await self._require(user, asset)
        return asset

    async def delete_skill(self, user: User, skill_id: str) -> None:
        """所有者删除技能，删除状态、选用和授权在同一事务提交。"""
        self._validate_identifier(skill_id)
        if not user.is_enabled:
            raise Forbidden()
        async with self.storage.definition_write() as write:
            await write.assets.lock_resource("skill", skill_id)
            skill = await write.assets.find_asset("skill", skill_id)
            if skill is None:
                raise NotFound("技能不存在")
            if (skill["owner_type"], skill["owner_id"]) != ("user", user.user_id):
                raise Forbidden("只能删除自己创建的个人技能")
            await write.assets.delete_skill(skill_id, actor=user.user_id)
            await write.access.revoke_resource("skill", skill_id, actor=user.user_id)
            await write.access.cancel_pending_reviews("skill", skill_id)

    async def set_asset_enabled(
        self, user, kind, rid, version, enabled, *, admin=False
    ):
        """同步更新版本状态和对应授权，保留其他资源版本。"""
        if admin or kind == "mcp":
            require_admin(user)
        async with self.storage.definition_write() as write:
            await write.assets.lock_resource(kind, rid)
            asset = await self._asset(kind, rid, version, assets=write.assets)
            if not admin and not self._editable(user, asset):
                raise Forbidden()
            if not enabled and is_system_default_tool(asset):
                raise Conflict("系统默认工具不能停用")
            await write.assets.set_enabled(
                kind, rid, asset["resource_version"], enabled, actor=user.user_id
            )
            if kind == "mcp" and enabled:
                company = UserService(self.storage, self.policies).company_group()
                await GrantService(write).publish_connector(
                    rid, company, actor=user.user_id, write=write
                )
            if kind == "skill":
                if enabled:
                    await GrantService(write).record_owner(
                        kind, rid, asset, actor=user.user_id
                    )
                else:
                    await write.access.revoke_resource(
                        kind, rid, actor=user.user_id, version=version
                    )
        updated = {**asset, "is_enabled": enabled}
        return (
            await self._admin_view(user, updated)
            if admin
            else await self._view(user, updated)
        )

    async def _admin_view(
        self,
        user,
        asset,
        *,
        bindings=None,
        grants=None,
        snapshot=None,
        installations=None,
        owner_names=None,
        credential_rows=None,
    ):
        view = await self._view(
            user,
            asset,
            admin=True,
            source=asset["resource_type"] == "mcp",
            snapshot=snapshot,
            installations=installations,
            owner_names=owner_names,
            credential_rows=credential_rows,
        )
        return {
            **view,
            "entrypoint": asset["entrypoint"],
            "bound_agents": bindings or [],
            "grant_sources": grants or [],
        }

    async def admin_list(self, user, kind, *, page=None):
        user = await UserService(self.storage).get(user.user_id)
        require_admin(user)
        self._kind(kind)
        if page is None:
            rows = sorted(
                await self._versions(kind),
                key=lambda row: (row["resource_id"], asset_version_order(row)),
            )
        else:
            rows = await self.storage.assets.page_admin_versions(
                kind, offset=page.offset, limit=page.fetch_limit
            )
            result = page.response(rows)
            rows = result["items"]
        items = await self._admin_assets(user, kind, rows)
        return items if page is None else {**result, "items": items}

    async def _admin_assets(self, user, kind, rows):
        snapshot = await self._read_snapshot(user, rows, refresh_user=False)
        installations, owners, credentials = await self._presentation(user, rows)
        identifiers = {row["resource_id"] for row in rows}
        references = [
            {
                "resource_type": kind,
                "resource_id": row["resource_id"],
                **(
                    {"resource_view_type": "system"}
                    if kind == "mcp"
                    else {"resource_version": row["resource_version"]}
                ),
            }
            for row in rows
        ]
        agents = await self.storage.definitions.agents.current_bindings(references)
        grants = (
            []
            if kind == "tool"
            else await self.storage.access.active_grants_for_resources(
                kind, identifiers
            )
        )
        grants_by_resource = {}
        for grant in grants:
            grants_by_resource.setdefault(grant["resource_id"], []).append(grant)
        return [
            await self._admin_view(
                user,
                row,
                bindings=[
                    {"id": agent["id"], "name": agent["agent_display_name"]}
                    for agent in agents
                    if any(
                        self._binding_matches(ref, row)
                        for ref in (agent["bindings"] or ())
                    )
                ],
                grants=grants_by_resource.get(row["resource_id"], []),
                snapshot=snapshot,
                installations=installations.get(kind),
                owner_names=owners,
                credential_rows=credentials,
            )
            for row in rows
        ]

    async def list_asset_versions(self, user, kind, rid):
        """列出此用户可访问的确切资源版本。"""
        self._kind(kind)
        candidates = await self._current_versions(kind, resource_ids=[rid])
        if kind == "skill" and any(self._editable(user, row) for row in candidates):
            candidates = await self._versions(kind, resource_ids=[rid])
        rows = sorted(
            candidates,
            key=asset_version_order,
            reverse=True,
        )
        snapshot = await self._read_snapshot(user, rows)
        user = snapshot.user
        installations, owners, credentials = await self._presentation(user, rows)
        result = []
        for asset in rows:
            visible = (
                self._tool_card_visible(user, asset)
                if kind == "tool"
                else self._editable(user, asset)
                or await self._usable(user, asset, snapshot=snapshot)
            )
            if visible:
                result.append(
                    await self._view(
                        user,
                        asset,
                        snapshot=snapshot,
                        installations=installations.get(kind),
                        owner_names=owners,
                        credential_rows=credentials,
                    )
                )
        return result

    @property
    def archive_limits(self) -> SkillArchiveConfig:
        if self.config is not None:
            return self.config.snapshot.skill.archive
        return SkillArchiveConfig(
            max_bytes=getattr(
                self.policies,
                "max_skill_archive_bytes",
                DEFAULT_SKILL_ARCHIVE_MAX_BYTES,
            )
        )

    @property
    def max_skill_archive_bytes(self) -> int:
        return self.archive_limits.max_bytes

    @staticmethod
    def _kind(kind):
        if kind not in {"skill", "mcp", "tool"}:
            raise BadRequest("Unknown resource type")
        return kind

    @staticmethod
    def _validate_identifier(rid):
        try:
            validate_resource_id(rid)
        except ValueError as exc:
            raise BadRequest("Invalid resource identifier") from exc

    @classmethod
    def _identity(cls, rid, version):
        cls._validate_identifier(rid)
        if not isinstance(version, str) or not re.fullmatch(
            r"\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?", version
        ):
            raise BadRequest("An exact resource version is required")

    @staticmethod
    def _runtime_id(user, kind, rid, version):
        identity = f"{user.user_id}\0{kind}\0{rid}"
        if kind != "mcp":
            identity += f"\0{version}"
        return hashlib.sha256(identity.encode()).hexdigest()

    def _runtime_name(self, user, kind, rid, version):
        return (
            re.sub("[^A-Za-z0-9_-]", "_", rid)[:40]
            + "_"
            + self._runtime_id(user, kind, rid, version)[:12]
        )

    async def _asset(self, kind, rid, version, *, assets=None):
        """从资产存储读取确切版本。"""
        self._kind(kind)
        if kind == "mcp":
            if version is not None:
                raise InvalidRequest("MCP 按 ID 查询，无须指定版本")
            version = MCP_STORAGE_VERSION
        self._identity(rid, version)
        row = await (assets or self.storage.assets).get(kind, rid, version)
        if row is None or (kind == "mcp" and not is_managed_connector(row)):
            raise NotFound()
        return row

    async def _shared_asset(self, user, asset):
        decision = await self.permissions.explain(
            user,
            asset["resource_type"],
            asset["resource_id"],
            "shared",
            version=asset["resource_version"],
        )
        return decision.is_allowed

    @staticmethod
    def _binding_matches(ref, asset):
        return (
            ref["resource_type"] == asset["resource_type"]
            and ref["resource_id"] == asset["resource_id"]
            and (
                (
                    ref.get("resource_version") is None
                    and ref.get("resource_view_type") == "system"
                )
                if asset["resource_type"] == "mcp"
                else ref.get("resource_version") == asset["resource_version"]
            )
        )

    @staticmethod
    def _scope_allows(user, asset, scope):
        """当前 Agent 的定义明确绑定该资源版本时才允许继续校验。"""
        if scope is None or scope.user_id != user.user_id:
            return False
        current = scope.definition
        root = scope.root_definition
        pinned = (
            root
            if root["definition_type"] == "agent"
            else scope.member_definitions.get(current["id"])
        )
        return pinned is not None and all(
            any(
                ResourceService._binding_matches(ref, asset)
                for ref in definition["payload"].get("bindings", [])
            )
            for definition in (pinned, current)
        )

    async def _usable(self, user, asset, scope=None, *, snapshot=None):
        """按独立授权或当前执行图校验版本使用权。"""
        if not user.is_enabled or not asset["is_enabled"]:
            return False
        if self.tool_registry.is_optional_tool(asset):
            if not self.tool_registry.optional_tool_available(asset, scope):
                return False
            if scope is None:
                return True
            if (
                scope.user_id != user.user_id
                or scope.definition["agent_type"] != "personal"
            ):
                return False
            selected = await self.personal.list_resources(user, "tool")
            return bool(selected.get(asset["resource_id"], {}).get("is_enabled"))
        if is_system_default_tool(asset):
            return self.tool_registry.is_default_tool(asset, scope) and (
                scope is None or getattr(scope, "user_id", None) == user.user_id
            )
        if asset["resource_type"] == "tool":
            definition = getattr(scope, "definition", {})
            if (
                not self.tool_registry.is_agent_tool(asset)
                or definition.get("agent_type") != "expert"
                or not any(
                    ref["resource_type"] == "tool"
                    and ref["resource_id"] == asset["resource_id"]
                    and ref["resource_version"] == asset["resource_version"]
                    for ref in definition["payload"].get("bindings", [])
                )
                or not self._scope_allows(user, asset, scope)
            ):
                return False
            if snapshot is None:
                await self.permissions.require_execution(
                    user, scope.root_definition, allow_pinned=True
                )
            else:
                await snapshot.require_execution(
                    scope.root_definition, allow_pinned=True
                )
            return True
        if scope is not None:
            if getattr(scope, "user_id", None) != user.user_id:
                return False
            personal = getattr(scope, "definition", {}).get("agent_type") == "personal"
            if not personal:
                if not self._scope_allows(user, asset, scope):
                    return False
                if snapshot is None:
                    await self.permissions.require_execution(
                        user, scope.root_definition, allow_pinned=True
                    )
                else:
                    await snapshot.require_execution(
                        scope.root_definition, allow_pinned=True
                    )
                if asset["resource_type"] != "mcp":
                    return True
            else:
                installed = await self.selected_for_personal(
                    user,
                    asset["resource_type"],
                    enabled_only=True,
                    resource_ids=[asset["resource_id"]],
                )
                if asset["resource_id"] not in installed:
                    return False
        if asset["resource_type"] == "mcp" and not is_managed_connector(asset):
            return False
        own = asset["owner_type"] == "user" and asset["owner_id"] == user.user_id
        view = (
            "private"
            if own
            else "system"
            if asset["owner_type"] == "system"
            else "shared"
        )
        if snapshot is not None:
            return (
                await snapshot.explain(
                    asset["resource_type"],
                    asset["resource_id"],
                    view,
                    version=asset["resource_version"],
                )
            ).is_allowed
        if view == "shared":
            return await self._shared_asset(user, asset)
        return (
            await self.permissions.explain(
                user,
                asset["resource_type"],
                asset["resource_id"],
                view,
                version=asset["resource_version"],
            )
        ).is_allowed

    async def _require(self, user, asset, scope=None):
        snapshot = (await self._read_snapshot(user, [asset])) if scope is None else None
        if not await self._usable(
            snapshot.user if snapshot is not None else user,
            asset,
            scope,
            snapshot=snapshot,
        ):
            raise Forbidden("未获得此资源版本的使用权限")

    @staticmethod
    def _editable(user, asset):
        return user.is_enabled and (
            (asset["owner_type"] == "user" and asset["owner_id"] == user.user_id)
            or (asset["owner_type"] == "system" and user.role_type == "admin")
        )

    @staticmethod
    def _asset_view(user, asset):
        if asset["owner_type"] == "system":
            return "system"
        return "private" if asset["owner_id"] == user.user_id else "shared"

    async def _read_snapshot(self, user, assets, *, refresh_user=True, scope=None):
        references = [
            {
                "resource_type": asset["resource_type"],
                "resource_id": asset["resource_id"],
                "resource_view_type": self._asset_view(user, asset),
                "resource_version": asset["resource_version"],
            }
            for asset in assets
        ]
        if scope is not None:
            root = scope.root_definition
            references.append(
                {
                    "resource_type": root["definition_type"],
                    "resource_id": root["id"],
                    "resource_view_type": root["resource_view_type"],
                    "resource_version": root["resource_version"],
                    "grant_id": root.get("access_grant_id"),
                    "review_id": root.get("shared_review_id"),
                    "allow_pinned": True,
                }
            )
        return await self.permissions.read_snapshot(
            user,
            references,
            loaded=[
                row
                for row in assets
                if row["resource_type"] != "skill" or "owner_version" in row
            ],
            refresh_user=refresh_user,
        )

    async def _presentation(self, user, assets):
        """Read only display data belonging to this already-selected asset set."""
        kinds = {row["resource_type"] for row in assets}
        installations = {}
        for kind in kinds & {"skill", "mcp", "tool"}:
            installations[kind] = await self.storage.personal_resources.list_resources(
                user.user_id,
                kind,
                resource_ids={
                    row["resource_id"] for row in assets if row["resource_type"] == kind
                },
            )
        owner_ids = {
            row["owner_id"]
            for row in assets
            if row["resource_type"] == "skill"
            and row["owner_type"] == "user"
            and row["owner_id"] != user.user_id
        }
        owners = await self.storage.assets.owner_display_names(owner_ids)
        credentials = await self.storage.mcp_credentials.read_many(
            user.user_id,
            {
                self.mcp_authorization._connection(row)
                for row in assets
                if row["resource_type"] == "mcp"
                and row["payload"].get("config", {}).get("auth_type", "none") != "none"
            },
        )
        return installations, owners, credentials

    async def _view(
        self,
        user,
        asset,
        *,
        source=False,
        admin=False,
        installations=None,
        snapshot=None,
        owner_names=None,
        credential_rows=None,
    ):
        """组装资源目录或详情的字段与权限标记。"""
        if snapshot is None:
            snapshot = await self._read_snapshot(user, [asset])
        user = snapshot.user
        own = asset["owner_type"] == "user" and asset["owner_id"] == user.user_id
        view = (
            "private"
            if own
            else "system"
            if asset["owner_type"] == "system"
            else "shared"
        )
        if (
            view == "shared"
            and not admin
            and not (
                await snapshot.explain(
                    asset["resource_type"],
                    asset["resource_id"],
                    view,
                    version=asset["resource_version"],
                )
            ).is_allowed
        ):
            raise Forbidden("此资源版本尚未共享")
        system_default = self.tool_registry.is_default_tool(asset)
        agent_tool = self.tool_registry.is_agent_tool(asset)
        optional_tool = self.tool_registry.is_optional_tool(asset)
        installation = None
        if asset["resource_type"] in ("skill", "mcp") or optional_tool:
            if installations is None:
                installations = await self.personal.list_resources(
                    user, asset["resource_type"]
                )
            installation = installations.get(asset["resource_id"])
        payload = {
            k: v
            for k, v in asset["payload"].items()
            if k
            in {
                "display_name",
                "description",
                "avatar",
                "category",
                "input_schema",
                "source",
                "exposure",
                "tool_name",
            }
        }
        if asset["payload"].get("avatar_checksum"):
            payload["has_generated_avatar"] = True
        usable = await self._usable(user, asset, snapshot=snapshot)
        editable = self._editable(user, asset)
        if source:
            if not usable and not editable and not self._tool_card_visible(user, asset):
                raise Forbidden()
            if asset["resource_type"] == "skill":
                payload["markdown"] = await self.read_skill_markdown(asset)
                payload["files"] = asset["payload"]["files"]
                payload["base_version"] = asset["resource_version"]
            elif asset["resource_type"] == "mcp":
                payload["config"] = self.mcp_registry.config(asset)
        revision = asset.get("publication_revision")
        is_connector = asset["resource_type"] == "mcp"
        authorized = (
            (
                await self.mcp_authorization.status(
                    user,
                    asset,
                    **(
                        {
                            "credential_row": credential_rows.get(
                                self.mcp_authorization._connection(asset)
                            )
                        }
                        if credential_rows is not None
                        else {}
                    ),
                )
            )["authorized"]
            if is_connector and usable and not admin
            else False
        )
        connector_attributes = (
            {"publication_revision": revision}
            if admin
            else {"is_authorized": authorized}
        )
        share = (
            await snapshot.share_availability(
                asset["resource_type"],
                asset["resource_id"],
                version=asset["resource_version"],
                view=view,
            )
            if editable and asset["resource_type"] == "skill"
            else {}
        )
        owner = {"owner_type": asset["owner_type"], "owner_id": asset["owner_id"]}
        if asset["resource_type"] == "skill":
            if asset["owner_type"] == "system":
                owner["owner_display_name"] = "系统"
            elif own:
                owner["owner_display_name"] = user.username
            else:
                if owner_names is None:
                    owner_names = await self.storage.assets.owner_display_names(
                        {asset["owner_id"]}
                    )
                owner["owner_display_name"] = owner_names.get(
                    asset["owner_id"], asset["owner_id"]
                )
        return {
            "resource_id": asset["resource_id"],
            **(
                connector_attributes
                if is_connector
                else {
                    "resource_version": asset["resource_version"],
                    "version_label": publication_label(
                        require_skill_publication_revision(revision)
                    )
                    if asset["resource_type"] == "skill"
                    else asset["resource_version"],
                }
            ),
            **share,
            **owner,
            "display_name": payload.get("display_name", asset["resource_id"]),
            "description": payload.get("description", ""),
            "resource_view_type": view,
            "is_usable": usable,
            "is_editable": editable,
            "is_deletable": editable and own and asset["resource_type"] == "skill",
            "is_available": asset["is_enabled"] and (not optional_tool or usable),
            "auth_type": asset["payload"].get("config", {}).get("auth_type", "none")
            if asset["resource_type"] == "mcp"
            else None,
            "is_system_default": system_default,
            "is_optional_tool": optional_tool,
            "is_agent_tool": agent_tool,
            "is_installed": system_default or installation is not None,
            "is_enabled": asset["is_enabled"]
            if asset["resource_type"] == "tool" and not optional_tool
            else bool(installation and installation["is_enabled"]),
            "payload": payload,
        }

    async def list(self, user, resource_type):
        """列出资源目录，工具目录只包含可见的平台和专家工具。"""
        self._kind(resource_type)
        users = UserService(self.storage, self.policies)
        user = await users.get(user.user_id)
        if resource_type == "skill":
            rows = await self.storage.assets.skills.page_catalog(
                user, after=None, limit=None, query="", category="", mine=False
            )
        elif resource_type == "mcp":
            company_id = users.company_group().group_id
            if company_id not in user.group_ids:
                return []
            rows = await self.storage.assets.page_connector_catalog(
                company_group_id=company_id, after=None, limit=None, query=""
            )
        else:
            rows = await self._current_versions(resource_type)
        snapshot = await self._read_snapshot(user, rows, refresh_user=False)
        selected = await self._latest_visible_assets(
            user, resource_type, rows, snapshot=snapshot
        )
        assets = sorted(selected.values(), key=lambda a: a["resource_id"])
        installations, owners, credentials = await self._presentation(user, assets)
        return [
            await self._catalog_view(
                user,
                asset,
                installations=installations.get(resource_type),
                snapshot=snapshot,
                owner_names=owners,
                credential_rows=credentials,
            )
            for asset in assets
        ]

    async def _catalog_view(
        self,
        user,
        asset,
        *,
        installations=None,
        snapshot=None,
        owner_names=None,
        credential_rows=None,
    ):
        item = await self._view(
            user,
            asset,
            installations=installations,
            snapshot=snapshot,
            owner_names=owner_names,
            credential_rows=credential_rows,
        )
        if asset["resource_type"] == "skill" and item["is_usable"]:
            item["runtime_name"] = self._runtime_name(
                user, "skill", asset["resource_id"], asset["resource_version"]
            )
        return item

    async def catalog_page(
        self, user, kind, *, cursor=None, limit=None, query="", category="", mine=False
    ):
        """Page the catalog after selecting visible current versions and filters."""
        self._kind(kind)
        config = self.config.snapshot if self.config is not None else None
        capability = "connector" if kind == "mcp" else kind
        pagination = (
            getattr(config, capability).catalog
            if config is not None
            else CatalogPaginationConfig()
        )
        size = pagination.default_page_size if limit is None else limit
        if not 1 <= size <= pagination.max_page_size:
            raise InvalidRequest(
                f"分页大小必须介于 1 和 {pagination.max_page_size} 之间"
            )
        if max(len(query), len(category)) > pagination.max_query_length:
            raise InvalidRequest(f"目录筛选最多 {pagination.max_query_length} 个字符")
        if "\0" in query or "\0" in category:
            raise InvalidRequest("无效的目录筛选")
        if cursor is not None and len(cursor) > pagination.max_cursor_length:
            raise InvalidRequest(f"目录游标最多 {pagination.max_cursor_length} 个字符")

        query = query.strip().lower()
        scope = hashlib.sha256(
            json.dumps([user.user_id, kind, query, category, mine]).encode()
        ).hexdigest()
        after = self._catalog_after(cursor, scope) if cursor is not None else None
        users = UserService(self.storage, self.policies)
        user = await users.get(user.user_id)
        assets = self.storage.assets
        if kind == "skill":
            read_page = partial(
                assets.skills.page_catalog,
                user,
                query=query,
                category=category,
                mine=mine,
            )
        elif kind == "mcp":
            company_group_id = users.company_group().group_id
            if company_group_id not in user.group_ids:
                return {"items": [], "next_cursor": None}
            read_page = partial(
                assets.page_connector_catalog,
                company_group_id=company_group_id,
                query=query,
            )
        else:
            read_page = partial(
                self._page_tool_catalog,
                query=query,
                category=category,
                default_ids=(
                    config.tool.defaults.registered_ids if config is not None else None
                ),
            )
        items, more = await self._render_catalog_page(
            user, kind, read_page, after=after, size=size
        )
        metadata = {}
        if after is None and kind == "skill":
            metadata = await assets.skills.catalog_metadata(user)
        elif after is None and kind == "tool":
            metadata = {"categories": list(TOOL_CATEGORIES)}
        return {
            "items": items,
            "next_cursor": base64.urlsafe_b64encode(
                json.dumps([scope, items[-1]["resource_id"]]).encode()
            ).decode()
            if more
            else None,
            **metadata,
        }

    async def _render_catalog_page(self, user, kind, read_page, *, after, size):
        # A grant can be revoked between selection and view assembly. Refill
        # from the last examined ID if a selected card is no longer visible.
        items = []
        while True:
            remaining = size - len(items)
            rows = await read_page(after=after, limit=remaining + 1)
            page, more = rows[:remaining], len(rows) > remaining
            snapshot = await self._read_snapshot(user, page, refresh_user=False)
            installations, owners, credentials = await self._presentation(user, page)
            for asset in page:
                try:
                    item = await self._catalog_view(
                        user,
                        asset,
                        installations=installations.get(kind),
                        snapshot=snapshot,
                        owner_names=owners,
                        credential_rows=credentials,
                    )
                except Forbidden:
                    continue
                if kind == "tool":
                    visible = (
                        item["is_system_default"]
                        or item["is_optional_tool"]
                        or item["is_agent_tool"]
                    )
                else:
                    visible = item["is_usable"] or (
                        kind == "skill" and item["is_editable"]
                    )
                if visible:
                    items.append(item)
            if not more or len(items) == size:
                return items, more
            after = page[-1]["resource_id"]

    @staticmethod
    def _catalog_after(cursor, scope):
        try:
            saved_scope, after = json.loads(
                base64.b64decode(cursor, altchars=b"-_", validate=True)
            )
            # Read keys may be legacy IDs that predate current write validation.
            if (
                saved_scope == scope
                and isinstance(after, str)
                and 0 < len(after) <= 128
                and "\0" not in after
            ):
                after.encode()
                return after
        except (ValueError, TypeError):
            pass
        raise InvalidRequest("无效的资源分页游标")

    async def _page_tool_catalog(self, *, after, limit, query, category, default_ids):
        # Choose the latest eligible version before applying the card filters.
        def matches(asset):
            platform, optional, expert = TOOL_CATEGORIES
            if self.tool_registry.is_optional_tool(asset):
                tool_category = optional
            elif self.tool_registry.is_default_tool(asset):
                tool_category = platform
            elif self.tool_registry.is_agent_tool(asset):
                tool_category = expert
            else:
                return False
            payload = asset["payload"]
            name = payload.get("display_name", asset["resource_id"])
            description = payload.get("description", "")
            return (not category or category == tool_category) and (
                not query or query in f"{name} {description}".lower()
            )

        deployed = [
            (rid, version, self.tool_registry.catalog[(rid, version)].checksum_sha256)
            for rid, version in set(self.tool_registry.deployed) | self.tool_registry.optional
        ]
        versions = self.storage.assets.catalog_tool_versions(
            after=after, batch_size=limit, default_ids=default_ids, deployed=deployed
        )
        result = []
        selected = None
        async with aclosing(versions):
            async for asset in versions:
                if (
                    selected is not None
                    and selected["resource_id"] != asset["resource_id"]
                ):
                    if matches(selected):
                        result.append(selected)
                        if len(result) == limit:
                            return result
                    selected = None
                if selected is None or asset_version_order(asset) > asset_version_order(
                    selected
                ):
                    selected = asset
        if selected is not None and matches(selected):
            result.append(selected)
        return result

    async def _latest_visible_assets(
        self, user, resource_type, rows, *, usable_only=False, snapshot=None
    ):
        """按各类资源的可见条件选择最新目录版本。"""
        rows = list(rows)
        if snapshot is None:
            snapshot = await self._read_snapshot(user, rows)
        user = snapshot.user
        selected = {}
        ranks = {}
        for asset in rows:
            if resource_type == "skill" and not self._is_current_skill_view(
                user, asset
            ):
                continue
            usable = False
            if resource_type == "tool":
                if not self._tool_card_visible(user, asset):
                    continue
            else:
                usable = await self._usable(user, asset, snapshot=snapshot)
                if not usable and (
                    usable_only
                    or resource_type == "mcp"
                    or not self._editable(user, asset)
                ):
                    continue
            # 在可见版本中优先选择可用版本，再按发布版本排序。
            rid = asset["resource_id"]
            rank = (
                usable,
                asset["is_enabled"],
                asset_version_order(asset),
            )
            if rid not in ranks or rank > ranks[rid]:
                selected[rid], ranks[rid] = asset, rank
        return selected

    @staticmethod
    def _is_current_skill_view(user, asset):
        """系统技能及本人技能使用所有者版本，其他用户使用共享版本。"""
        version = (
            asset["owner_version"]
            if asset["owner_type"] == "system" or asset["owner_id"] == user.user_id
            else asset["shared_version"]
        )
        return asset["publication_revision"] == version

    def _tool_card_visible(self, user, asset):
        """只展示已启用的平台默认工具或专家绑定候选。"""
        return (
            user.is_enabled
            and asset["is_enabled"]
            and (
                self.tool_registry.is_default_tool(asset)
                or self.tool_registry.is_optional_tool(asset)
                or self.tool_registry.is_agent_tool(asset)
            )
        )

    async def detail(self, user, resource_type, resource_id, version=None):
        asset = await self._asset(resource_type, resource_id, version)
        snapshot = await self._read_snapshot(user, [asset])
        if resource_type == "mcp" and not await self._usable(
            snapshot.user, asset, snapshot=snapshot
        ):
            raise Forbidden("未获得此资源版本的使用权限")
        return await self._view(snapshot.user, asset, source=True, snapshot=snapshot)

    async def _owner_for_save(
        self, user, registry, rid, version, data, *, virtual_uploader=None
    ):
        """发布前核对资源归属以及版本是否已存在。"""
        if not user.is_enabled:
            raise Forbidden()
        current = await registry.find_current(rid)
        if current is not None:
            if not self._editable(user, current):
                raise Forbidden()
            if data.get("owner_type", current["owner_type"]) != current["owner_type"]:
                raise Forbidden("发布新版本时不能改变技能归属")
            if virtual_uploader is not None:
                self._require_virtual_skill_owner(current, virtual_uploader)
            if await registry.get_version(rid, version) is not None:
                raise Conflict("资源版本不可变，请发布新版本")
            return current["owner_type"], current["owner_id"]
        if data.get("owner_type") == "system":
            require_admin(user)
            return "system", data.get("owner_id", "system")
        if data.get("owner_id", user.user_id) != user.user_id:
            raise Forbidden()
        return "user", user.user_id

    @staticmethod
    def _require_virtual_skill_owner(asset, virtual_user_id):
        if (
            asset["owner_type"] != "system"
            or asset["source_user_id"] != virtual_user_id
        ):
            raise Forbidden("虚拟用户只能更新自己提交的系统技能")

    async def require_skill_upload_target(
        self, user, resource_id, *, owner_type, virtual
    ):
        current = await self.skill_registry.find_current(resource_id)
        if current is None:
            raise NotFound("待更新的技能不存在")
        if current["owner_type"] != owner_type:
            raise Forbidden("发布新版本时不能改变技能归属")
        if virtual:
            self._require_virtual_skill_owner(current, user.user_id)
        elif not self._editable(user, current):
            raise Forbidden()

    async def _save(
        self,
        user,
        kind,
        data,
        *,
        files=None,
        import_job_id=None,
        progress=None,
        archive_limits=None,
        uploader_id=None,
        uploader_type=None,
    ):
        """原子发布资源版本、附件和归属授权。"""
        rid = data.get("resource_id") or kind + "-" + uuid4().hex
        version = data.get("resource_version") or "1.0.0"
        self._identity(rid, version)
        metadata = {
            "dependency_only": data.get("dependency_only") is True,
            "avatar": normalize_skill_avatar(data.get("avatar")),
            "display_name": str(data.get("display_name") or rid)[:200],
            "description": str(data.get("description") or "")[:2000],
        }
        if progress:
            await progress("preparing", 35, resource_version=version)
        metadata.update(
            category=str(data.get("category") or "other")[:80],
            files=[
                {"path": name, "size": len(value)}
                for name, value in sorted(files.items())
            ],
        )
        if import_job_id:
            metadata["import_job_id"] = import_job_id
        uploader_id = uploader_id or user.user_id
        metadata["uploader_type"] = uploader_type or (
            "admin" if user.role_type == "admin" else "user"
        )
        limits = archive_limits or self.archive_limits
        if sum(map(len, files.values())) > limits.expanded_bytes_limit:
            raise BadRequest("Skill exceeds upload limit")
        digest = await asyncio.to_thread(calculate_files_digest, files)
        virtual_uploader = uploader_id if uploader_type == "virtual" else None
        thumbnail = None
        if metadata["avatar"] == AUTO_AVATAR and self.config is not None:
            existing = (
                await self.skill_registry.get_version(rid, version)
                if import_job_id
                else None
            )
            if existing is None:
                await self._owner_for_save(
                    user,
                    self.skill_registry,
                    rid,
                    version,
                    data,
                    virtual_uploader=virtual_uploader,
                )
                thumbnail = await generate_skill_avatar(
                    user,
                    self.storage,
                    self.config,
                    metadata["display_name"],
                    metadata["description"],
                )
        async with self.storage.definition_write() as write:
            registry = LocalSkillRegistry(write.assets, self.skill_versions)
            await write.assets.lock_resource(kind, rid)
            existing = (
                await registry.get_version(rid, version) if import_job_id else None
            )
            if existing:
                if (
                    existing["created_by"] != uploader_id
                    or existing["payload"].get("import_job_id") != import_job_id
                ):
                    raise Conflict("Import identity already exists")
                asset = existing
            else:
                revision = await write.assets.next_publication_revision(kind, rid)
                owner_type, owner_id = await self._owner_for_save(
                    user,
                    registry,
                    rid,
                    version,
                    data,
                    virtual_uploader=virtual_uploader,
                )
                if thumbnail:
                    locator = {
                        "resource_id": rid,
                        "resource_version": version,
                        "owner_type": owner_type,
                        "owner_id": owner_id,
                    }
                    try:
                        await asyncio.to_thread(
                            self.skill_versions.write_avatar, locator, thumbnail
                        )
                    except (AgentKitError, OSError) as exc:
                        logger.warning(
                            "skill.avatar_write_failed (%s)", type(exc).__name__
                        )
                    else:
                        metadata["avatar_checksum"] = hashlib.sha256(
                            thumbnail
                        ).hexdigest()
                model = Asset(
                    resource_type=kind,
                    resource_id=rid,
                    resource_version=version,
                    publication_revision=revision,
                    owner_type=owner_type,
                    owner_id=owner_id,
                    payload=metadata,
                    checksum_sha256=digest,
                )
                model = await registry.register(model, actor=uploader_id)
                asset = model.model_dump()
                await GrantService(write).record_owner(
                    kind, rid, asset, actor=user.user_id
                )
                await registry.publish_files(
                    asset, files, max_bytes=limits.expanded_bytes_limit
                )
        return await self._view(user, asset)

    @staticmethod
    def _skill_data(markdown, data):
        front = _skill_front(markdown)
        metadata = front.get("metadata")
        nested_version = metadata.get("version") if isinstance(metadata, dict) else None
        version = front.get("version", nested_version)
        if (
            front.get("version") is not None
            and nested_version is not None
            and str(front["version"]) != str(nested_version)
        ):
            raise BadRequest("Skill version fields in SKILL.md must agree")
        supplied = data.get("resource_version")
        if version is not None:
            version = str(version)
            if supplied and supplied != version:
                raise BadRequest("Skill version must match SKILL.md front matter")
        else:
            version = supplied or "1.0.0"
        return {
            **data,
            "resource_version": version,
            "display_name": front.get("name") or data.get("display_name") or "Skill",
            "description": front.get("description", data.get("description", "")),
        }

    async def save_skill(self, user, data):
        limits = self.archive_limits
        markdown = data.get("markdown", "")
        if not isinstance(markdown, str) or not markdown.strip():
            raise BadRequest("Skill markdown is required")
        if len(markdown.encode()) > limits.expanded_bytes_limit:
            raise BadRequest("Skill exceeds upload limit")
        data = await asyncio.to_thread(self._skill_data, markdown, data)
        files = {}
        rid = data.get("resource_id")
        base_version = data.get("base_version")
        if rid:
            current = await self.skill_registry.find_current(rid)
            if current is not None:
                if not self._editable(user, current):
                    raise Forbidden()
                if await self.skill_registry.get_version(rid, data["resource_version"]):
                    raise Conflict("资源版本不可变，请发布新版本")
                if not base_version:
                    raise BadRequest(
                        "A base version is required to edit an existing Skill"
                    )
        if base_version:
            base = await self._asset("skill", rid, base_version)
            if not self._editable(user, base):
                raise Forbidden()
            _, files, _ = await self.skill_registry.read(
                base, max_bytes=limits.expanded_bytes_limit
            )
            data.setdefault("category", base["payload"].get("category"))
        files["SKILL.md"] = markdown.encode()
        return await self._save(user, "skill", data, files=files, archive_limits=limits)

    async def upload_skill(
        self,
        user,
        archive,
        version=None,
        *,
        category="other",
        avatar=AUTO_AVATAR,
        resource_id=None,
        import_job_id=None,
        progress=None,
        owner_type=None,
        owner_id=None,
        uploader_id=None,
        uploader_type=None,
    ):
        limits = self.archive_limits
        files = await asyncio.to_thread(read_archive_files, archive, limits)
        markdown = decode_skill_markdown(files["SKILL.md"])
        data = await asyncio.to_thread(
            self._skill_data,
            markdown,
            {
                "resource_version": version,
                "category": category,
                "avatar": avatar,
                **({"resource_id": resource_id} if resource_id else {}),
                **({"owner_type": owner_type} if owner_type else {}),
                **({"owner_id": owner_id} if owner_id else {}),
            },
        )
        kwargs = {
            "files": files,
            "archive_limits": limits,
            "uploader_id": uploader_id,
            "uploader_type": uploader_type,
        }
        if import_job_id:
            kwargs.update(
                import_job_id=import_job_id,
                progress=progress,
            )
        return await self._save(user, "skill", data, **kwargs)

    async def save_mcp(self, user, data):
        asset = await self.connectors.save(user, data)
        return await self._admin_view(user, asset)

    async def skill_descriptors(self, user, refs, authorization_scope=None):
        refs = [
            ref if isinstance(ref, ResourceRef) else ResourceRef.model_validate(ref)
            for ref in refs
        ]
        refs = [ref for ref in refs if ref.resource_type == "skill"]
        if not refs:
            return []
        rows = await self.storage.assets.get_many(
            "skill", {(ref.resource_id, ref.resource_version) for ref in refs}
        )
        assets = {(row["resource_id"], row["resource_version"]): row for row in rows}
        scope = authorization_scope
        personal = (
            scope is not None and scope.definition.get("agent_type") == "personal"
        )
        snapshot = await self._read_snapshot(
            user, rows, scope=scope if scope is not None and not personal else None
        )
        user = snapshot.user
        selected = None
        if personal:
            if scope.user_id != user.user_id:
                raise Forbidden("资源装配用户与会话不一致")
            settings = await self.storage.personal_resources.list_resources(
                user.user_id, "skill", resource_ids={ref.resource_id for ref in refs}
            )
            selected = await self._latest_visible_assets(
                user,
                "skill",
                [
                    row
                    for row in snapshot.rows.values()
                    if row.get("resource_type") == "skill"
                    and (settings.get(row["resource_id"]) or {}).get("is_enabled")
                ],
                usable_only=True,
                snapshot=snapshot,
            )
        result = []
        for ref in refs:
            asset = assets.get((ref.resource_id, ref.resource_version))
            if asset is None:
                raise NotFound()
            if selected is not None and ref.resource_id not in selected:
                raise Forbidden("技能未启用或已不可用")
            if not await self._usable(
                user, asset, None if personal else scope, snapshot=snapshot
            ):
                raise Forbidden("未获得此资源版本的使用权限")
            result.append(
                Skill(
                    name=self._runtime_name(
                        user, "skill", ref.resource_id, ref.resource_version
                    ),
                    description=asset["payload"].get("description", ""),
                    dir="",
                    markdown="",
                    updated_at=0,
                )
            )
        return result

    async def read_skill_markdown(self, asset):
        """Read canonical Markdown for a version already authorized by the caller."""
        _, files, _ = await self.skill_registry.read(
            asset, max_bytes=self.archive_limits.expanded_bytes_limit
        )
        return files["SKILL.md"].decode("utf-8")

    async def export_skill_archive(self, asset, *, archive_limits=None):
        """Package the verified files of an already authorized version."""
        limits = archive_limits or self.archive_limits
        _, files, _ = await self.skill_registry.read(
            asset, max_bytes=limits.expanded_bytes_limit
        )
        content = await asyncio.to_thread(
            build_skill_archive, files, asset["resource_id"]
        )
        if len(content) > limits.max_bytes:
            raise BadRequest("Skill download exceeds configured limit")
        return content

    async def download_skill(
        self,
        user,
        resource_id,
        version,
        *,
        authorization_scope=None,
        for_edit=False,
        archive_limits=None,
    ):
        asset = await self._asset("skill", resource_id, version)
        if not (for_edit and self._editable(user, asset)):
            await self._require(user, asset, authorization_scope)
        return await self.export_skill_archive(asset, archive_limits=archive_limits)

    async def read_skill_avatar(self, user, resource_id, version):
        asset = await self._asset("skill", resource_id, version)
        if not self._editable(user, asset):
            await self._require(user, asset)
        expected = asset["payload"].get("avatar_checksum")
        if not expected:
            raise NotFound()
        locator = {
            key: asset[key]
            for key in ("resource_id", "resource_version", "owner_type", "owner_id")
        }
        content = await asyncio.to_thread(self.skill_versions.read_avatar, locator)
        if hashlib.sha256(content).hexdigest() != expected:
            raise NotFound()
        return content

    async def _runtime_skill(self, user, asset):
        path, files, updated_at = await self.skill_registry.read(
            asset, max_bytes=self.archive_limits.expanded_bytes_limit
        )
        return Skill(
            name=self._runtime_name(
                user, "skill", asset["resource_id"], asset["resource_version"]
            ),
            description=asset["payload"].get("description", ""),
            dir=str(path),
            markdown=files["SKILL.md"].decode("utf-8"),
            updated_at=updated_at,
        )

    async def materialize_skill(
        self, user, resource_id, version, *, authorization_scope=None
    ):
        asset = await self._asset("skill", resource_id, version)
        await self._require(user, asset, authorization_scope)
        return await self._runtime_skill(user, asset)

    async def skill_pin(self, user, ref, skill):
        asset = await self._asset("skill", ref["resource_id"], ref["resource_version"])
        return {
            **ref,
            "source_checksum_sha256": asset["checksum_sha256"],
            "skill_name": skill.name,
        }

    async def restore_skill(self, user, pin, *, authorization_scope):
        """Restore the pinned version after rechecking access and source integrity."""
        pin = SkillActivation.model_validate(pin).model_dump()
        rid, version = pin["resource_id"], pin["resource_version"]
        if pin["skill_name"] != self._runtime_name(user, "skill", rid, version):
            raise Forbidden("已激活技能名称无效")
        asset = await self._asset("skill", rid, version)
        await self._require(user, asset, authorization_scope)
        if asset["checksum_sha256"] != pin["source_checksum_sha256"]:
            raise Conflict("已激活技能的注册内容已变化，不能自动替换会话版本")
        return await self._runtime_skill(user, asset)

    async def mcps_for_refs(
        self,
        user,
        refs,
        authorization_scope=None,
        *,
        skip_unavailable=False,
        installations=None,
        refresh_user=True,
    ):
        refs = [
            ref if isinstance(ref, ResourceRef) else ResourceRef.model_validate(ref)
            for ref in refs
        ]
        refs = [ref for ref in refs if ref.resource_type == "mcp"]
        if not refs:
            return []
        rows = await self.storage.assets.get_many(
            "mcp", {(ref.resource_id, MCP_STORAGE_VERSION) for ref in refs}
        )
        rows = [row for row in rows if is_managed_connector(row)]
        assets = {row["resource_id"]: row for row in rows}
        scope = authorization_scope
        personal = (
            scope is not None and scope.definition.get("agent_type") == "personal"
        )
        try:
            if scope is not None and scope.user_id != user.user_id:
                raise Forbidden("资源装配用户与会话不一致")
            snapshot = await self._read_snapshot(
                user,
                rows,
                refresh_user=refresh_user,
                scope=scope if scope is not None and not personal else None,
            )
            user = snapshot.user
            if scope is not None and not personal:
                await snapshot.require_execution(
                    scope.root_definition, allow_pinned=True
                )
            elif personal and installations is None:
                installations = await self.storage.personal_resources.list_resources(
                    user.user_id, "mcp", resource_ids=set(assets)
                )
        except AgentKitError:
            if skip_unavailable:
                return []
            raise
        result = []
        for ref in refs:
            try:
                asset = assets.get(ref.resource_id)
                if asset is None:
                    raise NotFound()
                if personal and not (installations.get(ref.resource_id) or {}).get(
                    "is_enabled"
                ):
                    raise Forbidden("连接器未启用")
                if (
                    scope is not None
                    and not personal
                    and not self._scope_allows(user, asset, scope)
                ):
                    raise Forbidden("连接器未绑定至当前会话")
                if not await self._usable(user, asset, snapshot=snapshot):
                    raise Forbidden("未获得此资源版本的使用权限")
                client = await self._mcp_client(user, asset)
            except AgentKitError:
                if not skip_unavailable:
                    raise
                continue
            result.append(client)
        return result

    def _verified_mcp_card(self, asset):
        card = self.mcp_hub.card(asset)
        _verify_mcp_card(asset, card)
        return card

    async def _mcp_client(self, user, asset):
        rendered = render_mcp(
            self._verified_mcp_card(asset),
            {},
            name=self._runtime_name(user, "mcp", asset["resource_id"], None),
        )
        client = AuthorizedMCPClient.model_validate(rendered.model_dump())
        headers = await self.mcp_authorization.headers(user, asset)
        if headers:
            await client.set_runtime_headers(headers)
        return client

    async def authorize_session_tool(
        self, user_id, scope, workspace, agent, tool, parameters=None, *, users
    ):
        """工具调用时重新取得用户及授权状态，再按连接器或平台工具规则校验。"""
        # 1. 工具展示后用户或资源可能被撤权，实际执行前重新装载受权资源。
        current_user = await users.get(user_id)
        if parameters is not None:
            await self.prepare_workspace(current_user, scope, workspace)
        # 2. 连接器按受管来源验证，其余工具交给既有工具登记能力。
        if await self.authorize_mcp_tool(current_user, scope, tool):
            return
        await self.tool_registry.authorize(current_user, scope, agent, tool, parameters)

    async def authorize_mcp_tool(self, user, scope, tool):
        # 原生网关工具与本地 MCPTool 使用同一协议，二者并非同一具体类型。
        if not getattr(tool, "is_mcp", False):
            return False
        client = getattr(tool, "_egis_mcp_client", None)
        if client is None or client.name != tool.mcp_name:
            raise Forbidden("MCP 工具没有受管的连接来源")
        for row in await self._versions("mcp"):
            if client.name != self._runtime_name(
                user, "mcp", row["resource_id"], row["resource_version"]
            ):
                continue
            await self._require(user, row, scope)
            current = await self._mcp_client(user, row)
            if client.mcp_config != current.mcp_config or credential_fingerprint(
                client
            ) != credential_fingerprint(current):
                raise Forbidden("连接凭证已变化，请重新发起对话")
            return True
        raise Forbidden("当前会话没有此 MCP 连接")

    async def mcp_auth_action(self, user, rid, action, **kwargs):
        asset = await self._asset("mcp", rid, None)
        await self._require(user, asset)
        self._verified_mcp_card(asset)
        return await getattr(self.mcp_authorization, action)(user, asset, **kwargs)

    async def test_mcp(self, user, resource_id):
        require_admin(user)
        asset = await self._asset("mcp", resource_id, None)
        await self._require(user, asset)
        client = await self._mcp_client(user, asset)
        try:
            await client.connect()
            tools = await client.list_tools()
            return {
                "is_connected": True,
                "tools": [
                    {
                        "name": tool.name,
                        "description": tool.description,
                        "input_schema": tool.input_schema,
                    }
                    for tool in tools
                ],
            }
        except Exception:
            raise AgentKitError("MCP 连接测试失败", 502) from None
        finally:
            await client.close()
