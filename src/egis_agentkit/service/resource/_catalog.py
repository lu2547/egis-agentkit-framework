"""Skill、MCP 和 Function Tool 目录及使用。"""

from __future__ import annotations

import asyncio
import errno
import hashlib
import os
import re
import shutil
import tempfile
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path, PurePosixPath
from uuid import uuid4

from agentscope.app._service._mcp_render import render_mcp
from agentscope.skill import Skill
from packaging.version import Version

from egis_agentkit.backend.nacos._settings import (
    DEFAULT_SKILL_ARCHIVE_MAX_BYTES,
    SkillArchiveConfig,
)
from egis_agentkit.errors import AgentKitError, Conflict, Forbidden, NotFound
from egis_agentkit.service._user import UserService, require_admin
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import (
    PermissionService,
    is_system_default_tool,
)
from egis_agentkit.service.agent._installations import PersonalInstallations
from egis_agentkit.storage.model import (
    Asset,
    ResourceRef,
    SkillActivation,
)
from egis_agentkit.storage.model._base import publication_label

from ._connector import (
    AuthorizedMCPClient,
    LocalMCPRegistry,
    _verify_mcp_card,
    mcp_config_checksum,
)
from ._connector_auth import MCPAuthorizationService
from ._skill import (
    LocalSkillRegistry,
    _archive_files,
    _files_digest,
    _skill_front,
    _zip,
)
from ._tool import FunctionToolRegistry

__all__ = ["SkillActivation"]


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
        raise ValueError("请选择标准图标库中的图标")
    return value


_hub_access: ContextVar[tuple | None] = ContextVar(
    "agentkit_resource_hub_access", default=None
)


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
        from egis_agentkit.extension._resource_hubs_impl import (
            LocalMCPHub,
            LocalSkillHub,
        )

        self.storage = storage
        self.workspace_root = Path(workspace_root).resolve()
        self.policies = policies
        self.config = config
        self.permissions = PermissionService(storage, policies)
        self.grants = GrantService(storage)
        self._cache_locks = {}
        self.skill_registry = LocalSkillRegistry(storage.assets)
        self.mcp_registry = LocalMCPRegistry(storage.assets)
        self.skill_hub = LocalSkillHub(self)
        self.mcp_hub = LocalMCPHub(self)
        self.mcp_authorization = MCPAuthorizationService(
            storage, key=credential_key, config=config
        )
        self.tool_registry = FunctionToolRegistry(self)
        self.installations = PersonalInstallations(self)
        self.tool_registry.bind_default_configuration()

    async def seed_builtin_tools(self):
        await self.tool_registry.seed()

    def _registry(self, kind):
        if kind == "skill":
            return self.skill_registry
        if kind == "mcp":
            return self.mcp_registry
        return self.tool_registry

    async def _versions(self, kind, **kwargs):
        return await self._registry(kind).list(**kwargs)

    async def hub_list(self, user_id, kind):
        user = await UserService(self.storage).get(user_id)
        rows = await self._versions(kind)
        usable = [row for row in rows if await self._usable(user, row)]
        latest = await self._latest_visible_assets(user, kind, usable)
        return sorted(latest.values(), key=lambda row: row["resource_id"])

    async def hub_asset(self, user_id, kind, rid, version):
        if version is None:
            for asset in await self.hub_list(user_id, kind):
                if asset["resource_id"] == rid:
                    return asset
            raise NotFound()
        user = await UserService(self.storage).get(user_id)
        asset = await self._asset(kind, rid, version)
        if _hub_access.get() != (user_id, kind, rid, version):
            await self._require(user, asset)
        return asset

    async def set_asset_enabled(
        self, user, kind, rid, version, enabled, *, admin=False
    ):
        """同步更新版本状态和对应授权，保留其他资源版本。"""
        if admin:
            require_admin(user)
        async with self.storage.definition_write() as write:
            await write.assets.lock_resource(kind, rid)
            asset = await self._asset(kind, rid, version, assets=write.assets)
            if not admin and not self._editable(user, asset):
                raise Forbidden()
            if not enabled and is_system_default_tool(asset):
                raise Conflict("系统默认工具不能停用")
            await write.assets.set_enabled(
                kind, rid, version, enabled, actor=user.user_id
            )
            if kind != "tool":
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

    async def _admin_view(self, user, asset, *, bindings=None, grants=None):
        view = await self._view(user, asset, admin=True)
        return {
            **view,
            "entrypoint": asset["entrypoint"],
            "bound_agents": bindings or [],
            "grant_sources": grants or [],
        }

    async def admin_list(self, user, kind):
        require_admin(user)
        self._kind(kind)
        rows = await self._versions(kind)
        agents, _ = await self.storage.definitions.read_catalog("agent")
        grants = [] if kind == "tool" else await self.grants.list_grants(user)
        return [
            await self._admin_view(
                user,
                row,
                bindings=[
                    {"id": agent["id"], "name": agent["agent_display_name"]}
                    for agent in agents
                    if agent["is_enabled"]
                    and any(
                        (
                            ref["resource_type"],
                            ref["resource_id"],
                            ref["resource_version"],
                        )
                        == (kind, row["resource_id"], row["resource_version"])
                        for ref in agent["payload"].get("bindings", [])
                    )
                ],
                grants=[
                    grant
                    for grant in grants
                    if not grant["revoked_at"]
                    and (grant["resource_type"], grant["resource_id"])
                    == (kind, row["resource_id"])
                ],
            )
            for row in sorted(
                rows, key=lambda row: (row["resource_id"], row["resource_version"])
            )
        ]

    async def list_asset_versions(self, user, kind, rid):
        """列出此用户可访问的确切资源版本。"""
        self._kind(kind)
        rows = await self._versions(kind, resource_ids=[rid])
        if kind == "tool":
            return [
                await self._view(user, asset)
                for asset in sorted(
                    rows,
                    key=lambda asset: Version(asset["resource_version"]),
                    reverse=True,
                )
                if self._tool_card_visible(user, asset)
            ]
        return [
            await self._view(user, asset)
            for asset in sorted(
                rows, key=lambda asset: Version(asset["resource_version"]), reverse=True
            )
            if self._editable(user, asset) or await self._usable(user, asset)
        ]

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
            raise ValueError("Unknown resource type")
        return kind

    @staticmethod
    def _identity(rid, version):
        if not isinstance(rid, str) or not re.fullmatch(
            r"[A-Za-z0-9][A-Za-z0-9_-]{0,100}", rid
        ):
            raise ValueError("Invalid resource identifier")
        if not isinstance(version, str) or not re.fullmatch(
            r"\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?", version
        ):
            raise ValueError("An exact resource version is required")

    @staticmethod
    def _native_id(user, kind, rid, version):
        return hashlib.sha256(
            f"{user.user_id}\0{kind}\0{rid}\0{version}".encode()
        ).hexdigest()

    def _native_name(self, user, kind, rid, version):
        return (
            re.sub("[^A-Za-z0-9_-]", "_", rid)[:40]
            + "_"
            + self._native_id(user, kind, rid, version)[:12]
        )

    async def _asset(self, kind, rid, version, *, assets=None):
        """从资产存储读取确切版本。"""
        self._kind(kind)
        self._identity(rid, version)
        row = await (assets or self.storage.assets).get(kind, rid, version)
        if row is None:
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
    def _scope_allows(user, asset, scope):
        """当前 Agent 的定义明确绑定该资源版本时才允许继续校验。"""
        if scope is None or scope.user_id != user.user_id:
            return False
        current = scope.definition
        root = scope.root_definition
        pinned = (
            root if root["definition_type"] == "agent"
            else scope.member_definitions.get(current["id"])
        )
        identity = (asset["resource_type"], asset["resource_id"], asset["resource_version"])
        return pinned is not None and all(
            any(
                (ref["resource_type"], ref["resource_id"], ref["resource_version"])
                == identity
                for ref in definition["payload"].get("bindings", [])
            )
            for definition in (pinned, current)
        )

    async def _usable(self, user, asset, scope=None):
        """按独立授权或当前执行图校验版本使用权。"""
        if not user.is_enabled or not asset["is_enabled"]:
            return False
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
            await self.permissions.require_execution(
                user, scope.root_definition, allow_pinned=True
            )
            return True
        if scope is not None:
            if getattr(scope, "user_id", None) != user.user_id:
                return False
            personal = getattr(scope, "definition", {}).get("agent_type") == "personal"
            if not personal:
                if not self._scope_allows(user, asset, scope):
                    return False
                await self.permissions.require_execution(
                    user, scope.root_definition, allow_pinned=True
                )
                return True
            installed = await self.installations.assets(
                user,
                asset["resource_type"],
                enabled_only=True,
                resource_ids=[asset["resource_id"]],
            )
            selected = installed.get(asset["resource_id"])
            if not selected:
                return False
            if (
                asset["resource_type"] != "skill"
                and selected["resource_version"] != asset["resource_version"]
            ):
                return False
        own = asset["owner_type"] == "user" and asset["owner_id"] == user.user_id
        view = (
            "private"
            if own
            else "system"
            if asset["owner_type"] == "system"
            else "shared"
        )
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
        if not await self._usable(user, asset, scope):
            raise Forbidden("未获得此资源版本的使用权限")

    @staticmethod
    def _editable(user, asset):
        return user.is_enabled and (
            (asset["owner_type"] == "user" and asset["owner_id"] == user.user_id)
            or (asset["owner_type"] == "system" and user.role_type == "admin")
        )

    @contextmanager
    def _access(self, user, asset):
        token = _hub_access.set(
            (
                user.user_id,
                asset["resource_type"],
                asset["resource_id"],
                asset["resource_version"],
            )
        )
        try:
            yield
        finally:
            _hub_access.reset(token)

    async def _view(self, user, asset, *, source=False, admin=False):
        """组装资源目录或详情的字段与权限标记。"""
        own = asset["owner_type"] == "user" and asset["owner_id"] == user.user_id
        view = (
            "private"
            if own
            else "system"
            if asset["owner_type"] == "system"
            else "shared"
        )
        if view == "shared" and not admin and not await self._shared_asset(user, asset):
            raise Forbidden("此资源版本尚未共享")
        system_default = self.tool_registry.is_default_tool(asset)
        agent_tool = self.tool_registry.is_agent_tool(asset)
        installation = await self.installations.selection(user, asset)
        payload = {
            k: v
            for k, v in asset["payload"].items()
            if k
            in {
                "display_name",
                "description",
                "is_skill_market_visible",
                "avatar",
                "category",
                "input_schema",
                "source",
                "exposure",
                "tool_name",
            }
        }
        usable = await self._usable(user, asset)
        editable = self._editable(user, asset)
        if source:
            if not usable and not editable and not self._tool_card_visible(user, asset):
                raise Forbidden()
            if asset["resource_type"] == "skill":
                payload["markdown"] = asset["payload"]["original_markdown"]
                payload["files"] = asset["payload"]["files"]
                payload["base_version"] = asset["resource_version"]
            elif asset["resource_type"] == "mcp":
                payload["config"] = self.mcp_registry.config(asset)
        revision = asset.get("publication_revision")
        share = (
            await self.permissions.share_availability(
                user,
                asset["resource_type"],
                asset["resource_id"],
                version=asset["resource_version"],
                view=view,
            )
            if editable and asset["resource_type"] != "tool"
            else {}
        )
        return {
            "resource_id": asset["resource_id"],
            "resource_version": asset["resource_version"],
            "version_label": publication_label(revision)
            if revision
            else asset["resource_version"],
            **share,
            "owner_type": asset["owner_type"],
            "owner_id": asset["owner_id"],
            "display_name": payload.get("display_name", asset["resource_id"]),
            "description": payload.get("description", ""),
            "resource_view_type": view,
            "is_usable": usable,
            "is_editable": editable,
            "is_deletable": editable,
            "is_available": asset["is_enabled"],
            "auth_type": asset["payload"].get("config", {}).get("auth_type", "none")
            if asset["resource_type"] == "mcp"
            else None,
            "is_system_default": system_default,
            "is_agent_tool": agent_tool,
            "is_installed": system_default or installation is not None,
            "is_enabled": asset["is_enabled"]
            if asset["resource_type"] == "tool"
            else bool(installation and installation.enabled),
            "is_skill_market_visible": bool(
                payload.get("is_skill_market_visible", False)
            )
            if asset["resource_type"] == "skill"
            else None,
            "payload": payload,
        }

    async def list(self, user, resource_type):
        """列出资源目录，工具目录只包含可见的平台和专家工具。"""
        self._kind(resource_type)
        if not user.is_enabled:
            raise Forbidden()
        rows = await self._versions(resource_type)
        selected = await self._latest_visible_assets(user, resource_type, rows)
        return [
            await self._view(user, asset)
            for asset in sorted(selected.values(), key=lambda a: a["resource_id"])
        ]

    async def _latest_visible_assets(self, user, resource_type, rows):
        """按各类资源的可见条件选择最新目录版本。"""
        selected = {}
        ranks = {}
        for asset in rows:
            own = asset["owner_type"] == "user" and asset["owner_id"] == user.user_id
            usable = False
            if resource_type == "tool":
                if not self._tool_card_visible(user, asset):
                    continue
            else:
                usable = await self._usable(user, asset)
                if not self._marketplace_visible(user, asset, own=own, usable=usable):
                    continue
            # 在可见版本中优先选择可用版本，再按发布版本排序。
            rid = asset["resource_id"]
            rank = (
                usable,
                asset["is_enabled"],
                asset.get("publication_revision") or 0,
                Version(asset["resource_version"]),
            )
            if rid not in ranks or rank > ranks[rid]:
                selected[rid], ranks[rid] = asset, rank
        return selected

    def _marketplace_visible(self, user, asset, *, own, usable):
        """目录只展示可独立使用或有维护权的资源版本。"""
        if not usable and not self._editable(user, asset):
            return False
        return (
            asset["resource_type"] != "skill"
            or own
            or user.role_type == "admin"
            or asset["payload"].get("is_skill_market_visible", False)
        )

    def _tool_card_visible(self, user, asset):
        """只展示已启用的平台默认工具或专家绑定候选。"""
        return (
            user.is_enabled
            and asset["is_enabled"]
            and (
                self.tool_registry.is_default_tool(asset)
                or self.tool_registry.is_agent_tool(asset)
            )
        )

    async def detail(self, user, resource_type, resource_id, version):
        return await self._view(
            user, await self._asset(resource_type, resource_id, version), source=True
        )

    async def _owner_for_save(self, user, registry, rid, version, data):
        """发布前核对资源归属以及版本是否已存在。"""
        if not user.is_enabled:
            raise Forbidden()
        rows = await registry.list(resource_ids=[rid])
        if any(row["owner_type"] == "system" for row in rows):
            require_admin(user)
        if any(
            row["owner_type"] == "user" and row["owner_id"] != user.user_id
            for row in rows
        ):
            raise Forbidden()
        if any(row["resource_version"] == version for row in rows):
            raise Conflict("资源版本不可变，请发布新版本")
        if rows:
            return rows[0]["owner_type"], rows[0]["owner_id"]
        if data.get("owner_type") == "system":
            require_admin(user)
            return "system", data.get("owner_id", "system")
        if data.get("owner_id", user.user_id) != user.user_id:
            raise Forbidden()
        return "user", user.user_id

    async def _save(
        self,
        user,
        kind,
        data,
        *,
        files=None,
        config=None,
        import_job_id=None,
        progress=None,
        archive_limits=None,
    ):
        """原子发布资源版本、附件和归属授权。"""
        rid = data.get("resource_id") or kind + "-" + uuid4().hex
        version = data.get("resource_version") or "1.0.0"
        self._identity(rid, version)
        registry_type = LocalSkillRegistry if kind == "skill" else LocalMCPRegistry
        metadata = {
            "dependency_only": data.get("dependency_only") is True,
            "avatar": catalog_icon_id(
                data.get("avatar"), default="book" if kind == "skill" else "code"
            ),
            "display_name": str(data.get("display_name") or rid)[:200],
            "description": str(data.get("description") or "")[:2000],
        }
        archive = None
        if kind == "skill":
            if progress:
                await progress("preparing", 35, resource_version=version)
            metadata.update(
                category=str(data.get("category") or "other")[:80],
                original_markdown=files["SKILL.md"].decode("utf-8"),
                files=[
                    {"path": name, "size": len(value)}
                    for name, value in sorted(files.items())
                ],
                is_skill_market_visible=bool(data.get("is_skill_market_visible", True)),
            )
            if import_job_id:
                metadata["import_job_id"] = import_job_id
            limits = archive_limits or self.archive_limits
            if sum(map(len, files.values())) > limits.expanded_bytes_limit:
                raise ValueError("Skill exceeds upload limit")
            archive, digest = await asyncio.to_thread(
                lambda: (_zip(files, rid), _files_digest(files))
            )
            if len(archive) > limits.max_bytes:
                raise ValueError("Skill archive exceeds upload limit")
        else:
            metadata["config"] = config
            digest = mcp_config_checksum(config)
        if progress:
            await progress("registering", 65)
        async with self.storage.definition_write() as write:
            registry = registry_type(write.assets)
            await write.assets.lock_resource(kind, rid)
            existing = (
                await registry.get_version(rid, version) if import_job_id else None
            )
            if existing:
                if (
                    existing["owner_id"] != user.user_id
                    or existing["payload"].get("import_job_id") != import_job_id
                ):
                    raise Conflict("Import identity already exists")
                asset = existing
            else:
                revision = await write.assets.next_publication_revision(kind, rid)
                if kind == "mcp" and not data.get("resource_version"):
                    version = publication_label(revision) + ".0"
                owner_type, owner_id = await self._owner_for_save(
                    user, registry, rid, version, data
                )
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
                await registry.register(model, actor=user.user_id, archive=archive)
                asset = model.model_dump()
                await GrantService(write).record_owner(
                    kind, rid, asset, actor=user.user_id
                )
        return await self._view(user, asset)

    @staticmethod
    def _skill_data(markdown, data, *, require_version=False):
        front = _skill_front(markdown)
        metadata = front.get("metadata")
        nested_version = metadata.get("version") if isinstance(metadata, dict) else None
        version = front.get("version", nested_version)
        if (
            front.get("version") is not None
            and nested_version is not None
            and str(front["version"]) != str(nested_version)
        ):
            raise ValueError("Skill version fields in SKILL.md must agree")
        supplied = data.get("resource_version")
        if version is not None:
            version = str(version)
            if supplied and supplied != version:
                raise ValueError("Skill version must match SKILL.md front matter")
        elif require_version:
            raise ValueError("SKILL.md front matter version is required")
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
            raise ValueError("Skill markdown is required")
        if len(markdown.encode()) > limits.expanded_bytes_limit:
            raise ValueError("Skill exceeds upload limit")
        data = await asyncio.to_thread(self._skill_data, markdown, data)
        files = {}
        rid = data.get("resource_id")
        base_version = data.get("base_version")
        if rid:
            versions = await self._versions("skill", resource_ids=[rid])
            if versions:
                if any(not self._editable(user, asset) for asset in versions):
                    raise Forbidden()
                if any(
                    asset["resource_version"] == data["resource_version"]
                    for asset in versions
                ):
                    raise Conflict("资源版本不可变，请发布新版本")
                if not base_version:
                    raise ValueError(
                        "A base version is required to edit an existing Skill"
                    )
        if base_version:
            base = await self._asset("skill", rid, base_version)
            if not self._editable(user, base):
                raise Forbidden()
            content = await self.download_skill(
                user, rid, base_version, for_edit=True, archive_limits=limits
            )
            files = await asyncio.to_thread(_archive_files, content, limits)
            for key in ("avatar", "category", "is_skill_market_visible"):
                data.setdefault(key, base["payload"].get(key))
        files["SKILL.md"] = markdown.encode()
        return await self._save(user, "skill", data, files=files, archive_limits=limits)

    async def upload_skill(
        self,
        user,
        archive,
        version=None,
        is_skill_market_visible=True,
        *,
        category="other",
        avatar="book",
        resource_id=None,
        import_job_id=None,
        progress=None,
        owner_type=None,
        owner_id=None,
    ):
        limits = self.archive_limits
        files = await asyncio.to_thread(_archive_files, archive, limits)
        markdown = files["SKILL.md"].decode("utf-8")
        data = await asyncio.to_thread(
            self._skill_data,
            markdown,
            {
                "resource_version": version,
                "category": category,
                "avatar": avatar,
                "is_skill_market_visible": is_skill_market_visible,
                **({"resource_id": resource_id} if resource_id else {}),
                **({"owner_type": owner_type} if owner_type else {}),
                **({"owner_id": owner_id} if owner_id else {}),
            },
            require_version=version is None,
        )
        kwargs = {"files": files, "archive_limits": limits}
        if import_job_id:
            kwargs.update(
                import_job_id=import_job_id,
                progress=progress,
            )
        return await self._save(user, "skill", data, **kwargs)

    async def save_mcp(self, user, data):
        config = MCPAuthorizationService.validate_config(data.get("config"))
        return await self._save(user, "mcp", data, config=config)

    async def skill_descriptors(self, user, refs, authorization_scope=None):
        result = []
        for ref in refs:
            ref = (
                ref if isinstance(ref, ResourceRef) else ResourceRef.model_validate(ref)
            )
            if ref.resource_type != "skill":
                continue
            asset = await self._asset("skill", ref.resource_id, ref.resource_version)
            await self._require(user, asset, authorization_scope)
            result.append(
                Skill(
                    name=self._native_name(
                        user, "skill", ref.resource_id, ref.resource_version
                    ),
                    description=asset["payload"].get("description", ""),
                    dir="",
                    markdown="",
                    updated_at=0,
                )
            )
        return result

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
        limits = archive_limits or self.archive_limits
        asset = await self._asset("skill", resource_id, version)
        if not (for_edit and self._editable(user, asset)):
            await self._require(user, asset, authorization_scope)
        with self._access(user, asset):
            archive = await self.skill_hub.download(
                user.user_id, asset["resource_id"], version
            )
        content = await self._read_skill_archive(archive, limits.max_bytes)
        files = await asyncio.to_thread(_archive_files, content, limits)
        checksum = await asyncio.to_thread(_files_digest, files)
        if checksum != asset["checksum_sha256"]:
            raise Conflict("Skill source checksum changed")
        return await asyncio.to_thread(_zip, files, resource_id)

    @staticmethod
    async def _read_skill_archive(archive, maximum):
        if archive.format != "zip":
            raise ValueError("Skill source must be ZIP")
        chunks = []
        size = 0
        try:
            async for chunk in archive.stream:
                size += len(chunk)
                if size > maximum:
                    raise ValueError("Skill download exceeds configured limit")
                chunks.append(chunk)
        finally:
            close = getattr(archive.stream, "aclose", None)
            if close is not None:
                await close()
        return b"".join(chunks)

    @staticmethod
    def _skill_cache_parts(user, asset, scope):
        def segment(value):
            if (
                not isinstance(value, str)
                or value in (".", "..")
                or not re.fullmatch(r"[\w.@+-]{1,128}", value)
            ):
                raise Forbidden("Skill cache path contains an invalid segment")
            return value

        definition = getattr(scope, "definition", None) if scope is not None else None
        if definition and definition.get("agent_type") != "personal":
            identifier = segment(definition.get("id"))
            number = definition.get("config_version")
            if type(number) is not int or number < 1:
                raise Forbidden("Skill cache definition version is invalid")
            version = f"config-{number}"
            return (
                "common",
                identifier,
                version,
                "skills",
                segment(asset["resource_id"]),
            )
        if asset["owner_type"] == "user" and asset["owner_id"] == user.user_id:
            return (
                "user",
                segment(user.user_id),
                "skills",
                segment(asset["resource_id"]),
            )
        return ("common", "skills", segment(asset["resource_id"]))

    @staticmethod
    def _validate_skill_cache(destination, checksum):
        if destination.is_symlink():
            raise Forbidden("Skill cache cannot use symlinks")
        if not destination.is_dir():
            raise Conflict("Skill cache destination is not a directory")
        cached = {}
        for path in destination.rglob("*"):
            if path.is_symlink():
                raise Forbidden("Skill cache contains a symlink")
            if path.is_file():
                cached[path.relative_to(destination).as_posix()] = path.read_bytes()
            elif not path.is_dir():
                raise Forbidden("Skill cache contains a special file")
        if _files_digest(cached) != checksum:
            raise Conflict("Skill cache integrity check failed")

    async def materialize_skill(
        self, user, resource_id, version, *, authorization_scope=None
    ):
        limits = self.archive_limits
        asset = await self._asset("skill", resource_id, version)
        await self._require(user, asset, authorization_scope)
        parts = self._skill_cache_parts(user, asset, authorization_scope)
        lock = self._cache_locks.setdefault((resource_id, version), asyncio.Lock())
        async with lock:
            # Materialize the verified bytes shown in the editor and export.
            # The digest selects a new cache only when the content changes.
            content = await self.download_skill(
                user,
                resource_id,
                version,
                authorization_scope=authorization_scope,
                archive_limits=limits,
            )
            files = _archive_files(content, limits)
            checksum = _files_digest(files)
            base = self.workspace_root
            for part in parts:
                base = base / part
                if base.is_symlink():
                    raise Forbidden("Skill cache cannot use symlink directories")
                base.mkdir(parents=True, exist_ok=True)
            destination = base / (version + "-" + checksum[:20])
            if destination.is_symlink():
                raise Forbidden("Skill cache cannot use symlinks")
            if destination.exists():
                self._validate_skill_cache(destination, checksum)
            else:
                staging = Path(tempfile.mkdtemp(prefix=".skill-", dir=base))
                try:
                    for name, content in files.items():
                        path = staging / name
                        path.parent.mkdir(parents=True, exist_ok=True)
                        path.write_bytes(content)
                    try:
                        os.rename(staging, destination)
                    except OSError as exc:
                        if exc.errno not in (errno.EEXIST, errno.ENOTEMPTY):
                            raise
                    # A competing process may have installed the winner; never
                    # trust that directory until it is checked in full.
                    self._validate_skill_cache(destination, checksum)
                finally:
                    if staging.exists():
                        shutil.rmtree(staging)
            return Skill(
                name=self._native_name(user, "skill", resource_id, version),
                description=asset["payload"].get("description", ""),
                dir=str(destination),
                markdown=files["SKILL.md"].decode("utf-8"),
                updated_at=destination.stat().st_mtime,
            )

    async def skill_pin(self, user, ref, skill):
        asset = await self._asset("skill", ref["resource_id"], ref["resource_version"])
        if not asset["checksum_sha256"]:
            raise Conflict("技能缺少注册内容校验值，无法固定会话版本")
        destination = Path(skill.dir)

        def digest():
            files = {
                p.relative_to(destination).as_posix(): p.read_bytes()
                for p in destination.rglob("*")
                if p.is_file()
            }
            checksum = _files_digest(files)
            self._validate_skill_cache(destination, checksum)
            return checksum

        checksum = await asyncio.to_thread(digest)
        return {
            **ref,
            "source_checksum_sha256": asset["checksum_sha256"],
            "cache_checksum_sha256": checksum,
            "cache_dir": destination.relative_to(self.workspace_root).as_posix(),
            "skill_name": skill.name,
        }

    async def restore_skill(self, user, pin, *, authorization_scope):
        """Restore only the exact server-pinned content, never a newer version."""
        pin = SkillActivation.model_validate(pin).model_dump()
        rid, version = pin["resource_id"], pin["resource_version"]
        if pin["skill_name"] != self._native_name(user, "skill", rid, version):
            raise Forbidden("已激活技能名称无效")
        asset = await self._asset("skill", rid, version)
        await self._require(user, asset, authorization_scope)
        if asset["checksum_sha256"] != pin["source_checksum_sha256"]:
            raise Conflict("已激活技能的注册内容已变化，不能自动替换会话版本")
        relative = PurePosixPath(pin["cache_dir"])
        if relative.is_absolute() or ".." in relative.parts:
            raise Forbidden("已激活技能目录无效")
        destination = self.workspace_root / relative
        checksum = pin["cache_checksum_sha256"]
        if (
            destination.name != version + "-" + checksum[:20]
            or destination.parent.name != rid
            or destination.parent.parent.name != "skills"
            or destination.resolve() != destination
            or not destination.is_relative_to(self.workspace_root)
        ):
            raise Forbidden("已激活技能目录无效")
        if not destination.exists():
            skill = await self.materialize_skill(
                user, rid, version, authorization_scope=authorization_scope
            )
            recovered = await self.skill_pin(user, pin, skill)
            if recovered["cache_checksum_sha256"] != checksum:
                raise Conflict("已激活技能的缓存内容已变化，不能自动替换会话版本")
            return skill
        await asyncio.to_thread(self._validate_skill_cache, destination, checksum)
        return Skill(
            name=self._native_name(user, "skill", rid, version),
            description=asset["payload"].get("description", ""),
            dir=str(destination),
            markdown=(destination / "SKILL.md").read_text("utf-8"),
            updated_at=destination.stat().st_mtime,
        )

    async def mcps_for_refs(
        self, user, refs, authorization_scope=None, *, skip_unavailable=False
    ):
        result = []
        for ref in refs:
            ref = (
                ref if isinstance(ref, ResourceRef) else ResourceRef.model_validate(ref)
            )
            if ref.resource_type != "mcp":
                continue
            try:
                asset = await self._asset("mcp", ref.resource_id, ref.resource_version)
                await self._require(user, asset, authorization_scope)
                client = await self._mcp_client(user, asset)
            except AgentKitError:
                if not skip_unavailable:
                    raise
                continue
            result.append(client)
        return result

    async def _mcp_client(self, user, asset):
        version = asset["resource_version"]
        card = self.mcp_hub.card(asset)
        _verify_mcp_card(asset, card)
        client = render_mcp(
            card, {}, name=self._native_name(user, "mcp", asset["resource_id"], version)
        )
        headers = await self.mcp_authorization.headers(user, asset)
        client.mcp_config.headers = headers or None
        return AuthorizedMCPClient(**client.model_dump())

    async def authorize_mcp_tool(self, user, scope, tool):
        from agentscope.tool import MCPTool

        if not isinstance(tool, MCPTool):
            return False
        client = getattr(tool, "_egis_mcp_client", None)
        if client is None or client.name != tool.mcp_name:
            raise Forbidden("MCP 工具没有受管的连接来源")
        for row in await self._versions("mcp"):
            if client.name != self._native_name(
                user, "mcp", row["resource_id"], row["resource_version"]
            ):
                continue
            await self._require(user, row, scope)
            current = await self._mcp_client(user, row)
            if client.mcp_config != current.mcp_config:
                raise Forbidden("连接凭证已变化，请重新发起对话")
            return True
        raise Forbidden("当前会话没有此 MCP 连接")

    async def mcp_dependency_authorizations(self, user):
        """给有 Agent 依赖的用户提供凭据入口，广场安装权限保持独立。"""
        result = []
        for rid, version in await self.permissions.granted_dependency_versions(
            user, "mcp"
        ):
            asset = await self._asset("mcp", rid, version)
            if await self._usable(user, asset):
                continue
            auth_type = self.mcp_registry.config(asset).get("auth_type", "none")
            if auth_type != "none":
                result.append(
                    {
                        "resource_id": rid,
                        "resource_version": version,
                        "display_name": asset["payload"].get("display_name") or rid,
                        "auth_type": auth_type,
                    }
                )
        return result

    async def mcp_auth_action(self, user, rid, version, action, **kwargs):
        asset = await self._asset("mcp", rid, version)
        if action != "revoke" and not await self._usable(user, asset):
            if not await self.permissions.granted_dependency_versions(
                user, "mcp", resource_id=rid, version=version
            ):
                raise Forbidden("未获得此资源版本的使用权限")
        return await getattr(self.mcp_authorization, action)(user, asset, **kwargs)

    async def test_mcp(self, user, resource_id, version):
        clients = await self.mcps_for_refs(
            user,
            [
                ResourceRef(
                    resource_type="mcp",
                    resource_id=resource_id,
                    resource_version=version,
                )
            ],
        )
        client = clients[0]
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
