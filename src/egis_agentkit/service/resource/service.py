"""Enterprise resource ownership and installs; contents remain in Nacos."""

from __future__ import annotations

import asyncio
import errno
import hashlib
import io
import json
import os
import re
import shutil
import stat
import tempfile
import zipfile
from contextlib import contextmanager
from contextvars import ContextVar
from pathlib import Path, PurePosixPath
from urllib.parse import urlsplit
from uuid import uuid4

import yaml
from agentscope.app._service._mcp_render import render_mcp
from agentscope.app.storage import MCPRecord, SkillRecord
from agentscope.skill import Skill
from packaging.version import Version
from sqlalchemy import or_, select, text

from egis_agentkit.catalog_icons import catalog_icon_id
from egis_agentkit.config.limits import DEFAULT_SKILL_ARCHIVE_MAX_BYTES
from egis_agentkit.errors import AgentKitError, Conflict, Forbidden, NotFound
from egis_agentkit.models import Asset, ResourceRef, utcnow
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.resource.activation import SkillActivation
from egis_agentkit.service.user.service import UserService, require_admin
from egis_agentkit.storage import tables as t

ResourceView = dict
_hub_access: ContextVar[tuple | None] = ContextVar(
    "agentkit_resource_hub_access", default=None
)


def mcp_config_checksum(config: dict) -> str:
    """Canonical identity for a credential-free HTTP MCP transport and URL."""
    if not isinstance(config, dict) or config.get("transport") != "streamable_http":
        raise ValueError("Only streamable HTTP MCP configuration is supported")
    try:
        parsed = urlsplit(config["url"])
        if (
            parsed.scheme not in {"http", "https"}
            or not parsed.hostname
            or parsed.username
            or parsed.password
            or parsed.query
            or parsed.fragment
            or parsed.port == 0
        ):
            raise ValueError
        host = parsed.hostname.lower()
        if ":" in host:
            host = "[" + host + "]"
        port = parsed.port or (443 if parsed.scheme == "https" else 80)
        canonical = {
            "transport": "streamable_http",
            "url": f"{parsed.scheme}://{host}:{port}{parsed.path or '/mcp'}",
        }
    except (KeyError, ValueError, TypeError, AttributeError):
        raise ValueError("Invalid MCP endpoint") from None
    return hashlib.sha256(json.dumps(canonical, sort_keys=True).encode()).hexdigest()


def _verify_mcp_card(asset, card):
    try:
        config = card.config_template
        actual = mcp_config_checksum(
            {"transport": "streamable_http", "url": config.url}
        )
    except (ValueError, TypeError, AttributeError):
        raise Conflict("MCP configuration is not a pinned HTTP endpoint") from None
    if not asset["checksum_sha256"] or actual != asset["checksum_sha256"]:
        raise Conflict("MCP source checksum changed or is missing")


def _archive_files(
    archive: bytes, maximum: int = DEFAULT_SKILL_ARCHIVE_MAX_BYTES
) -> dict[str, bytes]:
    if len(archive) > maximum:
        raise ValueError("Skill archive exceeds upload limit")
    try:
        with zipfile.ZipFile(io.BytesIO(archive)) as z:
            entries = z.infolist()
            if sum(e.file_size for e in entries) > maximum:
                raise ValueError("Expanded Skill archive exceeds limit")
            files = {}
            for entry in entries:
                path = PurePosixPath(entry.filename)
                if (
                    path.is_absolute()
                    or ".." in path.parts
                    or "\\" in entry.filename
                    or stat.S_ISLNK(entry.external_attr >> 16)
                ):
                    raise ValueError("Unsafe Skill archive path")
                if entry.is_dir():
                    continue
                if not path.parts or ":" in path.parts[0] or path.as_posix() in files:
                    raise ValueError("Invalid or duplicate Skill archive path")
                files[path.as_posix()] = z.read(entry)
            manifests = [
                name for name in files if PurePosixPath(name).name == "SKILL.md"
            ]
            if len(manifests) != 1:
                raise ValueError("Exactly one SKILL.md is required")
            prefix = PurePosixPath(manifests[0]).parent
            if prefix != PurePosixPath("."):
                if any(
                    not PurePosixPath(name).is_relative_to(prefix) for name in files
                ):
                    raise ValueError(
                        "All Skill files must belong to the manifest directory"
                    )
                files = {
                    str(PurePosixPath(name).relative_to(prefix)): value
                    for name, value in files.items()
                }
            return files
    except (zipfile.BadZipFile, RuntimeError, UnicodeError):
        raise ValueError("Invalid Skill ZIP archive") from None


def _skill_front(markdown: str) -> dict:
    try:
        match = re.match(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|$)", markdown, re.S)
        front = yaml.safe_load(match.group(1)) if match else {}
        if not isinstance(front, dict):
            raise ValueError("Invalid Skill front matter")
        return front
    except yaml.YAMLError:
        raise ValueError("Invalid Skill front matter") from None


def _files_digest(files: dict[str, bytes]) -> str:
    digest = hashlib.sha256()
    for name, value in sorted(files.items()):
        for part in (name.encode(), value):
            digest.update(len(part).to_bytes(8, "big"))
            digest.update(part)
    return digest.hexdigest()


def skill_archive_digest(archive: bytes) -> str:
    """Stable content identity independent of generated ZIP timestamps/root folder."""
    return _files_digest(_archive_files(archive))


def _zip(files: dict[str, bytes], name: str) -> bytes:
    result = io.BytesIO()
    with zipfile.ZipFile(result, "w", zipfile.ZIP_DEFLATED) as z:
        for path, content in sorted(files.items()):
            z.writestr(name + "/" + path, content)
    return result.getvalue()


def _prepare_skill_archive(files, source_name, version, metadata):
    """Parse, rewrite, pack and hash a source away from the async event loop."""
    files = dict(files)
    original = files.get("SKILL.md", b"").decode("utf-8")
    manifest = re.match(r"\A---\r?\n(.*?)\r?\n---(?:\r?\n|$)", original, re.S)
    body = original[manifest.end():] if manifest else original
    front = {
        **_skill_front(original),
        "name": source_name,
        "description": metadata["description"],
        "version": version,
        "is_skill_market_visible": metadata["is_skill_market_visible"],
    }
    files["SKILL.md"] = (
        "---\n" + yaml.safe_dump(front, sort_keys=False) + "---\n" + body
    ).encode()
    return _zip(files, source_name), _files_digest(files)


class ResourceService:
    def __init__(
        self, storage, skill_hub, mcp_hub, management, workspace_root, policies=None
    ):
        self.storage, self.skill_hub, self.mcp_hub, self.management = (
            storage,
            skill_hub,
            mcp_hub,
            management,
        )
        self.workspace_root = Path(workspace_root).resolve()
        self.policies = policies
        self.permissions = PermissionService(storage, policies)
        self._cache_locks = {}

    @property
    def max_skill_archive_bytes(self):
        return getattr(
            self.policies, "max_skill_archive_bytes", DEFAULT_SKILL_ARCHIVE_MAX_BYTES
        )

    @staticmethod
    def _kind(kind):
        if kind not in {"skill", "mcp"}:
            raise ValueError("Resource type must be skill or mcp")
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

    @staticmethod
    def _source_name(asset):
        return asset["payload"].get("nacos_name", asset["resource_id"])

    async def _asset(self, kind, rid, version, *, conn=None):
        self._kind(kind)
        self._identity(rid, version)
        query = select(t.assets).where(
            t.assets.c.resource_type == kind,
            t.assets.c.resource_id == rid,
            t.assets.c.resource_version == version,
        )
        if conn is None:
            async with self.storage.engine.connect() as connection:
                row = (await connection.execute(query)).mappings().first()
        else:
            row = (await conn.execute(query)).mappings().first()
        if row is None:
            raise NotFound()
        return dict(row)

    async def _review_asset(self, asset):
        async with self.storage.engine.connect() as conn:
            row = (
                await conn.execute(
                    select(t.reviews.c.snapshot).where(
                        t.reviews.c.resource_type == asset["resource_type"],
                        t.reviews.c.resource_id == asset["resource_id"],
                        t.reviews.c.is_current,
                        t.reviews.c.review_status_type == "approved",
                    )
                )
            ).scalar_one_or_none()
        approved = (row or {}).get("asset")
        if (
            not approved
            or approved.get("resource_version") != asset["resource_version"]
        ):
            return None
        return approved

    def _scope_allows(self, user, asset, scope):
        if scope is None or getattr(scope, "user_id", None) != user.user_id:
            return False
        snapshot = getattr(scope, "snapshot", {})
        nodes = snapshot.get("nodes", {})
        binding = getattr(scope, "binding", {}) or {}
        pending = [binding.get("agent_node_key", snapshot.get("root_key"))]
        seen = set()
        while pending:
            key = pending.pop()
            if key in seen:
                continue
            seen.add(key)
            node = nodes.get(key, {})
            data = node.get("data", {})
            if (
                node.get("resource_type") == asset["resource_type"]
                and node.get("resource_id") == asset["resource_id"]
                and data.get("resource_version") == asset["resource_version"]
            ):
                return (
                    data.get("owner_type") == asset["owner_type"]
                    and data.get("owner_id") == asset["owner_id"]
                    and data.get("checksum_sha256", "") == asset["checksum_sha256"]
                )
            pending.extend(node.get("edges", []))
        return False

    async def _usable(self, user, asset, scope=None):
        if not user.is_enabled or not asset["is_enabled"]:
            return False
        personal_skill = (
            scope is not None
            and getattr(scope, "definition", {}).get("agent_type") == "personal"
            and asset["resource_type"] == "skill"
        )
        if personal_skill:
            installed = await self.installed_assets(
                user, "skill", enabled_only=True, resource_ids=[asset["resource_id"]]
            )
            if asset["resource_id"] not in installed:
                return False
        if self._scope_allows(user, asset, scope):
            await self.permissions.validate_snapshot(user, scope.snapshot)
            return True
        if scope is not None:
            # Expert scopes are closed even when the user has another direct
            # USE grant. Personal assistants additionally discover only their
            # own enabled native installations, with current authorization.
            if (
                getattr(scope, "user_id", None) != user.user_id
                or getattr(scope, "definition", {}).get("agent_type") != "personal"
            ):
                return False
            if asset["resource_type"] != "skill":
                installed = await self._installation(user, asset)
                if installed is None or not installed.enabled:
                    return False
        own = asset["owner_type"] == "user" and asset["owner_id"] == user.user_id
        view = (
            "private"
            if own
            else "system"
            if asset["owner_type"] == "system"
            else "shared"
        )
        if view == "shared" and not await self._review_asset(asset):
            # Only a server-persisted activation may retain a previously shared
            # version. Current USE and current installation are still required.
            pins = (getattr(scope, "binding", {}) or {}).get("activated_skills", {})
            pin = pins.get(asset["resource_id"], {})
            if (
                pin.get("resource_version") != asset["resource_version"]
                or pin.get("source_checksum_sha256") != asset["checksum_sha256"]
                or pin.get("resource_view_type") != "shared"
            ):
                return False
        return (
            await self.permissions.explain(
                user, asset["resource_type"], asset["resource_id"], view
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
                self._source_name(asset),
                asset["resource_version"],
            )
        )
        try:
            yield
        finally:
            _hub_access.reset(token)

    async def authorize_hub(self, user_id, resource_type, resource_id, operation):
        if resource_type not in {"skill", "mcp"} or operation not in {
            "list",
            "get",
            "download",
        }:
            return False
        user = await UserService(self.storage).get(user_id)
        active = _hub_access.get()
        if active and active[:3] == (user_id, resource_type, resource_id):
            return True
        async with self.storage.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        select(t.assets).where(
                            t.assets.c.resource_type == resource_type,
                            t.assets.c.is_enabled,
                            or_(
                                t.assets.c.resource_id == resource_id,
                                t.assets.c.payload["nacos_name"].astext == resource_id,
                            ),
                        )
                    )
                )
                .mappings()
                .all()
            )
        # The native callback has no version argument. Other users' shared sources
        # are admitted only inside an exact-version enterprise operation above.
        for row in rows:
            if row["owner_type"] == "user" and row["owner_id"] != user_id:
                continue
            if await self._usable(user, dict(row)):
                return True
        return False

    async def _installation(self, user, asset):
        identifier = self._native_id(
            user,
            asset["resource_type"],
            asset["resource_id"],
            asset["resource_version"],
        )
        getter = (
            self.storage.get_skill
            if asset["resource_type"] == "skill"
            else self.storage.get_mcp
        )
        return await getter(user.user_id, identifier)

    async def _view(self, user, asset, *, source=False):
        own = asset["owner_type"] == "user" and asset["owner_id"] == user.user_id
        view = (
            "private"
            if own
            else "system"
            if asset["owner_type"] == "system"
            else "shared"
        )
        if view == "shared" and not await self._review_asset(asset):
            raise Forbidden("此资源版本尚未共享")
        installation = await self._installation(user, asset)
        payload = {
            k: v
            for k, v in asset["payload"].items()
            if k in {"display_name", "description", "is_skill_market_visible", "avatar", "category"}
        }
        usable = await self._usable(user, asset)
        editable = self._editable(user, asset)
        if source:
            if not usable and not editable:
                raise Forbidden()
            if asset["resource_type"] == "skill":
                payload["markdown"] = asset["payload"].get("original_markdown")
                if payload["markdown"] is None:
                    archive = await self.download_skill(user, asset["resource_id"], asset["resource_version"], for_edit=editable)
                    files = await asyncio.to_thread(_archive_files, archive, self.max_skill_archive_bytes)
                    payload["markdown"] = files["SKILL.md"].decode("utf-8")
                    payload["files"] = [{"path": name, "size": len(value)} for name, value in sorted(files.items())]
                else:
                    payload["files"] = asset["payload"].get("files", [])
                payload["base_version"] = asset["resource_version"]
            else:
                result = await self._backend_read(
                    "mcp",
                    mcpName=self._source_name(asset),
                    version=asset["resource_version"],
                )
                endpoints = result.get("backendEndpoints") or []
                if endpoints:
                    endpoint = endpoints[0]
                    host = endpoint["address"]
                    scheme = (
                        endpoint.get("protocol")
                        or (result.get("remoteServerConfig") or {})
                        .get("serviceRef", {})
                        .get("transportProtocol")
                        or "http"
                    )
                    if ":" in host and not host.startswith("["):
                        host = "[" + host + "]"
                    path = (
                        endpoint.get("path")
                        or (result.get("remoteServerConfig") or {}).get("exportPath")
                        or "/mcp"
                    )
                    payload["config"] = {
                        "transport": "streamable_http",
                        "url": f"{scheme}://{host}:{endpoint['port']}{path}",
                    }
        return {
            "resource_id": asset["resource_id"],
            "resource_version": asset["resource_version"],
            "owner_type": asset["owner_type"],
            "owner_id": asset["owner_id"],
            "display_name": payload.get("display_name", asset["resource_id"]),
            "description": payload.get("description", ""),
            "resource_view_type": view,
            "is_usable": usable,
            "is_editable": editable,
            "is_deletable": editable,
            "is_installed": installation is not None,
            "is_enabled": bool(installation and installation.enabled),
            "is_skill_market_visible": bool(
                payload.get("is_skill_market_visible", False)
            )
            if asset["resource_type"] == "skill"
            else None,
            "payload": payload,
        }

    async def _backend_read(self, path, **params):
        client = self.management.client
        return client.data(
            await client.request(
                "GET",
                "/nacos/v3/admin/ai/" + path,
                params={"namespaceId": client.bootstrap.namespace_id, **params},
            )
        )

    async def installed_assets(
        self, user, resource_type, rows=None, *, enabled_only=False, resource_ids=None
    ):
        """User selection comes from native installation records, not the hub.

        Old per-version records are retained. The most recently installed or
        toggled record is authoritative, including a disabled selection (never
        silently fall back to an older enabled version).
        """
        self._kind(resource_type)
        records = await (
            self.storage.list_skills(user.user_id)
            if resource_type == "skill"
            else self.storage.list_mcps(user.user_id)
        )
        records = {record.id: record for record in records}
        if rows is None:
            statement = select(t.assets).where(t.assets.c.resource_type == resource_type)
            if resource_ids is not None:
                statement = statement.where(t.assets.c.resource_id.in_(resource_ids))
            async with self.storage.engine.connect() as conn:
                rows = (await conn.execute(statement)).mappings().all()
        selected = {}
        for row in rows:
            asset = dict(row)
            record = records.get(self._native_id(
                user, resource_type, asset["resource_id"], asset["resource_version"]
            ))
            if record is None:
                continue
            # Native records historically contain both naive and aware dates.
            rank = (record.updated_at.replace(tzinfo=None), asset["resource_version"])
            previous = selected.get(asset["resource_id"])
            if previous is None or rank > previous[0]:
                selected[asset["resource_id"]] = (rank, asset, record.enabled)
        return {
            rid: asset for rid, (_, asset, enabled) in selected.items()
            if not enabled_only or enabled
        }

    async def list(self, user, resource_type):
        self._kind(resource_type)
        if not user.is_enabled:
            raise Forbidden()
        async with self.storage.engine.connect() as conn:
            rows = [
                dict(row)
                for row in (
                    await conn.execute(
                        select(t.assets).where(
                            t.assets.c.resource_type == resource_type
                        )
                    )
                ).mappings()
            ]
        selected = await self._latest_visible_assets(user, resource_type, rows)
        # Installed cards continue to describe/control the user's chosen
        # version even when a newer catalog version has been published.
        installed = (
            await self.installed_assets(user, resource_type, rows)
            if resource_type == "skill" else {}
        )
        for rid, asset in installed.items():
            own = asset["owner_type"] == "user" and asset["owner_id"] == user.user_id
            if asset["owner_type"] == "system" or own or await self._review_asset(asset):
                selected[rid] = asset
        return [
            await self._view(user, asset)
            for asset in sorted(selected.values(), key=lambda a: a["resource_id"])
        ]

    async def _latest_visible_assets(self, user, resource_type, rows):
        """One canonical catalog version per resource, shared with discovery."""
        selected = {}
        for asset in rows:
            own = asset["owner_type"] == "user" and asset["owner_id"] == user.user_id
            if (
                asset["owner_type"] == "user"
                and not own
                and not await self._review_asset(asset)
            ):
                continue
            if (
                resource_type == "skill"
                and not own
                and user.role_type != "admin"
                and not asset["payload"].get("is_skill_market_visible", False)
            ):
                continue
            previous = selected.get(asset["resource_id"])
            if previous is None or Version(asset["resource_version"]) > Version(
                previous["resource_version"]
            ):
                selected[asset["resource_id"]] = asset
        return selected

    async def detail(self, user, resource_type, resource_id, version):
        return await self._view(
            user, await self._asset(resource_type, resource_id, version), source=True
        )

    async def _owner_for_save(self, user, kind, rid, version, data, conn):
        if not user.is_enabled:
            raise Forbidden()
        rows = (
            (
                await conn.execute(
                    select(t.assets).where(
                        t.assets.c.resource_type == kind, t.assets.c.resource_id == rid
                    )
                )
            )
            .mappings()
            .all()
        )
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

    async def _publish_private(self, asset):
        kind = asset["resource_type"]
        name = self._source_name(asset)
        version = asset["resource_version"]
        if asset["owner_type"] == "user":
            if getattr(self.policies, "allow_private_self_use", True):
                # Technical publication of an immutable PRIVATE source is independent
                # of enterprise sharing; it never creates a grant or approval record.
                method = (
                    self.management.force_publish_skill
                    if kind == "skill"
                    else self.management.force_publish_mcp
                )
                await method(name, version)
        else:
            method = (
                self.management.publish_skill
                if kind == "skill"
                else self.management.publish_mcp
            )
            await method(name, version)

    async def _save(self, user, kind, data, files=None, config=None, *, import_job_id=None, progress=None, remote_uploaded=False):
        rid = data.get("resource_id") or kind + "-" + uuid4().hex
        version = data.get("resource_version") or data.get("version") or "1.0.0"
        self._identity(rid, version)
        avatar = catalog_icon_id(
            data.get("avatar"), default="book" if kind == "skill" else "code"
        )
        async with self.storage.transaction() as conn:
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:identity,0))"),
                {"identity": kind + ":" + rid},
            )
            if import_job_id:
                existing = (await conn.execute(select(t.assets).where(
                    t.assets.c.resource_type == kind, t.assets.c.resource_id == rid,
                    t.assets.c.resource_version == version,
                ))).mappings().first()
                if existing:
                    if existing["owner_id"] != user.user_id or existing["payload"].get("import_job_id") != import_job_id:
                        raise Conflict("Import identity already exists")
                    return await self._view(user, dict(existing))
            owner_type, owner_id = await self._owner_for_save(
                user, kind, rid, version, data, conn
            )
            source_name = (
                rid
                if owner_type == "system"
                else "egis-"
                + kind
                + "-"
                + hashlib.sha256((owner_id + "\0" + rid).encode()).hexdigest()[:24]
            )
            metadata = {
                "nacos_name": source_name,
                "avatar": avatar,
                "display_name": str(data.get("display_name") or rid)[:200],
                "description": str(data.get("description") or "")[:2000],
            }
            if kind == "skill":
                metadata["category"] = str(data.get("category") or "other")[:80]
                metadata["original_markdown"] = files["SKILL.md"].decode("utf-8")
                metadata["files"] = [{"path": name, "size": len(value)} for name, value in sorted(files.items())]
                if import_job_id:
                    metadata["import_job_id"] = import_job_id
                metadata["is_skill_market_visible"] = bool(
                    data.get("is_skill_market_visible", True)
                )
            asset = Asset(
                resource_type=kind,
                resource_id=rid,
                resource_version=version,
                owner_type=owner_type,
                owner_id=owner_id,
                payload=metadata,
            )
            if kind == "skill":
                if progress:
                    await progress("preparing", 35, resource_version=version)
                archive, asset.checksum_sha256 = await asyncio.to_thread(
                    _prepare_skill_archive, files, source_name, version, metadata
                )
                if progress:
                    await progress("registering", 55)
                if not remote_uploaded:
                    await self.management.upload_skill(archive, version, overwrite=bool(import_job_id))
                    if progress:
                        await progress("registered", 75, remote_uploaded=True)
            else:
                parsed = urlsplit(config["url"])
                port = parsed.port or (443 if parsed.scheme == "https" else 80)
                specification = {
                    "protocol": "streamable",
                    "frontProtocol": "streamable",
                    "description": metadata["description"],
                    "remoteServerConfig": {"exportPath": parsed.path or "/mcp"},
                }
                endpoint = {
                    "type": "DIRECT",
                    "data": {
                        "address": parsed.hostname,
                        "port": str(port),
                        "transportProtocol": parsed.scheme,
                        "path": parsed.path or "/mcp",
                    },
                }
                asset.checksum_sha256 = mcp_config_checksum(config)
                await self.management.create_mcp(
                    source_name, version, specification, endpoint_specification=endpoint
                )
            if progress:
                await progress("publishing", 85)
            await self._publish_private(asset.model_dump())
            await conn.execute(
                t.assets.insert().values(
                    **asset.model_dump(),
                    created_by=user.user_id,
                    updated_by=user.user_id,
                )
            )
        return await self._view(user, asset.model_dump())

    @staticmethod
    def _skill_data(markdown, data, *, require_version=False):
        front = _skill_front(markdown)
        metadata = front.get("metadata")
        nested_version = metadata.get("version") if isinstance(metadata, dict) else None
        version = front.get("version", nested_version)
        if front.get("version") is not None and nested_version is not None and str(front["version"]) != str(nested_version):
            raise ValueError("Skill version fields in SKILL.md must agree")
        supplied = data.get("resource_version") or data.get("version")
        if version is not None:
            version = str(version)
            if supplied and supplied != version:
                raise ValueError("Skill version must match SKILL.md front matter")
        elif require_version:
            raise ValueError("SKILL.md front matter version is required")
        else:
            version = supplied or "1.0.0"
        return {
            **data, "resource_version": version,
            "display_name": front.get("name") or data.get("display_name") or "Skill",
            "description": front.get("description", data.get("description", "")),
        }

    async def save_skill(self, user, data):
        markdown = data.get("markdown", "")
        if not isinstance(markdown, str) or not markdown.strip():
            raise ValueError("Skill markdown is required")
        if len(markdown.encode()) > self.max_skill_archive_bytes:
            raise ValueError("Skill exceeds upload limit")
        data = await asyncio.to_thread(self._skill_data, markdown, data)
        files = {}
        rid = data.get("resource_id")
        base_version = data.get("base_version")
        if rid:
            async with self.storage.engine.connect() as conn:
                versions = (await conn.execute(select(t.assets).where(
                    t.assets.c.resource_type == "skill", t.assets.c.resource_id == rid,
                ))).mappings().all()
            if versions:
                if any(not self._editable(user, asset) for asset in versions):
                    raise Forbidden()
                if any(asset["resource_version"] == data["resource_version"] for asset in versions):
                    raise Conflict("资源版本不可变，请发布新版本")
                # Legacy clients omit base_version. Preserve the latest archive
                # instead of replacing the Skill with a single document.
                base_version = base_version or max(versions, key=lambda asset: Version(asset["resource_version"]))["resource_version"]
        if base_version:
            base = await self._asset("skill", rid, base_version)
            if not self._editable(user, base):
                raise Forbidden()
            content = await self.download_skill(user, rid, base_version, for_edit=True)
            files = await asyncio.to_thread(_archive_files, content, self.max_skill_archive_bytes)
            for key in ("avatar", "category", "is_skill_market_visible"):
                data.setdefault(key, base["payload"].get(key))
        files["SKILL.md"] = markdown.encode()
        return await self._save(user, "skill", data, files=files)

    async def upload_skill(self, user, archive, version=None, is_skill_market_visible=True,
                           *, category="other", avatar="book", resource_id=None,
                           import_job_id=None, progress=None, remote_uploaded=False):
        files = await asyncio.to_thread(_archive_files, archive, self.max_skill_archive_bytes)
        markdown = files["SKILL.md"].decode("utf-8")
        data = await asyncio.to_thread(self._skill_data, markdown, {
            "resource_version": version, "category": category, "avatar": avatar,
            "is_skill_market_visible": is_skill_market_visible,
            **({"resource_id": resource_id} if resource_id else {}),
        }, require_version=version is None)
        kwargs = {"files": files}
        if import_job_id:
            kwargs.update(import_job_id=import_job_id, progress=progress, remote_uploaded=remote_uploaded)
        return await self._save(user, "skill", data, **kwargs)

    async def save_mcp(self, user, data):
        config = data.get("config")
        if (
            not isinstance(config, dict)
            or set(config) != {"transport", "url"}
            or config.get("transport") != "streamable_http"
        ):
            raise ValueError(
                "Only a credential-free streamable HTTP MCP endpoint is supported"
            )
        try:
            parsed = urlsplit(config["url"])
            if (
                parsed.scheme not in {"http", "https"}
                or not parsed.hostname
                or parsed.username
                or parsed.password
                or parsed.query
                or parsed.fragment
                or parsed.port == 0
            ):
                raise ValueError
        except (ValueError, TypeError, AttributeError):
            raise ValueError(
                "Invalid MCP endpoint; credentials and query secrets are not accepted"
            ) from None
        return await self._save(user, "mcp", data, config=config)

    async def install(self, user, resource_type, resource_id, version):
        asset = await self._asset(resource_type, resource_id, version)
        await self._require(user, asset)
        identifier = self._native_id(user, resource_type, resource_id, version)
        name = self._native_name(user, resource_type, resource_id, version)
        with self._access(user, asset):
            if resource_type == "skill":
                card = await self.skill_hub.get_skill_version(
                    user.user_id, self._source_name(asset), version
                )
                record = SkillRecord(
                    id=identifier,
                    user_id=user.user_id,
                    name=name,
                    display_name=asset["payload"].get("display_name"),
                    description=card.description,
                    markdown=card.markdown or "",
                    hub_id=self.skill_hub.hub_id,
                    card_id=self._source_name(asset),
                    version=version,
                    enabled=True,
                )
                await self.storage.upsert_skill(user.user_id, record)
            else:
                card = await self.mcp_hub.get_mcp_version(
                    user.user_id, self._source_name(asset), version
                )
                _verify_mcp_card(asset, card)
                client = render_mcp(card, {}, name=name)
                record = MCPRecord(
                    id=identifier,
                    user_id=user.user_id,
                    client=client,
                    display_name=asset["payload"].get("display_name"),
                    description=card.description,
                    hub_id=self.mcp_hub.hub_id,
                    card_id=self._source_name(asset),
                    version=version,
                    enabled=True,
                )
                await self.storage.upsert_mcp(user.user_id, record)
        return await self._view(user, asset)

    async def set_installation(
        self, user, resource_type, resource_id, version, is_enabled
    ):
        asset = await self._asset(resource_type, resource_id, version)
        await self._require(user, asset)
        record = await self._installation(user, asset)
        if record is None:
            raise NotFound("资源尚未安装")
        record.enabled = bool(is_enabled)
        record.updated_at = utcnow()
        method = (
            self.storage.upsert_skill
            if resource_type == "skill"
            else self.storage.upsert_mcp
        )
        await method(user.user_id, record)
        return await self._view(user, asset)

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
        self, user, resource_id, version, *, authorization_scope=None, raw_source=False, for_edit=False
    ):
        asset = await self._asset("skill", resource_id, version)
        if not (for_edit and self._editable(user, asset)):
            await self._require(user, asset, authorization_scope)
        with self._access(user, asset):
            archive = await self.skill_hub.download(
                user.user_id, self._source_name(asset), version
            )
        if archive.format != "zip":
            raise ValueError("Nacos Skill source must be ZIP")
        chunks = []
        size = 0
        maximum = self.max_skill_archive_bytes
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
        content = b"".join(chunks)
        files = await asyncio.to_thread(_archive_files, content, maximum)
        checksum = await asyncio.to_thread(_files_digest, files)
        if asset["checksum_sha256"] and checksum != asset["checksum_sha256"]:
            raise Conflict("Skill source checksum changed")
        original = asset["payload"].get("original_markdown")
        if original is not None and not raw_source:
            files["SKILL.md"] = original.encode("utf-8")
            return await asyncio.to_thread(_zip, files, asset["resource_id"])
        return content

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
            implementation = definition.get("payload", {}).get("implementation")
            if implementation:
                version = segment(implementation.get("assembly_version"))
            else:
                number = definition.get("config_version")
                if type(number) is not int or number < 1:
                    raise Forbidden("Skill cache definition version is invalid")
                version = segment(f"config-{number}")
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
        asset = await self._asset("skill", resource_id, version)
        await self._require(user, asset, authorization_scope)
        parts = self._skill_cache_parts(user, asset, authorization_scope)
        lock = self._cache_locks.setdefault((resource_id, version), asyncio.Lock())
        async with lock:
            # Nacos transport rewrites frontmatter for registry identity. Runtime
            # must use the exact same original SKILL.md as editor/export, after
            # verifying the transport checksum inside download_skill(). The
            # restored content digest naturally selects a new immutable cache.
            content = await self.download_skill(
                user, resource_id, version, authorization_scope=authorization_scope
            )
            maximum = self.max_skill_archive_bytes
            files = _archive_files(content, maximum)
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
            files = {p.relative_to(destination).as_posix(): p.read_bytes()
                     for p in destination.rglob("*") if p.is_file()}
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
        if (destination.name != version + "-" + checksum[:20]
                or destination.parent.name != rid
                or destination.parent.parent.name != "skills"
                or destination.resolve() != destination
                or not destination.is_relative_to(self.workspace_root)):
            raise Forbidden("已激活技能目录无效")
        if not destination.exists():
            skill = await self.materialize_skill(user, rid, version, authorization_scope=authorization_scope)
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

    async def mcps_for_refs(self, user, refs, authorization_scope=None):
        result = []
        for ref in refs:
            ref = (
                ref if isinstance(ref, ResourceRef) else ResourceRef.model_validate(ref)
            )
            if ref.resource_type != "mcp":
                continue
            asset = await self._asset("mcp", ref.resource_id, ref.resource_version)
            await self._require(user, asset, authorization_scope)
            with self._access(user, asset):
                card = await self.mcp_hub.get_mcp_version(
                    user.user_id, self._source_name(asset), ref.resource_version
                )
            _verify_mcp_card(asset, card)
            result.append(
                render_mcp(
                    card,
                    {},
                    name=self._native_name(
                        user, "mcp", ref.resource_id, ref.resource_version
                    ),
                )
            )
        return result

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
