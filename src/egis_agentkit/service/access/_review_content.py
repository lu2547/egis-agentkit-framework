"""Build review-only snapshots; never confer resource execution permission."""

import copy
import hashlib
import json
from urllib.parse import urlsplit, urlunsplit

from egis_agentkit.errors import AgentKitError, NotFound
from egis_agentkit.storage.model import utcnow
from egis_agentkit.storage.model._base import publication_label
from egis_agentkit.storage.model._resource import MCP_STORAGE_VERSION


def _safe_connector_config(config):
    safe = {key: config[key] for key in (
        "transport", "auth_type", "key_header", "oauth_client_id", "oauth_scope"
    ) if key in config}
    try:
        url = urlsplit(config.get("url", ""))
        host = url.hostname or ""
        if ":" in host:
            host = f"[{host}]"
        if url.port:
            host += f":{url.port}"
        safe["url"] = urlunsplit((url.scheme, host, url.path, "", ""))
    except ValueError:
        safe["url"] = "[invalid endpoint]"
    return safe


def _node(row, *, origin):
    kind = row.get("definition_type", row.get("resource_type"))
    version = row["resource_version"]
    if kind in ("agent", "team"):
        payload = copy.deepcopy(row["payload"])
        name = row.get(f"{kind}_display_name") or row[f"{kind}_name"]
        checksum = hashlib.sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()
        revision = int(version)
    else:
        payload = {key: copy.deepcopy(row["payload"][key]) for key in (
            "display_name", "description", "files", "category", "avatar"
        ) if key in row["payload"]}
        name = payload.get("display_name", row["resource_id"])
        checksum, revision = row["checksum_sha256"], row.get("publication_revision")
        if kind == "mcp":
            payload["config"] = _safe_connector_config(row["payload"].get("config", {}))
    return {
        "resource_type": kind,
        "resource_id": row["id"] if kind in ("agent", "team") else row["resource_id"],
        "resource_version": version,
        "version_label": publication_label(revision) if revision else version,
        "display_name": name,
        "owner_type": row["owner_type"], "owner_id": row["owner_id"],
        "checksum_sha256": checksum,
        "configuration_revision": revision if kind == "mcp" else None,
        "origin": origin,
        "payload": payload,
    }


async def capture_content(storage, grants, kind, resource, *, origin="submitted", require_available=True):
    """Freeze definitions and exact dependency metadata, never archive bytes."""
    nodes = {}
    if kind in ("agent", "team"):
        members, skills = await grants.definition_dependencies(
            resource, write=storage, require_available=require_available
        )
        for member in members:
            node = _node(member, origin=origin)
            if member["id"] != resource["id"]:
                nodes[("agent", member["id"], member["resource_version"])] = node
        for skill in skills.values():
            node = _node(skill, origin=origin)
            nodes[("skill", skill["resource_id"], skill["resource_version"])] = node
        references = {
            (ref["resource_type"], ref["resource_id"],
             MCP_STORAGE_VERSION if ref["resource_type"] == "mcp" else ref["resource_version"])
            for member in members for ref in member["payload"].get("bindings", [])
            if ref["resource_type"] != "skill"
        }
        for ref in sorted(references):
            asset = await storage.assets.get(*ref)
            if asset is None:
                raise NotFound("审核依赖资源不存在")
            nodes[ref] = _node(asset, origin=origin)
    return {
        "origin": origin,
        "captured_at": utcnow().isoformat() if origin == "submitted" else None,
        "root": _node(resource, origin=origin),
        "dependencies": list(nodes.values()),
    }


async def with_skill_documents(storage, snapshot):
    """Read verified immutable files using review scope, including unshared NAS copies."""
    from egis_agentkit.service.resource._catalog import ResourceService

    snapshot = copy.deepcopy(snapshot)
    files = storage.history.files
    resources = ResourceService(storage, files.root, config=files.config)
    for node in [snapshot["root"], *snapshot["dependencies"]]:
        if node["resource_type"] != "skill":
            continue
        asset = {**node, "payload": node["payload"]}
        for scope in ("owner", "shared"):
            try:
                node["payload"]["markdown"] = await resources.read_skill_markdown(
                    {**asset, "storage_scope": scope}
                )
                break
            except (OSError, UnicodeError, AgentKitError, ValueError):
                continue
        else:
            node["content_error"] = "技能版本文件暂不可读取，请恢复文件后审核"
    return snapshot
