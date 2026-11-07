"""Immutable execution/review closures. Views never borrow private latest edges."""

from copy import deepcopy

from sqlalchemy import select

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service.agent.views import (
    DefinitionViews,
    require_current_shared_asset,
)
from egis_agentkit.storage import tables as t


def node_key(resource_type, resource_id, view, version=None):
    key = f"{resource_type}:{view}:{resource_id}"
    return f"{key}:{version}" if version is not None else key


def normalize_nodes(nodes):
    """Upgrade legacy asset keys and reject conflicting graph identities."""
    remap = {}
    for key, node in nodes.items():
        if node["resource_type"] not in ("agent", "team"):
            version = node["data"].get("resource_version")
            if not version:
                raise Conflict("资源快照缺少版本")
            remap[key] = node_key(
                node["resource_type"],
                node["resource_id"],
                node["resource_view_type"],
                version,
            )
        else:
            remap[key] = key
    result = {}
    for key, value in nodes.items():
        node = deepcopy(value)
        try:
            node["edges"] = [remap[edge] for edge in node.get("edges", [])]
        except KeyError as exc:
            raise Conflict("执行快照引用不完整") from exc
        new_key = remap[key]
        if new_key in result and result[new_key] != node:
            raise Conflict("执行快照节点冲突")
        result[new_key] = node
    return result, remap


class SnapshotService:
    def __init__(self, storage):
        self.storage = storage

    async def capture(self, definition_id, view, *, conn=None):
        if conn is None:
            async with self.storage.engine.connect() as connection:
                return await self.capture(definition_id, view, conn=connection)
        if view == "shared":
            review = (
                (
                    await conn.execute(
                        select(t.reviews).where(
                            t.reviews.c.resource_id == definition_id,
                            t.reviews.c.is_current,
                            t.reviews.c.review_status_type == "approved",
                        )
                    )
                )
                .mappings()
                .first()
            )
            if review is None:
                raise Forbidden("资源尚未共享")
            # Always consult the source tombstone, but never its current payload.
            source = await self.storage.get_definition(definition_id, conn=conn)
            if not source["is_enabled"]:
                raise Forbidden("资源已停用")
            result = deepcopy(review["snapshot"])
            result["nodes"], remap = normalize_nodes(result["nodes"])
            result["root_key"] = remap[result["root_key"]]
            old_root_key = result["root_key"]
            new_root_key = node_key(source["definition_type"], definition_id, "shared")
            result["nodes"][new_root_key] = result["nodes"].pop(old_root_key)
            result["nodes"][new_root_key]["resource_view_type"] = "shared"
            result["root_key"] = new_root_key
            result["root"]["resource_view_type"] = "shared"
            result["root"]["shared_review_id"] = review["review_id"]
            result["root"]["shared_version"] = review["shared_version"]
            return result
        root = await DefinitionViews(self.storage).raw_view(definition_id, view, conn=conn)
        snapshot = {
            "root": {**root, "resource_view_type": view},
            "root_key": node_key(root["definition_type"], root["id"], view),
            "nodes": {},
            "source_versions": {},
        }
        await self._visit(root, view, snapshot, set(), conn)
        return snapshot

    async def _visit(self, data, view, snapshot, visiting, conn):
        key = node_key(data["definition_type"], data["id"], view)
        if key in visiting:
            raise Conflict("定义引用存在循环")
        if key in snapshot["nodes"]:
            if snapshot["nodes"][key]["data"] != data:
                raise Conflict("执行快照节点冲突")
            return
        visiting = visiting | {key}
        edges = []
        if data["definition_type"] == "team":
            refs = [data["payload"]["orchestrator"], *data["payload"]["members"]]
            for ref in refs:
                child_view = ref["resource_view_type"]
                child = await DefinitionViews(self.storage).raw_view(
                    ref["definition_id"], child_view, conn=conn
                )
                child_key = node_key("agent", child["id"], child_view)
                edges.append(child_key)
                if child_view == "shared":
                    sub = await self.capture(child["id"], "shared", conn=conn)
                    nodes, _ = normalize_nodes(sub["nodes"])
                    for sub_key, sub_node in nodes.items():
                        existing = snapshot["nodes"].get(sub_key)
                        if existing is not None and existing != sub_node:
                            raise Conflict("执行快照节点冲突")
                        snapshot["nodes"][sub_key] = sub_node
                else:
                    await self._visit(child, child_view, snapshot, visiting, conn)
        else:
            for ref in data["payload"].get("bindings", []):
                row = (
                    (
                        await conn.execute(
                            select(t.assets).where(
                                t.assets.c.resource_type == ref["resource_type"],
                                t.assets.c.resource_id == ref["resource_id"],
                                t.assets.c.resource_version == ref["resource_version"],
                                t.assets.c.is_enabled,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
                if row is None:
                    raise NotFound("绑定资源指定版本不存在或已停用")
                if ref["resource_view_type"] == "shared":
                    await require_current_shared_asset(conn, row)
                asset_key = node_key(
                    ref["resource_type"],
                    ref["resource_id"],
                    ref["resource_view_type"],
                    ref["resource_version"],
                )
                asset = {
                    k: row[k]
                    for k in (
                        "resource_type",
                        "resource_id",
                        "resource_version",
                        "owner_type",
                        "owner_id",
                        "download_url",
                        "checksum_sha256",
                        "entrypoint",
                        "is_enabled",
                        "payload",
                    )
                }
                node = {
                    "resource_type": ref["resource_type"],
                    "resource_id": ref["resource_id"],
                    "resource_view_type": ref["resource_view_type"],
                    "data": asset,
                    "edges": [],
                }
                existing = snapshot["nodes"].get(asset_key)
                if existing is not None and existing != node:
                    raise Conflict("执行快照节点冲突")
                snapshot["nodes"][asset_key] = node
                edges.append(asset_key)
        snapshot["nodes"][key] = {
            "resource_type": data["definition_type"],
            "resource_id": data["id"],
            "resource_view_type": view,
            "data": deepcopy(data),
            "edges": edges,
        }
        if view == "private":
            snapshot["source_versions"][data["id"]] = data["config_version"]

    @staticmethod
    def path(snapshot, target):
        def walk(key, parents):
            if key in parents:
                return None
            node = snapshot["nodes"][key]
            value = {
                k: node[k]
                for k in ("resource_type", "resource_id", "resource_view_type")
            }
            if all(value[k] == target[k] for k in value):
                return [value]
            for child in node["edges"]:
                path = walk(child, parents | {key})
                if path:
                    return [value, *path]
            return None

        return walk(snapshot["root_key"], set())
