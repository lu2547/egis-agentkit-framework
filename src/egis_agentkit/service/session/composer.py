"""Bounded composer discovery using existing authorization and workspace adapters."""

import asyncio
import base64
import hashlib
import json
from itertools import islice

from sqlalchemy import select

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service.access.snapshot import SnapshotService
from egis_agentkit.service.team.collaboration import collaboration_allowed
from egis_agentkit.storage import tables as t


class ComposerService:
    def __init__(self, storage, agents, resources, workspace):
        self.storage = storage
        self.agents = agents
        self.resources = resources
        self.workspace = workspace

    async def options(
        self,
        user,
        category,
        *,
        query="",
        cursor=None,
        limit=10,
        session_id=None,
        project_id=None,
    ):
        if not user.is_enabled:
            raise Forbidden()
        if category not in {"context", "experts", "skills", "commands"}:
            raise ValueError("Unknown composer category")
        if not 1 <= limit <= 10:
            raise ValueError("Composer limit must be between 1 and 10")
        query = query.strip().casefold()
        scope = hashlib.sha256(
            json.dumps(
                [user.user_id, category, query, session_id, project_id],
                ensure_ascii=False,
            ).encode()
        ).hexdigest()[:24]
        after = self._decode_cursor(cursor, scope) if cursor else None
        personal = can_invite = True
        if session_id:
            session = await self.workspace.tasks.session(user, session_id)
            binding = session.state.middle_context["egis_agentkit"]
            can_invite = collaboration_allowed(session, binding)
            snapshot = binding["snapshot"]
            root = snapshot["nodes"][snapshot["root_key"]]["data"]
            personal = (
                not binding.get("is_team") and root.get("agent_type") == "personal"
            )
            # A session always supplies the authoritative project scope.
            if project_id and project_id != binding["project_id"]:
                raise Forbidden("项目与会话不匹配")
            project_id = binding["project_id"]
        if category == "experts" and can_invite:
            items = await self._experts(user, query, after, limit + 1)
        elif category == "skills" and personal:
            items = await self._skills(user, query, after, limit + 1)
        elif category == "context" and project_id:
            async with self.workspace.projects.guard(user, project_id):
                items = await asyncio.to_thread(
                    self._files, user.user_id, project_id, query, after, limit + 1
                )
        else:
            # There is no slash-command registry. Never relabel tools as commands.
            items = []
        more = len(items) > limit
        items = items[:limit]
        return {
            "items": items,
            "next_cursor": self._encode_cursor(scope, items[-1]["id"])
            if more
            else None,
        }

    @staticmethod
    def _encode_cursor(scope, after):
        return base64.urlsafe_b64encode(json.dumps([scope, after]).encode()).decode()

    @staticmethod
    def _decode_cursor(cursor, scope):
        try:
            saved_scope, after = json.loads(base64.urlsafe_b64decode(cursor))
            if saved_scope != scope or not isinstance(after, str):
                raise ValueError
            return after
        except (ValueError, TypeError, UnicodeError):
            raise ValueError("Invalid composer cursor") from None

    @staticmethod
    def _matches(query, name, detail, identifier=""):
        return not query or query in f"{name} {detail} {identifier}".casefold()

    async def _experts(self, user, query, after, count):
        result = []
        scanned = after or ""
        while len(result) < count:
            # Authorization can reject many candidates; keep each DB read bounded.
            async with self.storage.engine.connect() as conn:
                rows = (
                    (
                        await conn.execute(
                            select(t.definitions)
                            .where(
                                t.definitions.c.definition_type == "agent",
                                t.definitions.c.agent_type == "expert",
                                t.definitions.c.is_enabled,
                                t.definitions.c.id > scanned,
                            )
                            .order_by(t.definitions.c.id)
                            .limit(50)
                        )
                    )
                    .mappings()
                    .all()
                )
            if not rows:
                break
            for row in rows:
                scanned = row["id"]
                own = row["owner_type"] == "user" and row["owner_id"] == user.user_id
                view = (
                    "private"
                    if own
                    else "system"
                    if row["owner_type"] == "system"
                    else "shared"
                )
                try:
                    value = await self.agents.resolve(user, row["id"], view)
                    snapshot = await SnapshotService(self.storage).capture(
                        row["id"], view
                    )
                    await self.resources.permissions.validate_snapshot(user, snapshot)
                except (Conflict, Forbidden, NotFound):
                    continue
                if value.get("agent_type") != "expert" or not value.get(
                    "is_enabled", True
                ):
                    continue
                name = value["agent_display_name"] or value["agent_name"]
                detail = value["payload"].get("description", "")
                if self._matches(query, name, detail, value["agent_name"]):
                    result.append(
                        {
                            "id": row["id"],
                            "name": name,
                            "detail": detail,
                            "kind": "expert",
                            "definition": {
                                **value,
                                "resource_view_type": view,
                                "is_usable": True,
                                "is_editable": own or user.role_type == "admin",
                            },
                        }
                    )
                    if len(result) == count:
                        break
        return result

    async def _skills(self, user, query, after, count):
        result = []
        scanned = after.rsplit("@", 1)[0] if after else ""
        while len(result) < count:
            async with self.storage.engine.connect() as conn:
                # Page resources, not versions. Otherwise an older installed
                # version can reappear after a cursor/page boundary.
                identifiers = list(
                    (
                        await conn.execute(
                            select(t.assets.c.resource_id)
                            .where(
                                t.assets.c.resource_type == "skill",
                                t.assets.c.resource_id > scanned,
                            )
                            .distinct()
                            .order_by(t.assets.c.resource_id)
                            .limit(50)
                        )
                    ).scalars()
                )
                if not identifiers:
                    break
                rows = (
                    (
                        await conn.execute(
                            select(t.assets)
                            .where(
                                t.assets.c.resource_type == "skill",
                                t.assets.c.resource_id.in_(identifiers),
                            )
                            .order_by(
                                t.assets.c.resource_id, t.assets.c.resource_version
                            )
                        )
                    )
                    .mappings()
                    .all()
                )
            selected = await self.resources.installed_assets(
                user, "skill", rows, enabled_only=True
            )
            for identifier in identifiers:
                scanned = identifier
                asset = selected.get(identifier)
                if asset is None or not asset["is_enabled"]:
                    continue
                installed = await self.resources._installation(user, asset)
                if installed is None or not installed.enabled:
                    continue
                if not await self.resources._usable(user, asset):
                    continue
                name = asset["payload"].get("display_name") or asset["resource_id"]
                detail = asset["payload"].get("description", "")
                if self._matches(query, name, detail, asset["resource_id"]):
                    result.append(
                        {
                            "id": f"{identifier}@{asset['resource_version']}",
                            "name": name,
                            "detail": detail,
                            "kind": "skill",
                            "insert_text": f"请使用{name}技能 ",
                        }
                    )
                    if len(result) == count:
                        break
        return result

    def _files(self, user_id, project_id, query, after, count):
        files = self.workspace.files

        def metadata(root, path="", ignored_root=()):
            # Reuse the no-follow directory adapter. Content scan byte ceilings
            # do not apply to metadata pages. Directory suffixes preserve the
            # same lexical order as full file paths across nested folders.
            entries = files._list_root(root, path, ignored_root=ignored_root)
            for item in sorted(
                entries,
                key=lambda item: item["path"] + ("/" if item["is_directory"] else ""),
            ):
                if item["is_directory"]:
                    yield from metadata(root, item["path"], ignored_root)
                else:
                    yield item

        def candidates():
            for area in ("artifacts", "uploads"):
                root = files.area_root(user_id, project_id, area)
                ignored = files.RESERVED if area == "uploads" else ()
                for item in metadata(root, ignored_root=ignored):
                    reference = (
                        item["path"]
                        if area == "uploads"
                        else "artifacts/" + item["path"]
                    )
                    identifier = f"file:{area}:{item['path']}"
                    if after and identifier <= after:
                        continue
                    if self._matches(query, item["name"], reference):
                        yield {
                            "id": identifier,
                            "name": item["name"],
                            "detail": reference,
                            "kind": "file",
                            "reference": reference,
                        }

        # Stop metadata enumeration as soon as a page and next-page marker exist.
        return list(islice(candidates(), count))
