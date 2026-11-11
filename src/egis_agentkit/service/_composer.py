"""聚合输入区可选择的专家、技能与项目文件。"""

import asyncio
import base64
import hashlib
import json
import re
from itertools import islice
from pathlib import PurePosixPath

from egis_agentkit.backend.nacos._settings import ComposerOptionsConfig
from egis_agentkit.errors import Conflict, Forbidden, InvalidRequest, NotFound
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.team._collaboration import (
    collaboration_allowed,
)
from egis_agentkit.storage.model._definition import (
    EXPERT_TYPES,
)

_CURSOR_SCOPE_DIGEST_LENGTH = 24


class ComposerService:
    """列出当前输入区和已绑定会话可以使用的选项。"""

    def __init__(
        self, storage, agent_query, resources, workspace, *, config_provider=None
    ):
        self.config = config_provider
        self.storage = storage
        self.agent_query = agent_query
        self.resources = resources
        self.workspace = workspace

    async def options(
        self,
        user,
        category,
        *,
        query="",
        cursor=None,
        limit=None,
        session_id=None,
        project_id=None,
    ):
        """校验项目范围，并根据固定定义类型筛选选项。"""
        if not user.is_enabled:
            raise Forbidden()
        if category not in {"context", "experts", "skills", "commands"}:
            raise ValueError("Unknown composer category")
        configuration = self.config.snapshot if self.config else None
        settings = (
            configuration.session.composer if configuration else ComposerOptionsConfig()
        )
        limit = settings.default_page_size if limit is None else limit
        if not 1 <= limit <= settings.max_page_size:
            raise InvalidRequest(
                f"输入区分页大小必须介于 1 和 {settings.max_page_size} 之间"
            )
        if len(query) > settings.max_query_length:
            raise InvalidRequest(f"输入区查询最多 {settings.max_query_length} 个字符")
        if cursor is not None and len(cursor) > settings.max_cursor_length:
            raise InvalidRequest(f"输入区游标最多 {settings.max_cursor_length} 个字符")
        query = query.strip().casefold()
        scope = hashlib.sha256(
            json.dumps(
                [user.user_id, category, query, session_id, project_id],
                ensure_ascii=False,
            ).encode()
        ).hexdigest()[:_CURSOR_SCOPE_DIGEST_LENGTH]
        after = self._decode_cursor(cursor, scope) if cursor else None
        personal = can_invite = True
        if session_id:
            session = await self.workspace.sessions.session(user, session_id)
            binding = session.state.middle_context["egis_agentkit"]
            root = await self.storage.definitions.read_reference(binding)
            can_invite = collaboration_allowed(session, root)
            personal = root.get("agent_type") == "personal"
            # 会话项目决定输入区可访问的文件范围。
            if project_id and project_id != binding["project_id"]:
                raise Forbidden("项目与会话不匹配")
            project_id = binding["project_id"]
        if category == "experts" and can_invite:
            permissions = (
                PermissionService(self.storage, configuration.access)
                if configuration
                else None
            )
            items = await self._experts(
                user,
                query,
                after,
                limit + 1,
                settings.candidate_batch_size,
                permissions,
            )
        elif category == "skills" and personal:
            items = await self._skills(
                user, query, after, limit + 1, settings.candidate_batch_size
            )
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

    async def _experts(self, user, query, after, count, batch_size, permissions):
        """按页筛选可调用的专家，并返回前端可提交的精确版本。"""
        result = []
        scanned = after or ""
        while len(result) < count:
            # Authorization can reject many candidates; keep each DB read bounded.
            rows = await self.storage.definitions.list_enabled_experts_after(
                scanned, limit=batch_size
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
                    value = await self.agent_query.read_for_use(
                        user, row["id"], view, permissions=permissions
                    )
                    verifier = (
                        self.resources.permissions
                        if permissions is None
                        else permissions
                    )
                    await verifier.require_execution(
                        user, {**value, "resource_view_type": view}
                    )
                except (Conflict, Forbidden, NotFound):
                    continue
                if value.get("agent_type") not in EXPERT_TYPES or not value.get(
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
                                "is_editable": user.role_type == "admin"
                                or (own and value["agent_type"] != "remote"),
                            },
                        }
                    )
                    if len(result) == count:
                        break
        return result

    async def _skills(self, user, query, after, count, batch_size):
        result = []
        scanned = after.rsplit("@", 1)[0] if after else ""
        while len(result) < count:
            # Page resources so an older installed version stays in its resource's page.
            identifiers, rows = await self.storage.assets.page_versions(
                "skill", scanned, limit=batch_size
            )
            if not identifiers:
                break
            selected = await self.resources.installations.assets(
                user, "skill", rows, enabled_only=True
            )
            for identifier in identifiers:
                scanned = identifier
                asset = selected.get(identifier)
                if asset is None or not asset["is_enabled"]:
                    continue
                installed = await self.resources.installations.selection(user, asset)
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

        def metadata(root, entries, ignored_root=()):
            # Reuse the no-follow directory adapter. Content scan byte ceilings
            # do not apply to metadata pages. Directory suffixes preserve the
            # same lexical order as full file paths across nested folders.
            for item in sorted(
                entries,
                key=lambda item: item["path"] + ("/" if item["is_directory"] else ""),
            ):
                if item["is_directory"]:
                    children = files._list_root(
                        root, item["path"], ignored_root=ignored_root
                    )
                    yield from metadata(root, children, ignored_root)
                else:
                    yield item

        def groups():
            artifacts = files.area_root(user_id, project_id, "artifacts")
            entries = files._list_root(artifacts, "")
            # Surface top-level deliveries and project inputs before nested artifacts.
            yield 0, "artifacts", (item for item in entries if not item["is_directory"])
            uploads = files.area_root(user_id, project_id, "uploads")
            yield (
                1,
                "uploads",
                metadata(
                    uploads,
                    files._list_root(uploads, "", ignored_root=files.RESERVED),
                    files.RESERVED,
                ),
            )
            yield (
                2,
                "artifacts",
                metadata(artifacts, (item for item in entries if item["is_directory"])),
            )

        after_key = None
        if after:
            _, area, path = after.split(":", 2)
            after_key = (1 if area == "uploads" else 2 if "/" in path else 0, after)

        def candidates():
            for rank, area, entries in groups():
                if after_key and rank < after_key[0]:
                    continue
                for item in entries:
                    reference = (
                        item["path"]
                        if area == "uploads"
                        else "artifacts/" + item["path"]
                    )
                    identifier = f"file:{area}:{item['path']}"
                    if after_key and (rank, identifier) <= after_key:
                        continue
                    name = item["name"]
                    display_name = (
                        self._artifact_display_name(name)
                        if area == "artifacts"
                        else name
                    )
                    if self._matches(query, display_name, reference):
                        yield {
                            "id": identifier,
                            "name": name,
                            "display_name": display_name,
                            "detail": reference,
                            "kind": "file",
                            "reference": reference,
                        }

        # Stop metadata enumeration as soon as a page and next-page marker exist.
        return list(islice(candidates(), count))

    @staticmethod
    def _artifact_display_name(name: str) -> str:
        path = PurePosixPath(name)
        stem = re.sub(r"-(?:[0-9a-f]{8}|[0-9a-f]{12})$", "", path.stem, flags=re.I)
        stem = re.sub(r"_\d{8}_\d{6}$", "", stem)
        return (stem.replace("_", " ").strip() or path.stem) + path.suffix
