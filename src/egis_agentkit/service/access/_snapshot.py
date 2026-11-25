"""An authorization read boundary, discarded after one service operation."""

from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service._user import UserService
from egis_agentkit.storage._authorization import resource_rows
from egis_agentkit.storage._definition import (
    build_definition_data,
    definition_view_selection,
)
from egis_agentkit.storage.model._base import publication_label, resource_available
from egis_agentkit.storage.model._resource import (
    MCP_STORAGE_VERSION,
    asset_version_order,
    is_managed_connector,
)

from ._decision import INTERNAL_AGENT_TYPES, DefinitionAccess, PermissionDecision
from ._publication import same_publication_content


def reference_parts(ref):
    return (
        ref.get("resource_type", ref.get("root_resource_type", "agent")),
        ref.get("resource_id", ref.get("definition_id")),
        ref.get("resource_version"),
    )


def row_parts(row):
    return (
        row.get("definition_type", row.get("resource_type")),
        row.get("id", row.get("resource_id")),
        str(row["resource_version"]),
    )


class PermissionReadSnapshot:
    """Reuse only current-operation identity, versions and scoped grant rows."""

    def __init__(self, permissions, user):
        self.permissions = permissions
        self.storage = permissions.storage
        self.user = user
        self.rows = {}
        self.grants = []
        self.by_grant = {}
        self.reviews = {}
        self.pending = None
        self._tool_current_loaded = False

    @classmethod
    async def create(
        cls,
        permissions,
        user,
        references,
        *,
        loaded=(),
        refresh_user=True,
        include_dependencies=True,
    ):
        if refresh_user:
            try:
                user = await UserService(permissions.storage).get(user.user_id)
            except (Forbidden, NotFound):
                user = user.model_copy(update={"is_enabled": False})
        snapshot = cls(permissions, user)
        for row in loaded:
            snapshot.rows[row_parts(row)] = row
        refs = list(references)
        root_ids = {ref["root_grant_id"] for ref in refs if ref.get("root_grant_id")}
        for grant in await snapshot.storage.access.active_root_grants(root_ids):
            refs.append(
                {
                    "resource_type": grant["resource_type"],
                    "resource_id": grant["resource_id"],
                    "resource_view_type": grant["resource_view_type"],
                    "grant_id": grant["id"],
                }
            )
        # A team has one member layer; each member has a binding layer. Expand
        # only these fixed references, never scan other definitions or grants.
        seen = set()
        for _ in range(3):
            needed = set()
            for ref in refs:
                kind, identifier, version = reference_parts(ref)
                if identifier is None:
                    continue
                version = MCP_STORAGE_VERSION if kind == "mcp" else version
                key = (kind, identifier, str(version) if version is not None else None)
                if key in seen:
                    continue
                seen.add(key)
                matching = [
                    row
                    for (k, ident, _), row in snapshot.rows.items()
                    if (k, ident) == (kind, identifier)
                ]
                selected = version
                if matching and kind in ("agent", "team", "skill"):
                    source = matching[0]
                    pointer_field = (
                        "version"
                        if kind in ("agent", "team")
                        else "publication_revision"
                    )
                    available = {row.get(pointer_field) for row in matching}
                    pointers = {
                        source.get("owner_version"),
                        source.get("shared_version"),
                    } - {None}
                    if "owner_version" not in source or not pointers.issubset(
                        available
                    ):
                        needed.add(key)
                if selected is None and matching:
                    source = matching[0]
                    if kind in ("agent", "team"):
                        selected = (
                            source.get("shared_version")
                            if ref.get("resource_view_type") == "shared"
                            else source.get("owner_version")
                        )
                    elif kind == "skill":
                        revision = (
                            source.get("shared_version")
                            if ref.get("resource_view_type") == "shared"
                            else source.get("owner_version")
                        )
                        selected = next(
                            (
                                row["resource_version"]
                                for row in matching
                                if row["publication_revision"] == revision
                            ),
                            None,
                        )
                    elif kind == "mcp":
                        selected = MCP_STORAGE_VERSION
                if (
                    selected is None
                    or (kind, identifier, str(selected)) not in snapshot.rows
                ):
                    needed.add(key)
            if needed:
                for row in await resource_rows(snapshot.storage.engine, needed):
                    snapshot.rows[row_parts(row)] = row
            if not include_dependencies:
                break
            expanded = []
            for row in list(snapshot.rows.values()):
                if row.get("definition_type") == "team":
                    expanded.extend(row["payload"]["members"])
                    expanded.append(row["payload"]["orchestrator"])
                elif row.get("definition_type") == "agent":
                    expanded.extend(row["payload"].get("bindings", []))
            if all(reference_parts(ref) in seen for ref in expanded):
                break
            refs.extend(expanded)
        resources = {(kind, identifier) for kind, identifier, _ in seen}
        snapshot.grants = await snapshot.storage.access.authorization_grants(
            user.user_id,
            user.group_ids,
            resources,
            grant_ids={
                ref.get("grant_id", ref.get("access_grant_id"))
                for ref in refs
                if ref.get("grant_id", ref.get("access_grant_id"))
            },
        )
        snapshot.by_grant = {
            row["id"]: row for row in snapshot.grants if row["root_grant_id"] is None
        }
        snapshot.reviews = await snapshot.storage.access.approved_reviews(
            {
                ref.get("review_id", ref.get("shared_review_id"))
                for ref in refs
                if ref.get("review_id", ref.get("shared_review_id"))
                and not ref.get("grant_id", ref.get("access_grant_id"))
            }
        )
        return snapshot

    def _identity_rows(self, kind, identifier):
        return [
            row
            for (k, ident, _), row in self.rows.items()
            if (k, ident) == (kind, identifier)
        ]

    def _resource(self, kind, identifier, version=None):
        if version is not None:
            return self.rows.get((kind, identifier, str(version)))
        rows = self._identity_rows(kind, identifier)
        if not rows:
            return None
        if kind in ("agent", "team"):
            return self.rows.get((kind, identifier, str(rows[0]["owner_version"])))
        if kind == "skill":
            return next(
                (
                    row
                    for row in rows
                    if row["publication_revision"] == row["owner_version"]
                ),
                None,
            )
        return max(rows, key=asset_version_order)

    def _shared_version(self, kind, identifier):
        rows = self._identity_rows(kind, identifier)
        if not rows:
            return None
        if kind in ("agent", "team"):
            shared = rows[0]["shared_version"]
            return str(shared) if shared is not None else None
        if kind == "skill":
            return next(
                (
                    row["resource_version"]
                    for row in rows
                    if row["publication_revision"] == row["shared_version"]
                ),
                None,
            )
        return next(
            (row["resource_version"] for row in rows if row.get("is_shared_current")),
            None,
        )

    async def definition_access(self, source, view=None, *, version=None):
        user = self.user
        own = source["owner_type"] == "user" and source["owner_id"] == user.user_id
        editable = own or user.role_type == "admin"
        catalog = view is None
        if catalog:
            view = (
                "system"
                if source["owner_type"] == "system"
                else "private"
                if own
                else "shared"
            )
        if not user.is_enabled or (view == "private" and not editable):
            raise Forbidden()
        decision = await self.explain(
            source["definition_type"], source["id"], view, version=version
        )
        if view == "shared" and not decision.is_allowed:
            if not catalog or not editable:
                raise Forbidden()
            view = "private"
        if view == "system" and not (editable or decision.is_allowed):
            raise Forbidden()
        current = version is None or str(source["owner_version"]) == str(version)
        return DefinitionAccess(
            view, editable and current and view != "shared", decision
        )

    async def definition_versions(self, source):
        user = self.user
        editable = user.role_type == "admin" or (
            source["owner_type"] == "user" and source["owner_id"] == user.user_id
        )
        if editable and user.is_enabled:
            view = "system" if source["owner_type"] == "system" else "private"
            versions = await self.storage.definitions.list_versions(
                source["id"], definition_type=source["definition_type"]
            )
            return [
                {"resource_version": str(version), "resource_view_type": view}
                for version in versions
            ]
        view = "system" if source["owner_type"] == "system" else "shared"
        decision = await self.explain(source["definition_type"], source["id"], view)
        return (
            [
                {
                    "resource_version": decision.resource_version,
                    "resource_view_type": view,
                }
            ]
            if decision.is_allowed
            else []
        )

    async def definition_view(self, reference, *, dependency=False):
        kind, identifier, version = reference_parts(reference)
        rows = self._identity_rows(kind, identifier)
        if not rows:
            raise NotFound()
        view = reference["resource_view_type"]
        grant_id = reference.get("grant_id", reference.get("access_grant_id"))
        review_id = reference.get("review_id", reference.get("shared_review_id"))
        if not dependency and grant_id is None and review_id is None:
            decision = await self.explain(
                kind,
                identifier,
                view,
                version=version,
                allow_pinned=reference.get("allow_pinned", True),
            )
            if decision.is_allowed:
                grant_id, review_id = decision.grant_id, decision.review_id
        selected, metadata = definition_view_selection(
            rows[0],
            view,
            version=version,
            grant_id=grant_id,
            review_id=review_id,
            grant=self.by_grant.get(grant_id),
            review=self.reviews.get(review_id),
            allow_pinned=reference.get("allow_pinned", True),
            dependency=dependency,
        )
        target = self._resource(kind, identifier, selected)
        if target is None:
            raise NotFound()
        return {**build_definition_data(target), "resource_view_type": view, **metadata}

    async def explain(
        self,
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
    ):
        permissions, user = self.permissions, self.user
        if not user.is_enabled:
            return PermissionDecision(False)
        if resource_type == "mcp":
            group_id = (
                UserService(self.storage, permissions.policies).company_group().group_id
            )
            asset = self._resource("mcp", resource_id, MCP_STORAGE_VERSION)
            allowed = (
                view == "system"
                and group_id in user.group_ids
                and asset is not None
                and asset["is_enabled"]
                and is_managed_connector(asset)
                and any(
                    row["resource_type"] == "mcp"
                    and row["resource_id"] == resource_id
                    and row["is_for_user"]
                    and row["grant_source_type"] == "manual"
                    and row["principal_type"] == "group"
                    and row["principal_id"] == group_id
                    and row["resource_view_type"] == "system"
                    and row["resource_owner_type"] == "system"
                    and row["resource_owner_id"] == "system"
                    and row["root_grant_id"] is None
                    for row in self.grants
                )
            )
            return PermissionDecision(bool(allowed))
        if root_grant_id is None and view == "shared":
            current = self._shared_version(resource_type, resource_id)
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
            if resource_type == "tool" and not self._tool_current_loaded:
                references = {
                    (kind, identifier, None)
                    for kind, identifier, _ in self.rows
                    if kind == "tool"
                }
                for row in await resource_rows(self.storage.engine, references):
                    self.rows[row_parts(row)] = row
                self._tool_current_loaded = True
            current = self._resource(resource_type, resource_id)
            if current is None or str(current["resource_version"]) != str(version):
                return PermissionDecision(False)
        resolve_dependency = root_grant_id is not None and version is None
        target = (
            None
            if resolve_dependency
            else self._resource(resource_type, resource_id, version)
        )
        if not resolve_dependency and not permissions._valid_view(target, view):
            return PermissionDecision(False)
        node = {
            "resource_type": resource_type,
            "resource_id": resource_id,
            "resource_view_type": view,
        }
        grants = [row for row in self.grants if row["is_for_user"]]
        if root_grant_id is None:
            return permissions._direct_decision(
                user, target, node, grants, review_id=review_id, grant_id=grant_id
            )
        intrinsic = (
            permissions._intrinsic_source(user, target, view) if target else None
        )
        if intrinsic and intrinsic != "private-self":
            return PermissionDecision(
                True,
                [[{"source": intrinsic}, node]],
                resource_version=target["resource_version"],
            )
        matches = [
            row
            for row in grants
            if permissions._matches_grant(
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
        if _parent_grant_id is None:
            root_grant = self.by_grant.get(root_grant_id)
            if root_grant is None:
                return PermissionDecision(False)
            try:
                root = await self.definition_view(
                    {
                        "resource_type": root_grant["resource_type"],
                        "resource_id": root_grant["resource_id"],
                        "resource_view_type": root_grant["resource_view_type"],
                        "grant_id": root_grant_id,
                    }
                )
                member = (await self.members(root)).get(agent_id)
            except (Forbidden, NotFound):
                return PermissionDecision(False)
            versions = set()
            if member is not None:
                if resource_type == "agent":
                    if (
                        member["id"] == resource_id
                        and member["resource_view_type"] == view
                    ):
                        versions.add(member["resource_version"])
                else:
                    versions = {
                        ref["resource_version"]
                        for ref in member["payload"].get("bindings", [])
                        if all(ref[key] == value for key, value in node.items())
                    }
            if version is None and len(versions) == 1:
                version = next(iter(versions))
                target = self._resource(resource_type, resource_id, version)
            if str(version) not in versions or not permissions._valid_view(
                target, view
            ):
                return PermissionDecision(False)
        if not permissions._valid_view(target, view):
            return PermissionDecision(False)
        selected = matches[0]
        paths = [
            [
                {
                    key: row[key]
                    for key in (
                        "id",
                        "root_grant_id",
                        "parent_grant_id",
                        "agent_id",
                        "team_id",
                    )
                },
                {**node, "resource_version": target["resource_version"]},
            ]
            for row in matches
        ]
        for path in paths:
            path[0]["grant_id"] = path[0].pop("id")
        return PermissionDecision(
            True,
            paths,
            review_id=selected["source_review_id"],
            root_grant_id=root_grant_id,
            resource_version=target["resource_version"],
        )

    async def require(self, resource_type, resource_id, view, **context):
        decision = await self.explain(resource_type, resource_id, view, **context)
        if not decision.is_allowed:
            raise Forbidden("未获得此资源的使用权限")
        return decision

    async def members(self, root):
        if root["definition_type"] == "agent":
            return {root["id"]: root}
        return {
            ref["definition_id"]: await self.definition_view(ref, dependency=True)
            for ref in (root["payload"]["orchestrator"], *root["payload"]["members"])
        }

    async def require_execution(self, root, *, allow_pinned=False):
        grant = await self.require(
            root["definition_type"],
            root["id"],
            root["resource_view_type"],
            version=root["resource_version"],
            grant_id=root.get("access_grant_id"),
            review_id=root.get("shared_review_id"),
            allow_pinned=allow_pinned,
        )
        team_id = root["id"] if root["definition_type"] == "team" else None
        for member in (await self.members(root)).values():
            member_grant = grant
            if team_id:
                member_grant = await self.require(
                    "agent",
                    member["id"],
                    member["resource_view_type"],
                    version=member["resource_version"],
                    allow_pinned=allow_pinned,
                    root_grant_id=grant.root_grant_id,
                    _parent_grant_id=grant.grant_id,
                    agent_id=member["id"],
                    team_id=team_id,
                )
            parent = (
                member_grant.paths[0][0]["grant_id"]
                if member_grant.root_grant_id
                else None
            )
            for binding in member["payload"].get("bindings", []):
                if binding["resource_type"] == "skill":
                    await self.require(
                        "skill",
                        binding["resource_id"],
                        binding["resource_view_type"],
                        version=binding["resource_version"],
                        allow_pinned=allow_pinned,
                        root_grant_id=member_grant.root_grant_id,
                        _parent_grant_id=parent,
                        agent_id=member["id"],
                        team_id=team_id if grant.root_grant_id else None,
                    )

    async def share_availability(self, kind, identifier, *, version, view):
        result = {
            "can_share": False,
            "can_share_update": False,
            "has_shared_version": False,
        }
        if (
            not self.user.is_enabled
            or view != "private"
            or kind not in ("agent", "team", "skill")
        ):
            return result
        target = self._resource(kind, identifier)
        if (
            target is None
            or target["owner_type"] != "user"
            or target["owner_id"] != self.user.user_id
            or target.get("agent_type") in INTERNAL_AGENT_TYPES
        ):
            return result
        shared = self._shared_version(kind, identifier)
        result["has_shared_version"] = shared is not None
        if resource_available(target) and target["resource_version"] == str(version):
            if shared is not None and shared != str(version) and self.pending is None:
                self.pending = await self.storage.access.pending_updates(
                    {(k, ident) for k, ident, _ in self.rows}
                )
            result["can_share"] = True
            changed = shared is not None and shared != str(version)
            previous = target
            if changed:
                previous = self._resource(kind, identifier, shared)
                if previous is None:
                    previous = await self.permissions._resource(kind, identifier, shared)
            revision = int(shared or version) if kind in ("agent", "team") else previous["publication_revision"]
            result.update(share_version=shared or str(version), share_version_label=publication_label(revision))
            changed = changed and not same_publication_content(target, previous)
            result["can_share_update"] = (
                changed
                and (kind, identifier) not in (self.pending or set())
            )
        return result
