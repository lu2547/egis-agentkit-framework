from dataclasses import dataclass, field

from sqlalchemy import and_, or_, select, update
from sqlalchemy.dialects.postgresql import insert

from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.models import Grant, User, utcnow
from egis_agentkit.service.agent.views import DefinitionViews
from egis_agentkit.service.user.service import require_admin
from egis_agentkit.storage import tables as t


@dataclass
class PermissionDecision:
    is_allowed: bool
    paths: list[list[dict]] = field(default_factory=list)


class PermissionService:
    def __init__(self, storage, policies=None):
        self.storage, self.policies = storage, policies

    async def explain(
        self, user: User, resource_type: str, resource_id: str, view: str
    ) -> PermissionDecision:
        # Long-running turns must observe membership/disable changes at the
        # next protected action instead of trusting their initial User object.
        from egis_agentkit.service.user.service import UserService

        try:
            user = await UserService(self.storage).get(user.user_id)
        except (Forbidden, NotFound):
            return PermissionDecision(False)
        if resource_type in ("agent", "team"):
            try:
                target = await self.storage.get_definition(resource_id)
            except NotFound:
                return PermissionDecision(False)
        else:
            async with self.storage.engine.connect() as conn:
                target = (
                    (
                        await conn.execute(
                            select(t.assets).where(
                                t.assets.c.resource_id == resource_id,
                                t.assets.c.resource_type == resource_type,
                                t.assets.c.is_enabled,
                            )
                        )
                    )
                    .mappings()
                    .first()
                )
            if target is None:
                return PermissionDecision(False)
        if not target["is_enabled"]:
            return PermissionDecision(False)
        node = {
            "resource_type": resource_type,
            "resource_id": resource_id,
            "resource_view_type": view,
        }
        if view == "private":
            own = target["owner_type"] == "user" and target["owner_id"] == user.user_id
            is_personal = target.get("agent_type") == "personal"
            enabled = getattr(
                self.policies,
                "allow_personal_self_use" if is_personal else "allow_private_self_use",
                True,
            )
            return PermissionDecision(
                bool(own and enabled),
                [[{"source": "private-self"}, node]] if own and enabled else [],
            )
        if (
            view == "system"
            and target["owner_type"] != "system"
            or view == "shared"
            and target["owner_type"] != "user"
        ):
            return PermissionDecision(False)
        async with self.storage.engine.connect() as conn:
            valid_groups = list(
                (
                    await conn.execute(
                        select(t.groups.c.group_id).where(
                            t.groups.c.group_id.in_(user.group_ids),
                            t.groups.c.is_enabled,
                        )
                    )
                ).scalars()
            )
            grants = [
                dict(r)
                for r in (
                    await conn.execute(
                        select(t.grants).where(
                            t.grants.c.revoked_at.is_(None),
                            or_(
                                and_(
                                    t.grants.c.principal_type == "user",
                                    t.grants.c.principal_id == user.user_id,
                                ),
                                and_(
                                    t.grants.c.principal_type == "group",
                                    t.grants.c.principal_id.in_(valid_groups),
                                ),
                                t.grants.c.principal_type == "all_users",
                            ),
                        )
                    )
                ).mappings()
            ]
        paths = []
        for grant in grants:
            if (
                grant["resource_type"] == resource_type
                and grant["resource_id"] == resource_id
                and grant["resource_view_type"] == view
            ):
                if await self._available(resource_type, resource_id, view):
                    paths.append([{"grant_id": grant["id"]}, node])
            elif grant["resource_type"] in ("agent", "team"):
                path = await self._find_path(
                    grant["resource_type"],
                    grant["resource_id"],
                    grant["resource_view_type"],
                    node,
                )
                if path:
                    paths.append([{"grant_id": grant["id"]}, *path])
        return PermissionDecision(bool(paths), paths)

    async def _available(self, resource_type, resource_id, view):
        if resource_type in ("agent", "team"):
            try:
                await DefinitionViews(self.storage).raw_view(resource_id, view)
            except (Forbidden, NotFound):
                return False
        elif view == "shared":
            async with self.storage.engine.connect() as conn:
                return (
                    await conn.execute(
                        select(t.reviews.c.review_id).where(
                            t.reviews.c.resource_id == resource_id,
                            t.reviews.c.resource_type == resource_type,
                            t.reviews.c.is_current,
                        )
                    )
                ).first() is not None
        return True

    async def _find_path(self, root_type, root_id, root_view, target):
        from egis_agentkit.service.access.snapshot import SnapshotService

        try:
            snapshot = await SnapshotService(self.storage).capture(root_id, root_view)
        except (Forbidden, NotFound):
            return None
        return SnapshotService.path(snapshot, target)

    async def require(self, user, resource_type, resource_id, view):
        decision = await self.explain(user, resource_type, resource_id, view)
        if not decision.is_allowed:
            raise Forbidden("未获得此资源的使用权限")
        return decision

    async def validate_snapshot(self, user, snapshot):
        """Check current authority without changing pinned configuration.

        Self-use of a private definition is not a new grant to its external
        dependencies. Crossing from private configuration into a system/shared
        node therefore needs an independently valid USE path. Once a granted
        public node is entered, its pinned descendants inherit that authority.
        """
        nodes = snapshot.get("nodes", {})
        root = nodes.get(snapshot.get("root_key"))
        if root is None or not all(
            k in root for k in ("resource_type", "resource_id", "resource_view_type")
        ):
            raise Forbidden("执行快照缺少权限根")
        await self.require(
            user, root["resource_type"], root["resource_id"], root["resource_view_type"]
        )
        pending = [(snapshot["root_key"], root["resource_view_type"] != "private")]
        seen = set()
        while pending:
            key, inherited = pending.pop()
            if (key, inherited) in seen:
                continue
            seen.add((key, inherited))
            node = nodes.get(key)
            if node is None:
                raise Forbidden("执行快照引用不完整")
            kind, identifier, view = (
                node["resource_type"],
                node["resource_id"],
                node["resource_view_type"],
            )
            if not inherited:
                await self.require(user, kind, identifier, view)
                inherited = view != "private"
            if kind in ("agent", "team"):
                current = await self.storage.get_definition(identifier)
                if not current["is_enabled"]:
                    raise Forbidden("绑定智能体已停用")
            else:
                async with self.storage.engine.connect() as conn:
                    enabled = await conn.scalar(
                        select(t.assets.c.is_enabled).where(
                            t.assets.c.resource_type == kind,
                            t.assets.c.resource_id == identifier,
                            t.assets.c.resource_version
                            == node["data"]["resource_version"],
                        )
                    )
                if not enabled:
                    raise Forbidden("绑定资源已停用")
            pending.extend((child, inherited) for child in node.get("edges", []))

    async def list_grants(self, admin):
        require_admin(admin)
        async with self.storage.engine.connect() as conn:
            return [
                dict(r)
                for r in (
                    await conn.execute(
                        select(t.grants).order_by(t.grants.c.granted_at.desc())
                    )
                ).mappings()
            ]

    async def grant(self, admin: User, data: Grant, *, conn=None):
        require_admin(admin)
        if conn is None:
            async with self.storage.transaction() as connection:
                return await self.grant(admin, data, conn=connection)
        if data.principal_type == "user":
            exists = await conn.scalar(
                select(t.users.c.user_id).where(
                    t.users.c.user_id == data.principal_id, t.users.c.is_enabled
                )
            )
            if exists is None:
                raise NotFound("授权用户不存在或已停用")
        if data.principal_type == "group":
            exists = await conn.scalar(
                select(t.groups.c.group_id).where(
                    t.groups.c.group_id == data.principal_id, t.groups.c.is_enabled
                )
            )
            if exists is None:
                raise NotFound("授权分组不存在或已停用")
        if data.resource_type in ("agent", "team"):
            resource = await self.storage.get_definition(data.resource_id, conn=conn)
        else:
            resource = (
                (
                    await conn.execute(
                        select(t.assets).where(
                            t.assets.c.resource_type == data.resource_type,
                            t.assets.c.resource_id == data.resource_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if resource is None:
                raise NotFound()
        if (resource["owner_type"], resource["owner_id"]) != (
            data.resource_owner_type,
            data.resource_owner_id,
        ):
            raise Forbidden("授权目标归属不匹配")
        if data.resource_view_type == "system" and resource["owner_type"] != "system":
            raise Forbidden("不能通过系统授权开放用户私有配置")
        values = data.model_dump(exclude={"revoked_at"})
        values.update(granted_by=admin.user_id, granted_at=utcnow())
        columns = [
            "principal_type",
            "principal_id",
            "resource_type",
            "resource_id",
            "resource_view_type",
            "resource_owner_type",
            "resource_owner_id",
            "grant_source_type",
        ]
        statement = insert(t.grants).values(**values)
        result = (
            (
                await conn.execute(
                    statement.on_conflict_do_update(
                        index_elements=[t.grants.c[k] for k in columns],
                        index_where=t.grants.c.revoked_at.is_(None),
                        set_={
                            "source_review_id": data.source_review_id,
                            "granted_by": admin.user_id,
                            "granted_at": utcnow(),
                        },
                    ).returning(t.grants)
                )
            )
            .mappings()
            .one()
        )
        return dict(result)

    async def revoke(self, admin, grant_id):
        require_admin(admin)
        async with self.storage.transaction() as conn:
            result = await conn.execute(
                update(t.grants)
                .where(t.grants.c.id == grant_id)
                .values(revoked_at=utcnow(), revoked_by=admin.user_id)
            )
            if not result.rowcount:
                raise NotFound()
