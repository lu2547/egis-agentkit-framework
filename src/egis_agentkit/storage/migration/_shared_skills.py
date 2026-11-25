"""Explicitly publish existing approved personal skills; never alter grants or owners."""

from sqlalchemy import String, and_, case, cast, or_, select

from egis_agentkit.errors import AgentKitError, NotFound
from egis_agentkit.storage import tables as t
from egis_agentkit.storage._definition import definition_view_selection
from egis_agentkit.storage.model._base import resource_available


async def _fixed_members(storage, root):
    """Load only the member editions pinned by this approved root snapshot."""
    if root["definition_type"] == "agent":
        return [root]
    payload = root["payload"]
    references = [payload["orchestrator"], *payload["members"]]
    members = []
    for start in range(0, len(references), 100):
        page = references[start : start + 100]
        loaded = await storage.definitions.get_many(
            {("agent", ref["definition_id"], ref["resource_version"]) for ref in page}
        )
        for ref in page:
            member = loaded.get(
                ("agent", ref["definition_id"], str(ref["resource_version"]))
            )
            if member is None:
                raise NotFound("绑定专家版本不存在")
            definition_view_selection(
                member,
                ref["resource_view_type"],
                version=ref["resource_version"],
                dependency=True,
            )
            members.append(member)
    return members


async def _approved_root_versions(storage):
    """Read approved history and current shared grant pointers in bounded pages."""
    reviews, grants = t.approvals, t.resource_permission
    agent, team = t.agent_definition_summary, t.team_definition_summary
    history = select(
        reviews.c.review_id.label("cursor"),
        reviews.c.resource_type,
        reviews.c.resource_id,
        reviews.c.resource_version,
    ).where(
        reviews.c.resource_type.in_(("agent", "team")),
        reviews.c.review_status_type == "approved",
    )
    current = (
        select(
            grants.c.id.label("cursor"),
            grants.c.resource_type,
            grants.c.resource_id,
            case(
                (
                    grants.c.resource_type == "agent",
                    cast(agent.c.shared_version, String),
                ),
                else_=cast(team.c.shared_version, String),
            ).label("resource_version"),
        )
        .select_from(
            grants.outerjoin(
                agent,
                and_(
                    grants.c.resource_type == "agent",
                    grants.c.resource_id == agent.c.id,
                ),
            ).outerjoin(
                team,
                and_(
                    grants.c.resource_type == "team",
                    grants.c.resource_id == team.c.id,
                ),
            )
        )
        .where(
            grants.c.root_grant_id.is_(None),
            grants.c.revoked_at.is_(None),
            grants.c.resource_view_type == "shared",
            grants.c.resource_owner_type == "user",
            grants.c.grant_source_type != "owner",
            or_(
                and_(agent.c.shared_version.is_not(None), agent.c.deleted_at.is_(None)),
                and_(team.c.shared_version.is_not(None), team.c.deleted_at.is_(None)),
            ),
        )
    )
    seen = set()
    for source in (history, current):
        source = source.subquery("backfill_roots")
        after = ""
        while True:
            async with storage.engine.connect() as conn:
                page = list(
                    (
                        await conn.execute(
                            select(source)
                            .where(source.c.cursor > after)
                            .order_by(source.c.cursor)
                            .limit(100)
                        )
                    ).mappings()
                )
            if not page:
                break
            references = {
                (row["resource_type"], row["resource_id"], row["resource_version"])
                for row in page
            } - seen
            loaded = await storage.definitions.get_many(references)
            for reference in sorted(references):
                root = loaded.get(reference)
                if (
                    root is not None
                    and root["owner_type"] == "user"
                    and resource_available(root)
                ):
                    yield root
            seen.update(references)
            after = page[-1]["cursor"]


async def backfill_shared_skills(resources, *, apply=False, resource_ids=None):
    if resource_ids is not None:
        for identifier in resource_ids:
            resources._validate_identifier(identifier)
    changes = []
    seen = set()

    async def publish(asset):
        identity = (asset["resource_id"], asset["resource_version"])
        if identity in seen or asset["owner_type"] != "user":
            return
        if apply:
            try:
                await resources.publish_shared_skill(asset)
            except AgentKitError as exc:
                raise AgentKitError(
                    f"技能 {asset['resource_id']}@{asset['resource_version']} "
                    f"(所有者 {asset['owner_id']}) 回填失败：{exc}",
                    exc.status_code,
                ) from exc
        changes.append(
            {
                key: asset[key]
                for key in (
                    "resource_id",
                    "resource_version",
                    "owner_id",
                    "checksum_sha256",
                )
            }
        )
        seen.add(identity)

    after = ("", 0)
    while True:
        rows = await resources.storage.assets.skills.page_shared_publications(
            after=after, limit=100, resource_ids=resource_ids
        )
        if not rows:
            break
        for asset in rows:
            await publish(asset)
        after = (rows[-1]["resource_id"], rows[-1]["publication_revision"])

    async for root in _approved_root_versions(resources.storage):
        members = await _fixed_members(resources.storage, root)
        references = sorted(
            {
                (ref["resource_id"], ref["resource_version"])
                for member in members
                for ref in member["payload"].get("bindings", [])
                if ref["resource_type"] == "skill"
                and (resource_ids is None or ref["resource_id"] in resource_ids)
            }
        )
        for start in range(0, len(references), 100):
            assets = await resources.storage.assets.get_many(
                "skill", references[start : start + 100]
            )
            for asset in assets:
                await publish(asset)
    return sorted(
        changes, key=lambda row: (row["resource_id"], row["resource_version"])
    )
