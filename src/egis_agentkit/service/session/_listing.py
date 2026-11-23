"""批量构建会话列表，只复用本次读取，不复用跨请求的身份或授权状态。"""

from agentscope.app.message_bus import MessageBusKeys

from egis_agentkit.core.message_bus import EgisMessageBus
from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service._user import UserService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.storage._definition import (
    build_definition_data,
    definition_view_selection,
)


def _reference_key(reference):
    return (
        reference.get("root_resource_type", "agent"),
        reference["definition_id"],
        str(reference["resource_version"]),
    )


def _definition_view(row, reference, grants, reviews, *, dependency=False):
    """应用与定义详情读取相同的当前状态、固定版本及来源约束。"""
    if row is None:
        raise NotFound()
    grant_id, review_id = (
        reference.get("access_grant_id"),
        reference.get("shared_review_id"),
    )
    view = reference["resource_view_type"]
    _, metadata = definition_view_selection(
        row,
        view,
        version=reference["resource_version"],
        grant_id=grant_id,
        review_id=review_id,
        grant=grants.get(grant_id),
        review=reviews.get(review_id),
        allow_pinned=True,
        dependency=dependency,
    )
    return {**build_definition_data(row), "resource_view_type": view, **metadata}


async def list_sessions(service, user, bus, *, limit):
    try:
        user = await UserService(service.storage).get(user.user_id)
    except (Forbidden, NotFound):
        return []
    records = await service.storage.sessions.list_main_records(
        user.user_id, limit=limit
    )
    if not records:
        return []
    bindings = [
        record.state.middle_context.get("egis_agentkit", {}) for record in records
    ]
    bindings = [binding for binding in bindings if binding.get("is_initialized")]
    references = {_reference_key(binding) for binding in bindings}
    definitions = await service.storage.definitions.get_many(references)
    projects = await service.storage.projects.get_many(
        user.user_id, {b["project_id"] for b in bindings}
    )
    grants = await service.storage.access.definition_listing_grants(
        user.user_id,
        user.group_ids,
        {(kind, identifier) for kind, identifier, _ in references},
        {b["access_grant_id"] for b in bindings if b.get("access_grant_id")},
    )
    by_id = {grant["id"]: grant for grant in grants}
    available = [grant for grant in grants if grant["is_for_user"]]
    reviews = await service.storage.access.approved_reviews(
        {
            b["shared_review_id"]
            for b in bindings
            if b.get("shared_review_id") and not b.get("access_grant_id")
        }
    )
    member_refs = set()
    for row in definitions.values():
        if row["definition_type"] == "team":
            member_refs.update(
                _reference_key(ref)
                for ref in (row["payload"]["orchestrator"], *row["payload"]["members"])
            )
    member_definitions = (
        await service.storage.definitions.get_many(member_refs) if member_refs else {}
    )
    permissions = PermissionService(
        service.storage, service.config.snapshot.access if service.config else None
    )
    result = []
    for session in records:
        binding = session.state.middle_context.get("egis_agentkit", {})
        if not binding.get("is_initialized"):
            continue
        try:
            service.projects.project.require_available(
                projects.get(binding["project_id"])
            )
            row = definitions.get(_reference_key(binding))
            decision = permissions.explain_loaded_definition(
                user,
                row,
                binding["resource_view_type"],
                available,
                review_id=binding.get("shared_review_id"),
                grant_id=binding.get("access_grant_id"),
            )
            if not decision.is_allowed:
                raise Forbidden()
            root = _definition_view(row, binding, by_id, reviews)
            if root["definition_type"] == "agent":
                agents = {root["id"]: root}
            else:
                agents = {
                    ref["definition_id"]: _definition_view(
                        member_definitions.get(_reference_key(ref)),
                        ref,
                        {},
                        {},
                        dependency=True,
                    )
                    for ref in (
                        root["payload"]["orchestrator"],
                        *root["payload"]["members"],
                    )
                }
            item = service._session_view(session, root, agents)
            result.append(item)
        except (Forbidden, NotFound):
            continue
    if bus is not None:
        keys = [MessageBusKeys.session_lock(item["session_id"]) for item in result]
        locked = (
            await bus.locked_keys(keys)
            if isinstance(bus, EgisMessageBus)
            else {key for key in keys if await bus.is_locked(key)}
        )
        for item, key in zip(result, keys):
            item["is_running"] = key in locked
    return result
