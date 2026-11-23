"""专家团的成员邀请、原生消息发送资格和临时协作。"""

from contextlib import asynccontextmanager

from agentscope.app._service._projectors._subagent_hitl import SubagentHitlProjector
from agentscope.app._service._session_projection import SessionProjection
from agentscope.app.message_bus import MessageBusKeys

from egis_agentkit.client.nacos._settings import TeamInviteConfig
from egis_agentkit.errors import (
    CollaborationSessionBusy,
    Conflict,
    Forbidden,
    InvalidRequest,
    NotFound,
)
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.storage.model import new_id
from egis_agentkit.storage.model._definition import (
    EXPERT_TYPES,
    build_definition_options,
    build_definition_reference,
)


class TeamCollaborationService:
    """管理个人助理和专家团中的成员协作。"""

    @staticmethod
    @asynccontextmanager
    async def _lock(bus, session_id, agent_id):
        key = MessageBusKeys.session_lock(session_id)
        if not await bus.try_lock(key):
            raise CollaborationSessionBusy(session_id=session_id, agent_id=agent_id)
        try:
            yield
        finally:
            await bus.unlock(key)

    @staticmethod
    async def stage_collaborators(tasks, user, session_id, bus, request_id, experts):
        """更新个人助理会话的待邀请专家，已入团成员继续由原生 Team 管理。"""
        # 已撤销的待邀请成员仍可从选择中移除。
        session = await tasks.get_session_record(user, session_id, require_use=False)
        binding = session.state.middle_context["egis_agentkit"]
        async with tasks.projects.guard(user, binding["project_id"]):
            async with TeamCollaborationService._lock(
                bus, session_id, session.agent_id
            ):
                user, session, _ = await tasks._read_session(
                    user, session_id, require_use=False
                )
                binding = session.state.middle_context["egis_agentkit"]
                permissions = PermissionService(
                    tasks.storage,
                    tasks.config.snapshot.access if tasks.config else None,
                )
                references = [
                    binding,
                    *collaborators(binding),
                    *[{**item, "root_resource_type": "agent"} for item in experts],
                ]
                snapshot = await permissions.read_snapshot(
                    user, references, refresh_user=False
                )
                await snapshot.require(
                    binding["root_resource_type"],
                    binding["definition_id"],
                    binding["resource_view_type"],
                    **build_definition_options(binding),
                )
                root = await snapshot.definition_view(binding)
                if not collaboration_allowed(session, root):
                    raise Forbidden(
                        "只有个人智能体主对话可以邀请专家协作；专家不能担任组长，固定专家团不能调整成员"
                    )
                config = tasks.config.snapshot if tasks.config else None
                limits = config.team.invite if config else TeamInviteConfig()
                requested = {}
                for item in experts:
                    identity = item["definition_id"], item["resource_view_type"]
                    version = item.get("resource_version")
                    if identity in requested and requested[identity] != version:
                        raise Conflict("同一专家不能选择多个版本")
                    requested[identity] = version
                data = binding.setdefault(
                    "collaboration", {"requests": [], "members": []}
                )
                previous = next(
                    (r for r in data["requests"] if r["request_id"] == request_id), None
                )
                if previous is not None:
                    previous_versions = {
                        (item["definition_id"], item["resource_view_type"]): item[
                            "resource_version"
                        ]
                        for item in previous["experts"]
                    }
                    if requested.keys() != previous_versions.keys() or any(
                        version is not None and version != previous_versions[identity]
                        for identity, version in requested.items()
                    ):
                        raise Conflict("邀请请求编号已用于不同的专家选择")
                    return await tasks._view_loaded(session, snapshot)
                if (
                    not request_id
                    or len(request_id) > limits.max_request_id_length
                    or len(experts) > limits.max_members
                ):
                    raise InvalidRequest(
                        f"邀请编号须为 1 到 {limits.max_request_id_length} 个字符，"
                        f"且最多选择 {limits.max_members} 位专家"
                    )
                # 保留原生成员的版本。尚未入团的专家可用新版本替换。
                retained = {}
                for member in collaborators(binding):
                    identity = member["definition_id"], member["resource_view_type"]
                    matches_version = requested.get(identity) in (
                        None,
                        member["resource_version"],
                    )
                    if member["status"] == "joined" and not matches_version:
                        raise Conflict("已入团专家的版本不能变更")
                    if member["status"] != "joined" and (
                        identity not in requested or not matches_version
                    ):
                        continue
                    definition = await snapshot.definition_view(member)
                    await snapshot.require_execution(definition, allow_pinned=True)
                    retained[identity] = member
                for (identifier, view), version in sorted(requested.items()):
                    if (identifier, view) in retained:
                        continue
                    root = await snapshot.definition_view(
                        {
                            "root_resource_type": "agent",
                            "definition_id": identifier,
                            "resource_view_type": view,
                            "resource_version": version,
                        }
                    )
                    await snapshot.require_execution(root)
                    if (
                        root["definition_type"] != "agent"
                        or root["agent_type"] not in EXPERT_TYPES
                    ):
                        raise Forbidden("只能邀请预配置的专家智能体")
                    retained[identifier, view] = dict(
                        **build_definition_reference(root),
                        display_name=root["agent_display_name"],
                        avatar=root["payload"].get("avatar"),
                        native_agent_id=new_id(),
                        status="pending",
                    )
                if len(retained) > limits.max_members:
                    raise Conflict(f"当前对话最多邀请 {limits.max_members} 位专家")
                for request in data["requests"]:
                    if not request.get("completed"):
                        request.update(completed=True, superseded=True)
                data["members"] = [
                    *[
                        member
                        for member in data["members"]
                        if member["status"] == "removed"
                    ],
                    *retained.values(),
                ]
                selection = [
                    {
                        "definition_id": identifier,
                        "resource_view_type": view,
                        "resource_version": retained[identifier, view][
                            "resource_version"
                        ],
                    }
                    for identifier, view in sorted(requested)
                ]
                data["requests"].append(
                    dict(
                        request_id=request_id,
                        experts=selection,
                        completed=not selection,
                    )
                )
                await tasks.storage.sessions.save_state(session)
                return await tasks._view_loaded(session, snapshot)

    @staticmethod
    async def remove_collaborator(tasks, user, session_id, worker_id, bus):
        """结束指定专家的协作，保留历史消息和项目产物。"""
        parent = await tasks.get_session_record(user, session_id, require_use=False)
        project_id = parent.state.middle_context["egis_agentkit"]["project_id"]
        async with (
            tasks.projects.guard(user, project_id),
            TeamCollaborationService._lock(bus, session_id, parent.agent_id),
        ):
            user, parent, _ = await tasks._read_session(
                user, session_id, require_use=False
            )
            binding = parent.state.middle_context["egis_agentkit"]
            permissions = PermissionService(
                tasks.storage, tasks.config.snapshot.access if tasks.config else None
            )
            snapshot = await permissions.read_snapshot(
                user, [binding], refresh_user=False
            )
            root = await snapshot.definition_view(binding)
            if not collaboration_allowed(parent, root):
                raise Forbidden("只有个人助理对话可以调整协作专家")
            member = next(
                (
                    item
                    for item in collaborators(binding, include_removed=True)
                    if item.get("worker_session_id") == worker_id
                ),
                None,
            )
            if member is None:
                raise NotFound("协作专家不存在")
            try:
                async with TeamCollaborationService._lock(
                    bus, worker_id, member["native_agent_id"]
                ):
                    if member["status"] != "removed":
                        await TeamCollaborationService._detach(
                            tasks, user, parent, binding, member
                        )
                    await bus.queue_delete(MessageBusKeys.inbox(worker_id))
                    await TeamCollaborationService._clear_confirmations(
                        bus, session_id, worker_id
                    )
                    progress = getattr(bus, "collaborator_progress", None)
                    if progress is not None:
                        await progress.forget(session_id, worker_id)
            except CollaborationSessionBusy:
                # The leader can be stopped while its invited worker is still running.
                await bus.publish(
                    MessageBusKeys.session_interrupt_channel(),
                    {"session_id": worker_id},
                )
                raise
            await snapshot.require(
                binding["root_resource_type"],
                binding["definition_id"],
                binding["resource_view_type"],
                **build_definition_options(binding),
            )
            return await tasks._view_loaded(parent, snapshot)

    @staticmethod
    async def _detach(tasks, user, parent, binding, member):
        worker_id = member["worker_session_id"]
        team = (
            await tasks.storage.get_team(user.user_id, parent.team_id)
            if parent.team_id
            else None
        )
        if team:
            team.data.members = [
                entry for entry in team.data.members if entry.session_id != worker_id
            ]
        member["status"] = "removed"
        key = member["definition_id"] + ":" + member["resource_view_type"]
        for request in binding["collaboration"]["requests"]:
            if request["completed"]:
                continue
            keys = [
                entry["definition_id"] + ":" + entry["resource_view_type"]
                for entry in request["experts"]
            ]
            delivered = request.setdefault("delivered", [])
            if key in keys and key not in delivered:
                delivered.append(key)
                request["completed"] = all(item in delivered for item in keys)
        await tasks.storage.sessions.detach_collaborator(parent, team, worker_id)

    @staticmethod
    async def _clear_confirmations(bus, session_id, worker_id):
        projection = SessionProjection(bus)
        kind = SubagentHitlProjector.KIND
        for pending in await projection.list(session_id, kind):
            if pending["worker_session_id"] == worker_id:
                reply_id = pending["reply_id"]
                await projection.delete(
                    session_id,
                    kind,
                    SubagentHitlProjector.entry_id(worker_id, reply_id),
                )
                await projection.publish(
                    session_id,
                    SubagentHitlProjector.EVT_RESULT,
                    {"worker_session_id": worker_id, "reply_id": reply_id},
                )


def collaboration_allowed(session, root):
    """只有以个人智能体为根的主会话可以选择临时专家。"""
    return (
        root.get("definition_type") == "agent"
        and root.get("agent_type") == "personal"
        and getattr(getattr(session, "origin", None), "type", None) == "user"
    )


def collaborators(binding, *, include_removed=False):
    """读取会话当前保留的待邀请或已入团成员。"""
    return [
        member
        for member in binding.get("collaboration", {}).get("members", [])
        if include_removed or member["status"] != "removed"
    ]


def team_say_allowed(session, binding):
    """固定团队及已建立原生关系的临时协作会话可发送队内消息。"""
    return bool(
        binding.get("is_team")
        or (
            getattr(session, "team_id", None)
            and (
                binding.get("collaboration_parent_session_id")
                or any(m.get("status") == "joined" for m in collaborators(binding))
            )
        )
    )


def pending_request_id(binding):
    """获取尚未完成的邀请请求编号。"""
    return next(
        (
            r["request_id"]
            for r in binding.get("collaboration", {}).get("requests", [])
            if not r.get("completed") and r.get("source") != "agent_invite"
        ),
        None,
    )


async def validate_collaborators(permissions, user, binding, *, snapshot=None):
    """在当前边界批读会话引用的专家及执行依赖，仍校验实时授权。"""
    selected = collaborators(binding)
    if not selected:
        return {}
    snapshot = snapshot or await permissions.read_snapshot(user, selected)
    definitions = {}
    for member in selected:
        definition = await snapshot.definition_view(member)
        await snapshot.require_execution(definition, allow_pinned=True)
        definitions[member["native_agent_id"]] = definition
    return definitions
