"""专家团的成员邀请、原生消息发送资格和临时协作。"""

from agentscope.app.message_bus import MessageBusKeys

from egis_agentkit.client.nacos._settings import TeamInviteConfig
from egis_agentkit.errors import Conflict, Forbidden, InvalidRequest
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.agent._agent_manager import get_definition_for_execution
from egis_agentkit.storage.model import new_id
from egis_agentkit.storage.model._definition import (
    EXPERT_TYPES,
    build_definition_reference,
)


class TeamCollaborationService:
    """管理个人助理和专家团中的成员协作。"""

    @staticmethod
    async def stage_collaborators(tasks, user, session_id, bus, request_id, experts):
        """更新个人助理会话的待邀请专家，已入团成员继续由原生 Team 管理。"""
        # 已撤销的待邀请成员仍可从选择中移除。
        session = await tasks.get_session_record(user, session_id, require_use=False)
        binding = session.state.middle_context["egis_agentkit"]
        async with tasks.projects.guard(user, binding["project_id"]):
            key = MessageBusKeys.session_lock(session_id)
            if not await bus.try_lock(key):
                raise Conflict("对话正在运行，请等待本轮完成后邀请专家")
            try:
                session = await tasks.get_session_record(
                    user, session_id, check_collaborators=False
                )
                binding = session.state.middle_context["egis_agentkit"]
                root = await tasks.storage.definitions.get_definition_by_reference(binding)
                if not collaboration_allowed(session, root):
                    raise Forbidden(
                        "只有个人智能体主对话可以邀请专家协作；专家不能担任组长，固定专家团不能调整成员"
                    )
                config = tasks.config.snapshot if tasks.config else None
                limits = config.team.invite if config else TeamInviteConfig()
                permissions = PermissionService(
                    tasks.storage, config.access if config else None
                )
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
                    return await tasks.get_session(user, session_id)
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
                for member in data["members"]:
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
                    definition = await tasks.storage.definitions.get_definition_by_reference(member)
                    await permissions.require_execution(
                        user, definition, allow_pinned=True
                    )
                    retained[identity] = member
                for (identifier, view), version in sorted(requested.items()):
                    if (identifier, view) in retained:
                        continue
                    root = await get_definition_for_execution(
                        permissions, user, "agent", identifier, view, version=version
                    )
                    if (
                        root["definition_type"] != "agent"
                        or root["agent_type"] not in EXPERT_TYPES
                    ):
                        raise Forbidden("只能邀请预配置的专家智能体")
                    retained[identifier, view] = dict(
                        **build_definition_reference(root),
                        display_name=root["agent_display_name"],
                        native_agent_id=new_id(),
                        status="pending",
                    )
                if len(retained) > limits.max_members:
                    raise Conflict(f"当前对话最多邀请 {limits.max_members} 位专家")
                for request in data["requests"]:
                    if not request.get("completed"):
                        request.update(completed=True, superseded=True)
                data["members"] = list(retained.values())
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
                await tasks.storage.update_session_state(
                    user.user_id, session.agent_id, session.id, session.state
                )
            finally:
                await bus.unlock(key)
        return await tasks.get_session(user, session_id)


def collaboration_allowed(session, root):
    """只有以个人智能体为根的主会话可以选择临时专家。"""
    return (
        session.origin.type == "user"
        and root.get("definition_type") == "agent"
        and root.get("agent_type") == "personal"
    )


def collaborators(binding):
    """读取会话当前保留的待邀请或已入团成员。"""
    return binding.get("collaboration", {}).get("members", [])


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
            if not r.get("completed")
        ),
        None,
    )


async def validate_collaborators(permissions, user, binding):
    """读取并校验会话实际引用的临时协作专家。"""
    definitions = {}
    for member in collaborators(binding):
        definition = await permissions.storage.definitions.get_definition_by_reference(member)
        await permissions.require_execution(user, definition, allow_pinned=True)
        definitions[member["native_agent_id"]] = definition
    return definitions
