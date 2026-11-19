"""为工具权限检查提供实时授权、协作资格和专家邀请回调。

核心能力
- tool_access_callbacks 组织权限校验和原生专家邀请，不创建中间件。
- team_prompt_arguments 从已校验的固定成员及临时成员构建提示词参数。
"""

from egis_agentkit.core.agent._invite_tool import ExpertInvite
from egis_agentkit.service._user import UserService
from egis_agentkit.service.team._candidates import ExpertCandidates
from egis_agentkit.service.team._collaboration import (
    collaboration_allowed,
    collaborators,
    team_say_allowed,
)
from egis_agentkit.service.team._invitation import apply_invitations, invite_candidate
from egis_agentkit.service.team._skill_invitation import stage_skill_invitation


def tool_access_callbacks(user, scope, permissions, *, files=None):
    """生成每次工具调用使用的实时授权及专家协作回调。"""

    async def validate_access():
        await permissions.require_execution(
            user, scope.root_definition, allow_pinned=True
        )
        for definition in scope.collaborator_definitions.values():
            await permissions.require_execution(user, definition, allow_pinned=True)

    async def prepare_collaboration(agent, inputs):
        if files is not None:
            await stage_skill_invitation(
                user, scope, agent.state, inputs, permissions, files
            )
        native_create = None
        if agent.state.middle_context.get("egis_agentkit", {}).get("collaboration"):
            native_create = await agent.toolkit.get_tool("TeamCreate")
            await apply_invitations(
                user, scope, agent.state, native_create, inputs, permissions
            )
        if not collaboration_allowed(scope.session_record, scope.root_definition):
            return None
        catalog = ExpertCandidates(permissions)
        joined = {
            (member["definition_id"], member["resource_view_type"])
            for member in collaborators(scope.binding)
            if member["status"] == "joined"
        }
        candidates = [
            expert
            for expert in await catalog.resolve(user)
            if (expert["id"], expert["resource_view_type"]) not in joined
        ]
        if not candidates:
            return None
        if native_create is None:
            native_create = await agent.toolkit.get_tool("TeamCreate")
        if native_create is None:
            return None

        async def invite(reference, prompt):
            current_user = await UserService(permissions.storage).get(user.user_id)
            definition = await catalog.require(
                current_user,
                reference["definition_id"],
                reference["resource_view_type"],
                version=reference["resource_version"],
            )
            return await invite_candidate(
                current_user,
                scope,
                agent.state,
                native_create,
                definition,
                prompt,
                permissions,
            )

        return ExpertInvite(candidates, invite)

    def may_send_team_message():
        return team_say_allowed(scope.session_record, scope.binding)

    return validate_access, may_send_team_message, prepare_collaboration


def team_prompt_arguments(scope):
    """从定义记录取得固定成员和临时协作成员名称。"""

    def invited_members():
        members = collaborators(scope.binding)
        if not members:
            return None
        return [
            scope.collaborator_definitions[member["native_agent_id"]]["agent_name"]
            + "@"
            + member["native_agent_id"][:8]
            for member in members
            if member["status"] == "joined"
        ]

    fixed_members = (
        [definition["agent_name"] for definition in scope.member_definitions.values()]
        if scope.binding.get("is_team")
        else None
    )
    invited_member = bool(scope.binding.get("collaboration_parent_session_id"))
    return fixed_members, invited_member, invited_members
