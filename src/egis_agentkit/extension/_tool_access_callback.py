"""为原生工具权限 Middleware 提供当前会话的授权和邀请回调。"""

from egis_agentkit.extension import (
    TeamPromptMiddleware,
    ToolAccessMiddleware,
)
from egis_agentkit.service.team._collaboration import collaborators, team_say_allowed

from ._team_invite_callback import apply_invitations


def tool_access_callbacks(user, scope, permissions):
    """生成每次工具调用使用的实时授权及专家协作回调。"""

    async def validate_access():
        await permissions.require_execution(user, scope.root_definition, allow_pinned=True)
        for definition in scope.collaborator_definitions.values():
            await permissions.require_execution(user, definition, allow_pinned=True)

    async def prepare_collaboration(agent, inputs):
        if scope.binding.get("collaboration"):
            native_create = await agent.toolkit.get_tool("TeamCreate")
            await apply_invitations(
                user, scope, agent.state, native_create, inputs, permissions
            )

    def may_send_team_message():
        return team_say_allowed(scope.session_record, scope.binding)

    return validate_access, may_send_team_message, prepare_collaboration


def tool_access_middleware(
    user, scope, permissions, skills=None, *, authorize_tool=None
):
    validate_access, may_send_team_message, prepare_collaboration = (
        tool_access_callbacks(user, scope, permissions)
    )
    return ToolAccessMiddleware(
        scope=scope,
        validate_access=validate_access,
        may_send_team_message=may_send_team_message,
        prepare_collaboration=prepare_collaboration,
        skills=skills,
        authorize_tool=authorize_tool,
    )


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
        [
            definition["agent_name"]
            for definition in scope.member_definitions.values()
        ]
        if scope.binding.get("is_team")
        else None
    )
    invited_member = bool(scope.binding.get("collaboration_parent_session_id"))
    return fixed_members, invited_member, invited_members


def team_prompt_middleware(scope):
    fixed_members, invited_member, invited_members = team_prompt_arguments(scope)
    return TeamPromptMiddleware(
        fixed_members=fixed_members,
        invited_member=invited_member,
        invited_members=invited_members,
    )
