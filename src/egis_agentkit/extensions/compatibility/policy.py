"""Wire enterprise policies and native collaboration into middleware hooks."""

from agentscope.middleware import MiddlewareBase

from egis_agentkit.extensions.middleware import (
    SkillBindingMiddleware,
    TeamPromptMiddleware,
    ToolAccessMiddleware,
    UserPreferenceMiddleware,
    WorkspacePromptMiddleware,
)
from egis_agentkit.extensions.skill.loader import LiveSkillLoader
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.team.collaboration import (
    collaborators,
    team_say_allowed,
    validate_collaborators,
)

from .collaboration import apply_invitations


def tool_access_callbacks(user, scope, permissions):
    async def validate_access():
        await permissions.validate_snapshot(user, scope.snapshot)
        await validate_collaborators(permissions, user, scope.binding)

    async def prepare_collaboration(agent, inputs):
        if scope.binding.get("collaboration"):
            native_create = await agent.toolkit.get_tool("TeamCreate")
            await apply_invitations(
                user, scope, agent.state, native_create, inputs, permissions
            )

    def may_send_team_message():
        return team_say_allowed(scope.session_record, scope.binding)

    return validate_access, may_send_team_message, prepare_collaboration


def tool_access_middleware(user, scope, permissions, skills=None):
    validate_access, may_send_team_message, prepare_collaboration = (
        tool_access_callbacks(user, scope, permissions)
    )
    return ToolAccessMiddleware(
        scope=scope,
        validate_access=validate_access,
        may_send_team_message=may_send_team_message,
        prepare_collaboration=prepare_collaboration,
        skills=skills,
    )


def team_prompt_arguments(scope):
    def invited_members():
        members = collaborators(scope.binding)
        if not members:
            return None
        return [
            member["snapshot"]["root"]["agent_name"]
            + "@"
            + member["native_agent_id"][:8]
            for member in members
            if member["status"] == "joined"
        ]

    fixed_members = (
        [
            node["data"]["agent_name"]
            for node in scope.snapshot["nodes"].values()
            if node["resource_type"] == "agent"
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


class EnterprisePolicyMiddleware(MiddlewareBase):
    """Legacy composition of the same middleware installed by the factory."""

    def __init__(
        self, storage, user, scope, *, resources=None, policies=None, preference_text=""
    ):
        self.storage, self.user, self.scope = storage, user, scope
        self.policies, self.preference_text = policies, preference_text
        self.skills = (
            LiveSkillLoader(user, scope, resources) if resources is not None else None
        )
        self.skill_binding = SkillBindingMiddleware(
            self.skills,
            bind_state=self.skills.bind_state if self.skills is not None else None,
        )
        self.tool_access = tool_access_middleware(
            user, scope, PermissionService(storage, policies), self.skills
        )
        self.workspace_prompt = WorkspacePromptMiddleware(scope)
        self.team_prompt = team_prompt_middleware(scope)
        self.user_preference = UserPreferenceMiddleware(preference_text)

    @property
    def uploads_dir(self):
        return self.workspace_prompt.uploads_dir

    async def on_reply(self, agent, input_kwargs, next_handler):
        async def bind_skills(**kwargs):
            async for event in self.skill_binding.on_reply(agent, kwargs, next_handler):
                yield event

        async for event in self.tool_access.on_reply(agent, input_kwargs, bind_skills):
            yield event

    async def on_system_prompt(self, agent, current_prompt):
        prompt = await self.workspace_prompt.on_system_prompt(agent, current_prompt)
        prompt = await self.team_prompt.on_system_prompt(agent, prompt)
        return await self.user_preference.on_system_prompt(agent, prompt)

    async def on_check_permission(self, agent, input_kwargs, next_handler):
        return await self.tool_access.on_check_permission(
            agent, input_kwargs, next_handler
        )
