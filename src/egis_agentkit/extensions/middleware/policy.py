"""Legacy composition; new runtimes install the individual middlewares."""

from agentscope.middleware import MiddlewareBase

from egis_agentkit.extensions.skill.loader import LiveSkillLoader
from egis_agentkit.service.access.authorization import PermissionService

from .skill_binding import SkillBindingMiddleware
from .team_prompt import TeamPromptMiddleware
from .tool_access import DYNAMIC_TOOLS, ToolAccessMiddleware
from .user_preference import UserPreferenceMiddleware
from .workspace_prompt import WorkspacePromptMiddleware

__all__ = ["DYNAMIC_TOOLS", "EnterprisePolicyMiddleware", "LiveSkillLoader"]


class EnterprisePolicyMiddleware(MiddlewareBase):
    def __init__(
        self, storage, user, scope, *, resources=None, policies=None, preference_text=""
    ):
        self.storage, self.user, self.scope = storage, user, scope
        self.policies, self.preference_text = policies, preference_text
        self.skills = (
            LiveSkillLoader(user, scope, resources) if resources is not None else None
        )
        self.skill_binding = SkillBindingMiddleware(self.skills)
        self.tool_access = ToolAccessMiddleware(
            user,
            scope,
            permissions=PermissionService(storage, policies),
            skills=self.skills,
        )
        self.workspace_prompt = WorkspacePromptMiddleware(scope)
        self.team_prompt = TeamPromptMiddleware(scope)
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
        return await self.tool_access.on_check_permission(agent, input_kwargs, next_handler)
