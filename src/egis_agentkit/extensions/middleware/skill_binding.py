from agentscope.middleware import MiddlewareBase

from egis_agentkit.extensions.skill.loader import LiveSkillLoader


class SkillBindingMiddleware(MiddlewareBase):
    """Bind the same loader used by access checks to the native Skill viewer."""

    def __init__(self, skills: LiveSkillLoader | None):
        self.skills = skills

    async def on_reply(self, agent, input_kwargs, next_handler):
        if self.skills is not None:
            await self.skills.bind_state(agent.state)
            agent.toolkit.tool_groups[0].skills_or_loaders = [self.skills]
        async for event in next_handler(**input_kwargs):
            yield event
