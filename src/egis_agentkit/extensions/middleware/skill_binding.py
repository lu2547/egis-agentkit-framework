from collections.abc import Awaitable, Callable

from agentscope.middleware import MiddlewareBase
from agentscope.skill import SkillLoaderBase
from agentscope.state import AgentState


class SkillBindingMiddleware(MiddlewareBase):
    """Bind the same loader used by access checks to the native Skill viewer."""

    def __init__(
        self,
        skills: SkillLoaderBase | None,
        *,
        bind_state: Callable[[AgentState], Awaitable[None]] | None = None,
    ):
        self.skills = skills
        self.bind_state = (
            bind_state
            if bind_state is not None
            else getattr(skills, "bind_state", None)
        )

    async def on_reply(self, agent, input_kwargs, next_handler):
        if self.bind_state is not None:
            await self.bind_state(agent.state)
        if self.skills is not None:
            agent.toolkit.tool_groups[0].skills_or_loaders = [self.skills]
        async for event in next_handler(**input_kwargs):
            yield event
