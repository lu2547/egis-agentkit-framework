from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Awaitable, Callable, Mapping, Protocol

from agentscope.middleware import MiddlewareBase
from agentscope.permission import PermissionBehavior, PermissionDecision

from egis_agentkit.errors import AgentKitError, Forbidden

from .workspace_prompt import WorkspacePaths

if TYPE_CHECKING:
    from agentscope.agent import Agent
    from agentscope.skill import Skill
    from agentscope.state import AgentState

DYNAMIC_TOOLS = [
    "TeamCreate",
    "AgentCreate",
    "AgentInvite",
    "TeamDelete",
    "ScheduleCreate",
    "ScheduleDelete",
]


class SkillAccess(Protocol):
    @property
    def loaded(self) -> Mapping[str, Skill]: ...

    async def bind_state(self, state: AgentState) -> None: ...

    async def refresh_access(self) -> None: ...

    async def activate(self, name: str) -> None: ...


class ToolAccessMiddleware(MiddlewareBase):
    """Enforce enterprise eligibility, then defer to native tool permissions."""

    def __init__(
        self,
        user=None,
        scope: WorkspacePaths | None = None,
        *,
        permissions=None,
        validate_access: Callable[[], Awaitable[None]] | None = None,
        may_send_team_message: Callable[[], bool] | None = None,
        prepare_collaboration: Callable[[Agent, object], Awaitable[None]] | None = None,
        skills: SkillAccess | None = None,
    ):
        if permissions is not None:
            from ..compatibility.policy import tool_access_callbacks

            if scope is None:
                raise TypeError("Legacy tool access requires an execution scope")
            self.user, self.permissions = user, permissions
            validate_access, may_send_team_message, prepare_collaboration = (
                tool_access_callbacks(user, scope, permissions)
            )
        elif scope is None:
            scope = (
                user  # The callback form also accepts the scope as its first argument.
            )
        if scope is None or validate_access is None or may_send_team_message is None:
            raise TypeError("Tool access requires a scope and access callbacks")

        self.scope = scope
        self.validate_access = validate_access
        self.may_send_team_message = may_send_team_message
        self.prepare_collaboration = prepare_collaboration
        self.skills = skills

    async def on_reply(self, agent, input_kwargs, next_handler):
        if self.prepare_collaboration is not None:
            await self.prepare_collaboration(agent, input_kwargs.get("inputs"))
        disabled = list(DYNAMIC_TOOLS)
        if not self.may_send_team_message():
            disabled.append("TeamSay")
        await agent.toolkit.remove_tool(disabled)
        async for event in next_handler(**input_kwargs):
            yield event

    async def on_check_permission(self, agent, input_kwargs, next_handler):
        tool = input_kwargs["tool"]
        try:
            await self.validate_access()
            if (
                tool.name in DYNAMIC_TOOLS
                or tool.name == "TeamSay"
                and not self.may_send_team_message()
            ):
                raise Forbidden("当前角色不能动态创建或调整专家团")
            if self.skills is not None:
                await self.skills.bind_state(agent.state)
                await self.skills.refresh_access()
            data = input_kwargs["tool_input"]
            if tool.name in ("Read", "Write", "Edit", "Glob", "Grep"):
                value = data.get("file_path") or data.get("path")
                if tool.name in ("Glob", "Grep") and not value:
                    # Native local search falls back to backend.getcwd(), which
                    # is the service process cwd, not the project workdir.
                    value = "."
                if value:
                    candidate = Path(value).resolve()
                    allowed = [
                        self.scope.workdir.resolve(),
                        self.scope.artifacts_dir.resolve(),
                    ]
                    if tool.name in ("Read", "Glob", "Grep"):
                        # Uploads belong to the project, not this session.
                        allowed.append(self.scope.workdir.parent.resolve())
                        if self.skills is not None:
                            allowed.extend(
                                Path(skill.dir)
                                for skill in self.skills.loaded.values()
                                if Path(skill.dir).resolve() == Path(skill.dir)
                            )
                    if not any(candidate.is_relative_to(root) for root in allowed):
                        raise Forbidden(
                            "文件不在当前任务或已激活技能的范围内，请先通过 Skill 工具激活所需技能"
                        )
            # Native ASK/DENY must not activate a skill. A confirmed retry also
            # traverses this middleware and activates only after native ALLOW.
            decision = await next_handler(**input_kwargs)
            if (
                tool.name == "Skill"
                and self.skills is not None
                and decision.behavior == PermissionBehavior.ALLOW
            ):
                await self.skills.activate(input_kwargs["tool_input"]["skill"])
            return decision
        except AgentKitError as exc:
            return PermissionDecision(PermissionBehavior.DENY, str(exc))
