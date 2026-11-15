"""实现 MiddlewareBase，在工具展示及运行前检查本轮授权。"""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING, Awaitable, Callable, Mapping, Protocol

from agentscope.middleware import MiddlewareBase
from agentscope.permission import PermissionBehavior, PermissionDecision

from egis_agentkit.errors import AgentKitError, Forbidden

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


class ToolPaths(Protocol):
    """工具策略所需的工作与交付目录，由调用方提供。"""

    @property
    def workdir(self) -> Path: ...

    @property
    def artifacts_dir(self) -> Path: ...


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
        scope: ToolPaths,
        *,
        validate_access: Callable[[], Awaitable[None]],
        may_send_team_message: Callable[[], bool],
        prepare_collaboration: Callable[[Agent, object], Awaitable[None]] | None = None,
        skills: SkillAccess | None = None,
        authorize_tool: Callable[..., Awaitable[None]] | None = None,
    ):
        self.scope = scope
        self.validate_access = validate_access
        self.may_send_team_message = may_send_team_message
        self.prepare_collaboration = prepare_collaboration
        self.skills = skills
        self.authorize_tool = authorize_tool

    async def on_reply(self, agent, input_kwargs, next_handler):
        if self.prepare_collaboration is not None:
            await self.prepare_collaboration(agent, input_kwargs.get("inputs"))
        disabled = list(DYNAMIC_TOOLS)
        if not self.may_send_team_message():
            disabled.append("TeamSay")
        if self.authorize_tool is not None:
            for group in agent.toolkit.tool_groups:
                for tool in group.tools:
                    if tool.name in disabled:
                        continue
                    try:
                        await self.authorize_tool(agent, tool)
                    except AgentKitError:
                        disabled.append(tool.name)
        await agent.toolkit.remove_tool(disabled)
        async for event in next_handler(**input_kwargs):
            yield event

    async def on_model_call(self, agent, input_kwargs, next_handler):
        if self.authorize_tool is None or not input_kwargs.get("tools"):
            return await next_handler(**input_kwargs)
        # Native toolkit adds certain tools while preparing model input, after
        # on_reply. Check those and active MCP tools against the same policy.
        selected = await agent.toolkit._get_available_tools(
            agent.state.tool_context.activated_groups
        )
        allowed = []
        for schema in input_kwargs["tools"]:
            registered = selected.get(schema.get("function", {}).get("name"))
            if registered is None or self._team_restricted(registered.tool.name):
                continue
            try:
                await self.authorize_tool(agent, registered.tool)
            except AgentKitError:
                continue
            allowed.append(schema)
        return await next_handler(**{**input_kwargs, "tools": allowed})

    def _team_restricted(self, name):
        return (
            name in DYNAMIC_TOOLS
            or name == "TeamSay"
            and not self.may_send_team_message()
        )

    async def on_check_permission(self, agent, input_kwargs, next_handler):
        tool = input_kwargs["tool"]
        try:
            await self.validate_access()
            if self._team_restricted(tool.name):
                raise Forbidden("当前角色不能动态创建或调整专家团")
            if self.skills is not None:
                await self.skills.bind_state(agent.state)
                await self.skills.refresh_access()
            data = input_kwargs["tool_input"]
            if self.authorize_tool is not None:
                await self.authorize_tool(agent, tool, data)
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
