from pathlib import Path

from agentscope.middleware import MiddlewareBase
from agentscope.permission import PermissionBehavior, PermissionDecision

from egis_agentkit.errors import AgentKitError, Forbidden
from egis_agentkit.extensions.compatibility.collaboration import apply_invitations
from egis_agentkit.extensions.skill.loader import LiveSkillLoader
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.team.collaboration import (
    team_say_allowed,
    validate_collaborators,
)

DYNAMIC_TOOLS = [
    "TeamCreate",
    "AgentCreate",
    "AgentInvite",
    "TeamDelete",
    "ScheduleCreate",
    "ScheduleDelete",
]


class ToolAccessMiddleware(MiddlewareBase):
    """Enforce enterprise eligibility, then defer to native tool permissions."""

    def __init__(
        self,
        user,
        scope,
        *,
        permissions: PermissionService,
        skills: LiveSkillLoader | None = None,
    ):
        self.user, self.scope = user, scope
        self.permissions, self.skills = permissions, skills

    async def on_reply(self, agent, input_kwargs, next_handler):
        if self.scope.binding.get("collaboration"):
            native_create = await agent.toolkit.get_tool("TeamCreate")
            await apply_invitations(
                self.user, self.scope, agent.state, native_create,
                input_kwargs.get("inputs"), self.permissions,
            )
        disabled = list(DYNAMIC_TOOLS)
        if not team_say_allowed(self.scope.session_record, self.scope.binding):
            disabled.append("TeamSay")
        await agent.toolkit.remove_tool(disabled)
        async for event in next_handler(**input_kwargs):
            yield event

    async def on_check_permission(self, agent, input_kwargs, next_handler):
        tool = input_kwargs["tool"]
        try:
            await self.permissions.validate_snapshot(self.user, self.scope.snapshot)
            await validate_collaborators(self.permissions, self.user, self.scope.binding)
            if (
                tool.name in DYNAMIC_TOOLS
                or tool.name == "TeamSay"
                and not team_say_allowed(self.scope.session_record, self.scope.binding)
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
                        raise Forbidden("文件不在当前任务或已激活技能的范围内，请先通过 Skill 工具激活所需技能")
            # Native ASK/DENY must not activate a skill. A confirmed retry also
            # traverses this middleware and activates only after native ALLOW.
            decision = await next_handler(**input_kwargs)
            if (tool.name == "Skill" and self.skills is not None
                    and decision.behavior == PermissionBehavior.ALLOW):
                await self.skills.activate(input_kwargs["tool_input"]["skill"])
            return decision
        except AgentKitError as exc:
            return PermissionDecision(PermissionBehavior.DENY, str(exc))
