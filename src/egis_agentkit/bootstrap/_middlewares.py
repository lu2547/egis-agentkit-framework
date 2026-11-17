"""组装 AgentScope 对话中间件，业务回调由所属 Service 提供。

核心能力
- build_middlewares 为单次会话装配授权、技能、团队、项目和远程接入。
- build_tool_access_middleware / build_team_prompt_middleware 只将 Service 回调传给原生契约实现。
- 中间件顺序、原生 next_handler 及异常语义保持不变。
"""

import logging
import time
from functools import partial

from agentscope.middleware import MiddlewareBase, TracingMiddleware
from agentscope.workspace import WorkspaceBase

from egis_agentkit.client._tracing import trace_phase
from egis_agentkit.core.session._context import get_session_execution_context
from egis_agentkit.errors import Forbidden
from egis_agentkit.middleware import (
    SkillBindingMiddleware,
    TeamPromptMiddleware,
    ToolAccessMiddleware,
)
from egis_agentkit.middleware.agent._remote_agent_middleware_impl import (
    RemoteAgentMiddlewareImpl,
)
from egis_agentkit.middleware.session._session_callback_middleware_impl import (
    SessionCallbackMiddleware,
)
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.resource._skill_loader import LiveSkillLoader
from egis_agentkit.service.team._tool_callbacks import (
    team_prompt_arguments,
    tool_access_callbacks,
)

logger = logging.getLogger(__name__)


def build_tool_access_middleware(
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


def build_team_prompt_middleware(scope):
    fixed_members, invited_member, invited_members = team_prompt_arguments(scope)
    return TeamPromptMiddleware(
        fixed_members=fixed_members,
        invited_member=invited_member,
        invited_members=invited_members,
    )


async def build_middlewares(
    user_id: str,
    agent_id: str,
    session_id: str,
    workspace: WorkspaceBase,
    *,
    sessions,
    users,
    file_changes,
    memory=None,
    resources=None,
    remote_agents=None,
    workspace_manager=None,
    tracing_enabled=False,
) -> list[MiddlewareBase]:
    """组合已绑定会话的运行回调、授权、资源和远程执行扩展。"""
    started = time.monotonic()
    context = {"user_id": user_id, "agent_id": agent_id, "session_id": session_id}
    logger.info("session.preparing", extra={"context": context})
    scope = get_session_execution_context(user_id, agent_id, session_id)
    user = await users.get(user_id)
    if resources is not None:
        # 原生 ChatService 随后才收集 Workspace 工具与 MCP，必须先完成授权装载。
        with trace_phase("resources.prepare"):
            await resources.prepare_workspace(user, scope, workspace)
    policies = scope.policies
    personal = scope.definition["agent_type"] == "personal"
    skills = (
        LiveSkillLoader(user, scope, resources, workspace=workspace)
        if resources is not None
        else None
    )

    tool_authorizer = (
        partial(
            resources.authorize_session_tool, user_id, scope, workspace, users=users
        )
        if resources is not None
        and getattr(resources, "tool_registry", None) is not None
        else None
    )
    reply = partial(file_changes.on_reply, scope)
    if workspace_manager is not None:
        reply = partial(
            sessions.on_workspace_reply, workspace_manager, workspace, reply
        )
    result: list[MiddlewareBase] = [
        SessionCallbackMiddleware(
            reply=reply,
            acting=partial(file_changes.on_acting, scope),
            system_prompt=partial(
                sessions.projects.on_system_prompt, scope, file_changes
            ),
        ),
        build_tool_access_middleware(
            user,
            scope,
            PermissionService(sessions.storage, policies),
            skills,
            authorize_tool=tool_authorizer,
        ),
        SkillBindingMiddleware(
            skills, bind_state=skills.bind_state if skills is not None else None
        ),
        build_team_prompt_middleware(scope),
    ]
    # 原生 TracingMiddleware 包住回复、模型和工具链；导出由 Client 生命周期管理。
    if tracing_enabled:
        result.insert(0, TracingMiddleware())
    if scope.definition["agent_type"] == "remote":
        if remote_agents is None:
            raise Forbidden("未配置远程专家执行服务")
        leader_name = await sessions.get_team_leader_name(scope)
        result.append(
            RemoteAgentMiddlewareImpl(
                remote_agents.get_client_for_definition(
                    scope.definition,
                    configuration=scope.configuration,
                ),
                session_id,
                leader_name,
                runtime=remote_agents,
            )
        )
    if personal and memory is not None:
        result.append(await memory.for_user(user))
    logger.info(
        "session.prepared",
        extra={
            "context": {
                **context,
                "duration_ms": round((time.monotonic() - started) * 1000),
            }
        },
    )
    return result
