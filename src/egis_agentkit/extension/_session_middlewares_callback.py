"""原生会话装配时调用，按会话和专家执行方式构建 Middleware。"""

import hashlib

from agentscope.app.storage._utils import _resolve_team_leader
from agentscope.middleware import MiddlewareBase
from agentscope.workspace import WorkspaceBase

from egis_agentkit.errors import Forbidden
from egis_agentkit.extension import (
    FileChangesMiddleware,
    SkillBindingMiddleware,
    UserPreferenceMiddleware,
    WorkspacePromptMiddleware,
)
from egis_agentkit.extension._agent_remote_middleware_impl import (
    RemoteAgentMiddlewareImpl,
)
from egis_agentkit.extension._skill_loader_impl import LiveSkillLoader
from egis_agentkit.extension._tool_access_callback import (
    team_prompt_middleware,
    tool_access_middleware,
)
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.storage.model._remote_agent import RemoteAgentConfig

from ._session_lock_override import bound_execution_scope


class SessionMiddlewareFactory:
    """Compose session middleware with explicitly supplied application services."""

    def __init__(
        self,
        storage,
        *,
        users,
        personal,
        file_changes,
        memory=None,
        resources=None,
        remote_agents=None,
    ):
        self.storage = storage
        self.remote_agents = remote_agents
        self.users, self.personal = users, personal
        self.file_changes, self.memory = file_changes, memory
        self.resources = resources

    async def __call__(
        self, user_id: str, agent_id: str, session_id: str, workspace: WorkspaceBase
    ) -> list[MiddlewareBase]:
        scope = bound_execution_scope(user_id, agent_id, session_id)
        user = await self.users.get(user_id)
        policies = scope.policies
        personal = scope.definition["agent_type"] == "personal"
        preferences = await self.personal.read_documents(user) if personal else {}
        skills = (
            LiveSkillLoader(user, scope, self.resources)
            if self.resources is not None
            else None
        )

        async def authorize_tool(agent, tool, parameters=None):
            current_user = await self.users.get(user_id)
            if await self.resources.authorize_mcp_tool(current_user, scope, tool):
                return
            await self.resources.tool_registry.authorize(
                current_user, scope, agent, tool, parameters
            )

        tool_authorizer = (
            authorize_tool
            if self.resources is not None
            and getattr(self.resources, "tool_registry", None) is not None
            else None
        )
        result: list[MiddlewareBase] = [
            FileChangesMiddleware(self.file_changes, scope),
            tool_access_middleware(
                user,
                scope,
                PermissionService(self.storage, policies),
                skills,
                authorize_tool=tool_authorizer,
            ),
            SkillBindingMiddleware(
                skills, bind_state=skills.bind_state if skills is not None else None
            ),
            WorkspacePromptMiddleware(scope),
            team_prompt_middleware(scope),
            UserPreferenceMiddleware("\n\n".join(preferences.values())),
        ]
        payload = scope.definition["payload"]
        if scope.definition["agent_type"] == "remote":
            if self.remote_agents is None:
                raise Forbidden("未配置远程专家执行服务")
            config = RemoteAgentConfig.model_validate(payload["remote_config"])
            digest = hashlib.sha256(
                f"{scope.definition['id']}:{config.model_dump_json()}".encode()
            ).hexdigest()
            leader_name = None
            if scope.session_record.team_id is not None:
                team = await self.storage.get_team(
                    user_id, scope.session_record.team_id
                )
                if team is not None and team.session_id != session_id:
                    leader = await _resolve_team_leader(self.storage, user_id, team)
                    if leader is None:
                        raise Forbidden("未找到远程专家的组长会话")
                    leader_name = leader.name
            result.append(
                RemoteAgentMiddlewareImpl(
                    self.remote_agents.client(
                        digest,
                        config,
                        settings=scope.configuration.agent.remote
                        if scope.configuration
                        else None,
                    ),
                    session_id,
                    leader_name,
                )
            )
        if personal and self.memory is not None:
            result.append(await self.memory.for_user(user))
        return result
