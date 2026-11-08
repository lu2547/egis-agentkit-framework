from agentscope.middleware import MiddlewareBase
from agentscope.tool import ToolBase
from agentscope.workspace import WorkspaceBase

from egis_agentkit.errors import Forbidden
from egis_agentkit.extensions.compatibility.execution import (
    ExecutionScope,
    execution_scope,
)
from egis_agentkit.extensions.compatibility.policy import (
    team_prompt_middleware,
    tool_access_middleware,
)
from egis_agentkit.extensions.middleware import (
    FileChangesMiddleware,
    SkillBindingMiddleware,
    UserPreferenceMiddleware,
    WorkspacePromptMiddleware,
)
from egis_agentkit.extensions.skill.loader import LiveSkillLoader
from egis_agentkit.packages import Assembly, AssemblyContext
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.user.memory import MemoryService
from egis_agentkit.service.user.preferences import PreferenceService
from egis_agentkit.service.user.service import UserService
from egis_agentkit.service.workspace.changes import FileChangeService

_UNSET = object()


def _scope(user_id: str, agent_id: str, session_id: str) -> ExecutionScope:
    scope = execution_scope.get()
    if scope is None or (scope.user_id, scope.agent_id, scope.session_id) != (
        user_id,
        agent_id,
        session_id,
    ):
        raise Forbidden("Agent 装配缺少固定执行上下文")
    return scope


def _assembly(
    scope: ExecutionScope,
) -> Assembly[AssemblyContext | ExecutionScope] | None:
    if scope.assembly is not None:
        return scope.assembly
    if not scope.definition["payload"].get("implementation"):
        return None
    raise Forbidden("执行上下文没有持有已注册的装配版本")


class BoundToolFactory:
    """Native three-ID callback; use only the assembly pinned in the scope."""

    async def __call__(
        self, user_id: str, agent_id: str, session_id: str
    ) -> list[ToolBase]:
        scope = _scope(user_id, agent_id, session_id)
        assembly = _assembly(scope)
        if assembly is None or assembly.tools_factory is None:
            return []
        return await assembly.tools_factory(scope.assembly_context)


class SessionMiddlewareFactory:
    """Compose session middleware with explicitly supplied application services."""

    def __init__(
        self,
        storage,
        *,
        users,
        preferences,
        file_changes,
        memory=None,
        resources=None,
        config_provider=None,
    ):
        self.storage = storage
        self.users, self.preferences = users, preferences
        self.file_changes, self.memory = file_changes, memory
        self.resources, self.config = resources, config_provider

    async def __call__(
        self, user_id: str, agent_id: str, session_id: str, workspace: WorkspaceBase
    ) -> list[MiddlewareBase]:
        scope = _scope(user_id, agent_id, session_id)
        assembly = _assembly(scope)
        user = await self.users.get(user_id)
        policies = getattr(scope, "policies", _UNSET)
        if policies is _UNSET:
            policies = self.config.snapshot.policies if self.config else None
        personal = scope.definition["agent_type"] == "personal"
        preferences = await self.preferences.read(user) if personal else {}
        skills = (
            LiveSkillLoader(user, scope, self.resources)
            if self.resources is not None
            else None
        )
        result: list[MiddlewareBase] = [
            FileChangesMiddleware(self.file_changes, scope),
            tool_access_middleware(
                user, scope, PermissionService(self.storage, policies), skills
            ),
            SkillBindingMiddleware(
                skills, bind_state=skills.bind_state if skills is not None else None
            ),
            WorkspacePromptMiddleware(scope),
            team_prompt_middleware(scope),
            UserPreferenceMiddleware("\n\n".join(preferences.values())),
        ]
        if personal and self.memory is not None:
            result.append(await self.memory.for_user(user))
        if assembly is not None and assembly.middlewares_factory is not None:
            result.extend(
                await assembly.middlewares_factory(scope.assembly_context, workspace)
            )
        return result


class AgentFactories:
    """Compatibility facade; omitted services retain legacy construction defaults.

    An explicit ``memory=None`` disables memory, even when configured as enabled.
    Supplied services remain owned by the caller, including their lifecycle.
    """

    def __init__(
        self,
        storage,
        loader,
        workspace_root,
        resources=None,
        config_provider=None,
        *,
        users=None,
        preferences=None,
        memory=_UNSET,
        file_changes=None,
    ):
        self.storage, self.loader, self.root = storage, loader, workspace_root
        self.resources, self.config = resources, config_provider
        if users is None:
            users = UserService(storage)
        if preferences is None:
            preferences = PreferenceService(storage, workspace_root)
        if file_changes is None:
            file_changes = FileChangeService(storage, workspace_root)
        if memory is _UNSET:
            memory = (
                MemoryService(storage, workspace_root, config_provider)
                if config_provider
                and config_provider.snapshot.runtime.memory.is_enabled
                else None
            )
        self.tool_factory = BoundToolFactory()
        self.middleware_factory = SessionMiddlewareFactory(
            storage,
            users=users,
            preferences=preferences,
            file_changes=file_changes,
            memory=memory,
            resources=resources,
            config_provider=config_provider,
        )

    @property
    def preferences(self):
        return self.middleware_factory.preferences

    @property
    def memory(self):
        return self.middleware_factory.memory

    @memory.setter
    def memory(self, value):
        self.middleware_factory.memory = value

    async def tools(self, user_id, agent_id, session_id):
        return await self.tool_factory(user_id, agent_id, session_id)

    async def middlewares(self, user_id, agent_id, session_id, workspace):
        return await self.middleware_factory(user_id, agent_id, session_id, workspace)
