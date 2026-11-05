from ..errors import Forbidden
from ..middleware import EnterprisePolicyMiddleware
from ..middleware.file_changes import FileChangesMiddleware
from .assembly import execution_scope
from .file_changes import FileChangeService
from .memory import MemoryService
from .preferences import PreferenceService
from .users import UserService


class AgentFactories:
    def __init__(
        self, storage, loader, workspace_root, resources=None, config_provider=None
    ):
        self.storage, self.loader, self.root = storage, loader, workspace_root
        self.resources, self.config = resources, config_provider
        self.preferences = PreferenceService(storage, workspace_root)
        self.memory = (
            MemoryService(storage, workspace_root, config_provider)
            if (config_provider and config_provider.snapshot.runtime.memory.is_enabled)
            else None
        )

    def _scope(self, user_id, agent_id, session_id):
        scope = execution_scope.get()
        if not scope or (scope.user_id, scope.agent_id, scope.session_id) != (
            user_id,
            agent_id,
            session_id,
        ):
            raise Forbidden("Agent 装配缺少固定执行上下文")
        return scope

    def _assembly(self, scope):
        if scope.assembly is not None:
            return scope.assembly
        implementation = scope.definition["payload"].get("implementation")
        if not implementation:
            return None
        raise Forbidden("执行上下文没有持有已注册的装配版本")

    async def tools(self, user_id, agent_id, session_id):
        scope = self._scope(user_id, agent_id, session_id)
        assembly = self._assembly(scope)
        return (
            await assembly.tools_factory(scope)
            if assembly and assembly.tools_factory
            else []
        )

    async def middlewares(self, user_id, agent_id, session_id, workspace):
        scope = self._scope(user_id, agent_id, session_id)
        user = await UserService(self.storage).get(user_id)
        policies = self.config.snapshot.policies if self.config else None
        preferences = (
            await self.preferences.read(user)
            if scope.definition["agent_type"] == "personal"
            else {}
        )
        result = [
            FileChangesMiddleware(FileChangeService(self.storage, self.root), scope),
            EnterprisePolicyMiddleware(
                self.storage,
                user,
                scope,
                resources=self.resources,
                policies=policies,
                preference_text="\n\n".join(preferences.values()),
            ),
        ]
        if scope.definition["agent_type"] == "personal" and self.memory:
            result.append(await self.memory.for_user(user))
        assembly = self._assembly(scope)
        if assembly and assembly.middlewares_factory:
            result.extend(await assembly.middlewares_factory(scope, workspace))
        return result
