"""覆盖原生会话锁及订阅，在锁内绑定本轮执行上下文。"""

from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass, field
from pathlib import Path

from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.storage import AgentRecord, SessionRecord

from egis_agentkit.client.nacos._settings import AccessConfig, AgentKitConfig
from egis_agentkit.core.agent._definition_base import AgentDefinitionBase
from egis_agentkit.core.agent._personal_definition import PersonalDefinition
from egis_agentkit.errors import AgentKitError, Forbidden, NotFound
from egis_agentkit.extension._message_bus_impl import EgisMessageBus
from egis_agentkit.service._user import UserService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.service.project._project_manager import ProjectManager
from egis_agentkit.service.team._collaboration import validate_collaborators
from egis_agentkit.service.team._members import team_member_definitions
from egis_agentkit.storage.project._files import ProjectFileStorage


@dataclass(frozen=True)
class ExecutionScope:
    """锁持有期间可用的会话、定义与工作区。"""

    user_id: str
    agent_id: str
    session_id: str
    definition: dict
    root_definition: dict
    binding: dict
    session_record: SessionRecord
    workdir: Path
    artifacts_dir: Path
    project_id: str = ""
    policies: AccessConfig | None = None
    collaborator_definitions: dict = field(default_factory=dict)
    member_definitions: dict = field(default_factory=dict)
    configuration: AgentKitConfig | None = None
    preferences: dict[str, str] | None = None


execution_scope: ContextVar[ExecutionScope | None] = ContextVar(
    "egis_execution_scope", default=None
)
execution_setup_error: ContextVar[AgentKitError | None] = ContextVar(
    "egis_execution_setup_error", default=None
)
# A transport hint only. The native stream remains the event producer and
# controls its own subscribe-before-replay ordering.
stream_subscription_ready: ContextVar[object | None] = ContextVar(
    "egis_stream_subscription_ready", default=None
)


class ExecutionCoordinator:
    """在原生会话加锁后装配固定版本并检查实时权限。"""

    def __init__(
        self,
        storage,
        workspace_root,
        config_provider=None,
        *,
        models=None,
        projects=None,
        users=None,
        personal=None,
    ):
        self.storage = storage
        self.files = ProjectFileStorage(Path(workspace_root))
        self.config_provider = config_provider
        self.models = models
        self.personal = personal or PersonalManager(
            storage, workspace_root=workspace_root, config=config_provider
        )
        self.users = users if users is not None else UserService(storage)
        self.projects = (
            projects
            if projects is not None
            else ProjectManager(storage, workspace_root)
        )

    @asynccontextmanager
    async def bind(self, session_id):
        """从会话引用重建执行上下文，结束时释放上下文变量。"""
        locator = await self.storage.locate_session(session_id)
        if locator is None:
            raise NotFound("会话不存在")
        user_id, agent_id = locator
        session = await self.storage.read_session_unscoped(
            user_id, agent_id, session_id
        )
        user = await self.users.get(user_id)
        projects = self.projects
        if not session.state.middle_context.get("egis_agentkit", {}).get(
            "is_initialized"
        ):
            raise Forbidden("会话尚未完成初始化")
        project = await projects.session_project(user, session)
        binding = deepcopy(session.state.middle_context["egis_agentkit"])
        configuration = self.config_provider.snapshot if self.config_provider else None
        policies = configuration.access if configuration is not None else None
        permissions = PermissionService(self.storage, policies)
        # 普通回复、工具确认恢复和 Team 成员均使用会话绑定的版本。
        root = await self.storage.definitions.get_definition_by_reference(binding)
        members = await team_member_definitions(self.storage, root)
        definition = members.get(binding["agent_definition_id"])
        if definition is None:
            raise Forbidden("会话绑定的智能体不在固定版本中")
        await permissions.require_execution(user, root, allow_pinned=True)
        invited = await validate_collaborators(permissions, user, binding)
        parked = session.state.has_awaiting_tool_calls(definition["agent_name"])
        if parked and binding.get("chat_model_config"):
            from agentscope.app.storage import ChatModelConfig

            session.config.chat_model_config = ChatModelConfig.model_validate(
                binding["chat_model_config"]
            )
        elif not binding.get("is_team") and self.models:
            session.config.chat_model_config = await self.models.resolve(
                user,
                binding.get("selected_model_key")
                or definition["payload"].get("model_key"),
                is_override=binding.get("selected_model_key") is not None,
                snapshot=configuration,
            )
        if session.config.chat_model_config:
            binding["chat_model_config"] = session.config.chat_model_config.model_dump(
                mode="json"
            )
        session.state.middle_context["egis_agentkit"] = binding
        workdir = self.files.area_root(user_id, project["id"], "work")
        artifacts = self.files.area_root(user_id, project["id"], "artifacts")
        scope = ExecutionScope(
            user_id,
            agent_id,
            session_id,
            definition,
            root,
            binding,
            session,
            workdir,
            artifacts,
            project["id"],
            policies=policies,
            collaborator_definitions=invited,
            member_definitions=members,
            configuration=configuration,
            preferences=await self.personal.get_preferences(user)
            if definition["agent_type"] == "personal"
            else None,
        )
        async with projects.guard(user, project["id"]):
            await asyncio.to_thread(projects.initialize_files, user_id, project["id"])
            token = execution_scope.set(scope)
            try:
                yield scope
            finally:
                execution_scope.reset(token)


class ScopedMessageBus(EgisMessageBus):
    """Delegate native bus primitives; add only the session-lock scope."""

    coordinator: ExecutionCoordinator | None = None

    async def subscribe(self, key, *, on_ready=None):
        ready = stream_subscription_ready.get()

        def subscribed():
            if on_ready is not None:
                on_ready()
            if ready is not None and key.startswith(MessageBusKeys.session_events("")):
                ready.set()

        async with aclosing(
            super().subscribe(key, on_ready=subscribed)
        ) as subscription:
            async for payload in subscription:
                yield payload

    @asynccontextmanager
    async def acquire_lock(self, key, ttl_secs=600):
        prefix = MessageBusKeys.session_lock("")
        async with super().acquire_lock(key, ttl_secs=ttl_secs):
            if key.startswith(prefix) and self.coordinator is not None:
                async with AsyncExitStack() as stack:
                    try:
                        await stack.enter_async_context(
                            self.coordinator.bind(key[len(prefix) :])
                        )
                    except AgentKitError as exc:
                        # The lock is outside native ChatService's setup try.
                        # Defer to the workspace adapter, after native setup
                        # resolves Team identity and before tools/models run.
                        # Native failure handling can then notify the leader.
                        token = execution_setup_error.set(exc)
                        try:
                            yield
                        finally:
                            execution_setup_error.reset(token)
                    else:
                        yield
            else:
                yield


def projected_agent(record: AgentRecord, scope: ExecutionScope) -> AgentRecord:
    definition = scope.definition
    personal = definition["agent_type"] == "personal"
    builder = PersonalDefinition if personal else AgentDefinitionBase
    return builder.build_agent(
        record.user_id,
        definition,
        record=record,
        preferences=getattr(scope, "preferences", None),
    )


def bound_execution_scope(
    user_id: str, agent_id: str, session_id: str
) -> ExecutionScope:
    scope = execution_scope.get()
    if scope is None or (scope.user_id, scope.agent_id, scope.session_id) != (
        user_id,
        agent_id,
        session_id,
    ):
        raise Forbidden("Agent 装配缺少固定执行上下文")
    return scope
