"""Bind a definition before native service reads Agent/Session/Workspace.

No execution loop lives here. Scope lifetime exactly matches the native
session lock; native State persists the JSON-only snapshot for HITL resume.
"""

import asyncio
from contextlib import AsyncExitStack, aclosing, asynccontextmanager
from contextvars import ContextVar
from copy import deepcopy
from dataclasses import dataclass
from pathlib import Path

from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.storage import AgentData, AgentRecord
from sqlalchemy import text

from egis_agentkit.errors import AgentKitError, Forbidden, NotFound
from egis_agentkit.extensions.message_bus import EgisMessageBus
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.access.snapshot import SnapshotService
from egis_agentkit.service.project.service import ProjectService
from egis_agentkit.service.team.collaboration import validate_collaborators
from egis_agentkit.service.user.service import UserService
from egis_agentkit.service.workspace.files import WorkspaceFiles


@dataclass(frozen=True)
class ExecutionScope:
    user_id: str
    agent_id: str
    session_id: str
    definition: dict
    snapshot: dict
    binding: dict
    session_record: object
    workdir: Path
    artifacts_dir: Path
    assembly: object | None = None
    project_id: str = ""


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
    def __init__(
        self,
        storage,
        workspace_root,
        config_provider=None,
        *,
        loader=None,
        models=None,
        packages=None,
        projects=None,
    ):
        self.storage = storage
        self.files = WorkspaceFiles(Path(workspace_root))
        self.config_provider = config_provider
        self.loader, self.models = loader, models
        self.packages = packages
        self.projects = (
            projects
            if projects is not None
            else ProjectService(storage, workspace_root)
        )

    @asynccontextmanager
    async def bind(self, session_id, *, session_lock_held=False):
        # Narrow native-schema adapter: identity lookup does not leak to HTTP.
        async with self.storage.engine.connect() as conn:
            locator = (
                (
                    await conn.execute(
                        text("SELECT user_id, agent_id FROM sessions WHERE id=:id"),
                        {"id": session_id},
                    )
                )
                .mappings()
                .first()
            )
        if locator is None:
            raise NotFound("会话不存在")
        user_id, agent_id = locator["user_id"], locator["agent_id"]
        session = await self.storage.read_session_unscoped(
            user_id, agent_id, session_id
        )
        user = await UserService(self.storage).get(user_id)
        projects = self.projects
        if not session.state.middle_context.get("egis_agentkit", {}).get(
            "is_initialized"
        ):
            raise Forbidden("会话尚未完成初始化")
        project = await projects.session_project(
            user, session, session_lock_held=session_lock_held
        )
        binding = deepcopy(session.state.middle_context["egis_agentkit"])
        policies = (
            self.config_provider.snapshot.policies if self.config_provider else None
        )
        await PermissionService(self.storage, policies).require(
            user,
            binding["root_resource_type"],
            binding["definition_id"],
            binding["resource_view_type"],
        )
        previous = binding.get("snapshot")
        previous_node = (
            previous["nodes"].get(binding["agent_node_key"]) if previous else None
        )
        parked = previous_node and session.state.has_awaiting_tool_calls(
            previous_node["data"]["agent_name"]
        )
        if (
            binding.get("is_team")
            or binding.get("collaboration_parent_session_id")
            or parked
        ):
            if previous is None:
                raise Forbidden("缺少固定执行快照")
            snapshot = previous
        else:
            snapshot = await SnapshotService(self.storage).capture(
                binding["definition_id"], binding["resource_view_type"]
            )
            binding["agent_node_key"] = snapshot["root_key"]
        node = snapshot["nodes"][binding["agent_node_key"]]
        await PermissionService(self.storage, policies).validate_snapshot(
            user, snapshot
        )
        await validate_collaborators(
            PermissionService(self.storage, policies), user, binding
        )
        # Availability remains current even though configuration stays pinned.
        current = await self.storage.get_definition(node["resource_id"])
        if not current["is_enabled"]:
            raise Forbidden("此智能体已停用")
        if parked and binding.get("chat_model_config"):
            from agentscope.app.storage import ChatModelConfig

            session.config.chat_model_config = ChatModelConfig.model_validate(
                binding["chat_model_config"]
            )
        elif not binding.get("is_team") and self.models:
            session.config.chat_model_config = await self.models.resolve(
                user,
                binding.get("selected_model_key")
                or node["data"]["payload"].get("model_key"),
                is_override=binding.get("selected_model_key") is not None,
            )
        if session.config.chat_model_config:
            binding["chat_model_config"] = session.config.chat_model_config.model_dump(
                mode="json"
            )
        binding["snapshot"] = snapshot
        session.state.middle_context["egis_agentkit"] = binding
        workdir = self.files.area_root(user_id, project["id"], "work")
        artifacts = self.files.area_root(user_id, project["id"], "artifacts")
        implementation = node["data"]["payload"].get("implementation")
        assembly = None
        if implementation:
            if self.packages is None:
                raise Forbidden("未配置发布包装配服务")
            assembly = await self.packages.resolve(
                implementation["assembly_ref"], implementation["assembly_version"]
            )
        scope = ExecutionScope(
            user_id,
            agent_id,
            session_id,
            node["data"],
            snapshot,
            binding,
            session,
            workdir,
            artifacts,
            assembly,
            project["id"],
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
                            self.coordinator.bind(
                                key[len(prefix) :], session_lock_held=True
                            )
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
    data = scope.definition
    payload = data["payload"]
    return record.model_copy(
        update={
            "data": AgentData(
                id=record.data.id,
                name=data["agent_name"],
                system_prompt=payload["system_prompt"],
                chat_config={"invite_config": {"invitable": False}},
            )
        },
        deep=True,
    )
