"""组装 AgentKit 的业务服务和运行时扩展，管理组件启动和关闭顺序。"""

import logging
import os
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from functools import partial

from agentscope.app.workspace_manager import WorkspaceManagerBase
from sqlalchemy.ext.asyncio import AsyncEngine

from egis_agentkit.bootstrap._demo_tools import deploy_demo_report_tools
from egis_agentkit.bootstrap._middlewares import build_middlewares
from egis_agentkit.bootstrap._skill_tools import deploy_skill_tools
from egis_agentkit.client._logging import LoggingClient
from egis_agentkit.client._postgres import (
    check_postgres,
    create_postgres_engine_from_config,
    postgres_url,
)
from egis_agentkit.client._tracing import TracingClient
from egis_agentkit.client.nacos import NacosClient, NacosConfigProvider
from egis_agentkit.client.nacos._config import LivePolicies
from egis_agentkit.client.sandbox import SandboxClient
from egis_agentkit.core.agent._expert_definition import ExpertDefinition
from egis_agentkit.core.agent._remote_definition import RemoteDefinition
from egis_agentkit.middleware.agent._project_memory_middleware_override import (
    ProjectMemoryMiddlewareOverride,
)
from egis_agentkit.middleware.workspace._workspace_read_guard_middleware_impl import (
    ReadToolGuardMiddleware,
)
from egis_agentkit.service._composer import ComposerService
from egis_agentkit.service._model import ModelService
from egis_agentkit.service._session_feedback import SessionFeedbackService
from egis_agentkit.service._user import UserService
from egis_agentkit.service._virtual_user import VirtualUserService
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.agent._remote_agent import (
    RemoteAgentService,
)
from egis_agentkit.service.project._document_preview import DocumentPreviewService
from egis_agentkit.service.project._file_manager import ProjectFileManager
from egis_agentkit.service.project._memory import ProjectMemoryService
from egis_agentkit.service.project._presentation_manager import (
    ProjectPresentationManager,
)
from egis_agentkit.service.project._project_manager import ProjectManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._hubs import (
    LocalMCPHub,
    LocalSkillHub,
)
from egis_agentkit.service.resource._skill_import import SkillImportService
from egis_agentkit.service.schedule._service import ScheduleService
from egis_agentkit.service.schedule._worker import ScheduleWorker
from egis_agentkit.service.session._message_bus_override import SessionMessageBus
from egis_agentkit.service.session._preparation import SessionPreparationService
from egis_agentkit.service.session._authorization import ChatAuthorizationStore
from egis_agentkit.service.session._session import SessionService
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.service.workspace._egis_sandbox_manager import (
    EgisSandboxManager,
)
from egis_agentkit.storage import AgentKitStorage
from egis_agentkit.storage._session_execution import SessionExecutionStorage
from egis_agentkit.storage.project._files import ProjectFileStorage
from egis_agentkit.storage.project._preview_cache import PreviewCacheStorage

from ._services import AgentKitServices

logger = logging.getLogger(__name__)


@dataclass(frozen=True, slots=True)
class AgentKit:
    services: AgentKitServices
    native_storage: SessionExecutionStorage
    bus: SessionMessageBus
    workspace: WorkspaceManagerBase
    skill_hub: LocalSkillHub
    mcp_hub: LocalMCPHub
    tools: Callable
    middlewares: Callable
    remote_agents: RemoteAgentService
    _storage: AgentKitStorage
    _config: NacosConfigProvider
    _owned_client: NacosClient | None
    _skill_imports: SkillImportService
    _schedule_worker: ScheduleWorker
    _memory: ProjectMemoryService | None
    _owns_engine: bool
    _logging: LoggingClient | None
    _tracing: TracingClient

    async def check_database(self) -> None:
        await check_postgres(self._storage.engine)

    @asynccontextmanager
    async def lifespan(self, native_lifespan: AbstractAsyncContextManager):
        phase = "logging"
        try:
            async with AsyncExitStack() as stack:
                # 观测初始化失败也释放已创建的资源。
                stack.push_async_callback(self._tracing.aclose)
                if self._owns_engine:
                    stack.push_async_callback(self._storage.engine.dispose)
                if self._owned_client is not None:
                    stack.push_async_callback(self._owned_client.close)
                stack.push_async_callback(self._config.close)
                if self._memory is not None:
                    stack.push_async_callback(self._memory.close)
                if self._logging is not None:
                    self._logging.start()
                phase = "tracing"
                self._tracing.start()
                logger.info(
                    "agentkit.starting",
                    extra={
                        "component": "AgentKit.lifespan",
                        "context": {
                            "revision": self._config.snapshot.revision,
                            "tracing_enabled": self._config.snapshot.client.tracing.enabled,
                        },
                    },
                )
                phase = "nacos"
                await self._config.start()
                phase = "storage"
                await stack.enter_async_context(self._storage)
                phase = "builtin_tools"
                await self.services.resources.seed_builtin_tools()
                phase = "native_app"
                await stack.enter_async_context(native_lifespan)
                phase = "skill_imports"
                stack.push_async_callback(self._skill_imports.close)
                await self._skill_imports.start()
                phase = "schedules"
                stack.push_async_callback(self._schedule_worker.close)
                await self._schedule_worker.start()
                phase = "running"
                logger.info("agentkit.ready", extra={"component": "AgentKit.lifespan"})
                try:
                    yield
                finally:
                    phase = "shutdown"
                    logger.info(
                        "agentkit.stopping", extra={"component": "AgentKit.lifespan"}
                    )
            logger.info("agentkit.stopped", extra={"component": "AgentKit.lifespan"})
        except Exception:
            if phase != "logging":
                logger.exception(
                    "AgentKit lifecycle failed",
                    extra={
                        "component": "AgentKit.lifespan",
                        "context": {"category": "system", "phase": phase},
                    },
                )
            raise
        finally:
            if self._logging is not None:
                self._logging.close()


def init_agentkit(
    config: NacosConfigProvider,
    client: NacosClient,
    *,
    engine: AsyncEngine | None = None,
    owns_client: bool = True,
    manage_logging: bool = True,
    sandbox_client_factory: Callable[[], SandboxClient] | None = None,
) -> AgentKit:
    snapshot = config.snapshot
    postgres = snapshot.client.postgres
    workspace = snapshot.workspace
    root = workspace.runtime.root
    files = ProjectFileStorage(root, config_provider=config)
    if files.uses_nas_api and snapshot.project.memory.is_enabled:
        raise ValueError(
            "HTTP 文件模式下，ReMe 项目记忆需要关闭 project.memory.isEnabled"
        )
    # 具体沙箱通信只在装配点选择；公司实现可直接注入中立 Client 工厂。
    runtime = workspace.runtime
    sandbox_settings = runtime.sandbox
    if sandbox_client_factory is None:
        if not sandbox_settings.domain:
            raise ValueError("未配置沙箱 domain，必须注入 sandbox_client_factory")
        from egis_agentkit.client.sandbox._opensandbox import OpenSandboxClient

        api_key = os.environ.get(sandbox_settings.api_key_env)
        if not api_key:
            raise ValueError(f"缺少沙箱凭据环境变量 {sandbox_settings.api_key_env}")
        sandbox_client_factory = partial(
            OpenSandboxClient, sandbox_settings, api_key=api_key
        )
    owns_engine = engine is None
    if engine is None:
        engine = create_postgres_engine_from_config(postgres)
    database_url = postgres_url(postgres)
    storage = AgentKitStorage(database_url, engine)
    native_storage = SessionExecutionStorage(database_url, engine)
    tracing = TracingClient(snapshot.client.tracing)
    bus = SessionMessageBus(
        engine=engine,
        kafka=snapshot.client.kafka,
        tracing=tracing,
        presentation_storage=storage,
    )
    remote_agents = RemoteAgentService(storage.remote_runs, bus, config=config)
    policies = LivePolicies(config)
    users = UserService(storage, policies)
    experts = ExpertDefinition()
    remote_definition = RemoteDefinition()
    agents = AgentManager(
        storage,
        policies,
        config_provider=config,
        expert=experts,
        remote=remote_definition,
    )
    models = ModelService(storage, config)
    memory = (
        ProjectMemoryService(
            storage, root, config, middleware_factory=ProjectMemoryMiddlewareOverride
        )
        if snapshot.project.memory.is_enabled
        else None
    )
    resources = ResourceService(storage, root, policies=policies, config=config)
    workspace_manager = EgisSandboxManager(
        root,
        sandbox_settings,
        client_factory=sandbox_client_factory,
        bus=bus,
        probe=workspace.probe,
        lock=workspace.lock,
        files=files,
        read_guard_factory=partial(
            ReadToolGuardMiddleware, settings=workspace.read_guard
        ),
    )
    projects = ProjectManager(
        storage,
        root,
        files=files,
        config_provider=config,
        workspaces=workspace_manager,
    )
    personal = resources.personal
    authorizations = ChatAuthorizationStore(bus)
    tasks = SessionService(
        storage, root, models, config, projects=projects, personal=personal,
        authorizations=authorizations,
    )
    schedules = ScheduleService(storage, users, tasks, projects, models, personal)
    resources.tool_registry.schedule_service = schedules
    bus.coordinator = SessionPreparationService(
        storage,
        root,
        config,
        models=models,
        projects=projects,
        users=users,
        personal=personal,
        authorizations=authorizations,
    )

    deploy_demo_report_tools(resources.tool_registry, files)
    project_files = ProjectFileManager(
        storage,
        root,
        files=files,
        projects=projects,
        sessions=tasks,
        preview=DocumentPreviewService(
            config_provider=config, cache_storage=PreviewCacheStorage(storage.engine)
        ),
        config_provider=config,
    )
    virtual_users = VirtualUserService(storage.virtual_users)
    skill_imports = SkillImportService(storage, resources, virtual_users=virtual_users)
    deploy_skill_tools(resources.tool_registry, skill_imports)
    services = AgentKitServices(
        users=users,
        virtual_users=virtual_users,
        agents=agents,
        personal=personal,
        teams=TeamDefinition(storage, policies, config_provider=config),
        grants=GrantService(storage, policies),
        sharing=ShareService(storage),
        models=models,
        projects=projects,
        resources=resources,
        skill_imports=skill_imports,
        sessions=tasks,
        schedules=schedules,
        session_feedback=SessionFeedbackService(storage, tasks, config),
        project_files=project_files,
        presentations=ProjectPresentationManager(project_files),
        composer=ComposerService(
            storage, agents, resources, project_files, config_provider=config
        ),
    )
    return AgentKit(
        services=services,
        native_storage=native_storage,
        bus=bus,
        workspace=workspace_manager,
        skill_hub=resources.skill_hub,
        mcp_hub=resources.mcp_hub,
        tools=partial(tasks.build_tools, resources=resources, users=users),
        middlewares=partial(
            build_middlewares,
            sessions=tasks,
            users=users,
            file_changes=project_files,
            memory=memory,
            resources=resources,
            remote_agents=remote_agents,
            workspace_manager=workspace_manager,
            tracing_enabled=snapshot.client.tracing.enabled,
        ),
        remote_agents=remote_agents,
        _storage=storage,
        _config=config,
        _owned_client=client if owns_client else None,
        _skill_imports=skill_imports,
        _schedule_worker=ScheduleWorker(schedules, bus),
        _memory=memory,
        _owns_engine=owns_engine,
        _logging=(
            LoggingClient(snapshot.logging, include_runtime=True)
            if manage_logging
            else None
        ),
        _tracing=tracing,
    )
