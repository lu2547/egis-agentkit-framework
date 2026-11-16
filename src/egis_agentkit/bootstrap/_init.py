"""组装 AgentKit 的业务服务和运行时扩展，管理组件启动和关闭顺序。"""

import os
from collections.abc import Callable
from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path

from agentscope.app.workspace_manager import WorkspaceManagerBase
from sqlalchemy.ext.asyncio import AsyncEngine

from egis_agentkit.client._postgres import check_postgres, create_postgres_engine
from egis_agentkit.client.nacos import NacosClient, NacosConfigProvider
from egis_agentkit.client.nacos._config import LivePolicies
from egis_agentkit.client.sandbox import SandboxClient
from egis_agentkit.core.agent._expert_definition import ExpertDefinition
from egis_agentkit.core.agent._remote_definition import RemoteDefinition
from egis_agentkit.extension.demo._demo_report_tool_impl import deploy_demo_report_tools
from egis_agentkit.extension.personal._personal_memory_callback import MemoryService
from egis_agentkit.extension.remote_agent._remote_agent_callback import (
    RemoteAgentRuntime,
)
from egis_agentkit.extension.resource._resource_hubs_impl import (
    LocalMCPHub,
    LocalSkillHub,
)
from egis_agentkit.extension.runtime._agent_tools_callback import BoundToolFactory
from egis_agentkit.extension.runtime._session_lock_override import (
    ExecutionCoordinator,
    ScopedMessageBus,
)
from egis_agentkit.extension.runtime._session_storage_override import ExecutionStorage
from egis_agentkit.service._composer import ComposerService
from egis_agentkit.service._model import ModelService
from egis_agentkit.service._session import SessionService
from egis_agentkit.service._session_feedback import SessionFeedbackService
from egis_agentkit.service._user import UserService
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.project._document_preview import DocumentPreviewService
from egis_agentkit.service.project._file_manager import ProjectFileManager
from egis_agentkit.service.project._presentation_manager import (
    ProjectPresentationManager,
)
from egis_agentkit.service.project._project_manager import ProjectManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill_import import SkillImportService
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.service.workspace._egis_sandbox_manager import (
    EgisSandboxManager,
)
from egis_agentkit.storage import AgentKitStorage
from egis_agentkit.storage.project._files import ProjectFileStorage
from egis_agentkit.storage.project._preview_cache import PreviewCacheStorage

from ._services import AgentKitServices


@dataclass(frozen=True, slots=True)
class AgentKit:
    services: AgentKitServices
    native_storage: ExecutionStorage
    bus: ScopedMessageBus
    workspace: WorkspaceManagerBase
    skill_hub: LocalSkillHub
    mcp_hub: LocalMCPHub
    tools: BoundToolFactory
    middlewares: Callable
    remote_agents: RemoteAgentRuntime
    _storage: AgentKitStorage
    _config: NacosConfigProvider
    _owned_client: NacosClient | None
    _skill_imports: SkillImportService
    _memory: MemoryService | None
    _owns_engine: bool

    async def check_database(self) -> None:
        await check_postgres(self._storage.engine)

    @asynccontextmanager
    async def lifespan(self, native_lifespan: AbstractAsyncContextManager):
        async with AsyncExitStack() as stack:
            if self._owns_engine:
                stack.push_async_callback(self._storage.engine.dispose)
            if self._owned_client is not None:
                stack.push_async_callback(self._owned_client.close)
            stack.push_async_callback(self._config.close)
            if self._memory is not None:
                stack.push_async_callback(self._memory.close)

            await self._config.start()
            await stack.enter_async_context(self._storage)
            await self.services.resources.seed_builtin_tools()
            await stack.enter_async_context(native_lifespan)
            stack.push_async_callback(self._skill_imports.close)
            await self._skill_imports.start()
            yield


def init_agentkit(
    config: NacosConfigProvider,
    client: NacosClient,
    database_url: str,
    *,
    engine: AsyncEngine | None = None,
    owns_client: bool = True,
    sandbox_client_factory: Callable[[], SandboxClient] | None = None,
) -> AgentKit:
    snapshot = config.snapshot
    postgres = snapshot.client.postgres
    workspace = snapshot.workspace
    # 具体沙箱通信只在装配点选择；公司实现可直接注入中立 Client 工厂。
    runtime = workspace.runtime
    sandbox_settings = (
        runtime.opensandbox if runtime.mode == "opensandbox" else runtime.sandbox
    )
    if sandbox_client_factory is None:
        if runtime.mode != "opensandbox":
            raise ValueError("custom 沙箱模式必须注入 sandbox_client_factory")
        from egis_agentkit.client.sandbox._opensandbox import OpenSandboxClient

        api_key = os.environ.get(runtime.opensandbox.api_key_env)
        if not api_key:
            raise ValueError(f"缺少沙箱凭据环境变量 {runtime.opensandbox.api_key_env}")
        sandbox_client_factory = partial(
            OpenSandboxClient, runtime.opensandbox, api_key=api_key
        )
    owns_engine = engine is None
    if engine is None:
        engine = create_postgres_engine(
            database_url,
            pool_size=postgres.pool.size,
            max_overflow=postgres.pool.max_overflow,
        )
    storage = AgentKitStorage(database_url, engine)
    native_storage = ExecutionStorage(database_url, engine)
    bus = ScopedMessageBus(engine=engine, kafka=snapshot.client.kafka)
    remote_agents = RemoteAgentRuntime(storage.remote_runs, bus, config=config)
    root = Path(workspace.runtime.root)
    policies = LivePolicies(config)
    users = UserService(storage)
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
        MemoryService(storage, root, config)
        if snapshot.agent.personal.memory.is_enabled
        else None
    )
    files = ProjectFileStorage(root, config_provider=config)
    resources = ResourceService(storage, root, policies=policies, config=config)
    workspace_manager = EgisSandboxManager(
        root,
        sandbox_settings,
        client_factory=sandbox_client_factory,
        read_guard=workspace.read_guard,
    )
    projects = ProjectManager(
        storage,
        root,
        files=files,
        config_provider=config,
        workspaces=workspace_manager,
    )
    personal = resources.personal
    tasks = SessionService(
        storage, root, models, config, projects=projects, personal=personal
    )
    bus.coordinator = ExecutionCoordinator(
        storage,
        root,
        config,
        models=models,
        projects=projects,
        users=users,
        personal=personal,
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
    skill_imports = SkillImportService(storage, resources)
    services = AgentKitServices(
        users=users,
        agents=agents,
        personal=personal,
        teams=TeamDefinition(storage, policies, config_provider=config),
        grants=GrantService(storage),
        sharing=ShareService(storage),
        models=models,
        projects=projects,
        resources=resources,
        skill_imports=skill_imports,
        sessions=tasks,
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
        tools=BoundToolFactory(resources=resources, users=users),
        middlewares=partial(
            tasks.build_middlewares,
            users=users,
            file_changes=project_files,
            memory=memory,
            resources=resources,
            remote_agents=remote_agents,
            workspace_manager=workspace_manager,
        ),
        remote_agents=remote_agents,
        _storage=storage,
        _config=config,
        _owned_client=client if owns_client else None,
        _skill_imports=skill_imports,
        _memory=memory,
        _owns_engine=owns_engine,
    )
