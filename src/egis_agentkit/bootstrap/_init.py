"""组装 AgentKit 的业务服务和运行时扩展，管理组件启动和关闭顺序。"""

from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy.ext.asyncio import AsyncEngine

from egis_agentkit.backend._postgres import check_postgres, create_postgres_engine
from egis_agentkit.backend.nacos import NacosClient, NacosConfigProvider
from egis_agentkit.backend.nacos._config import LivePolicies
from egis_agentkit.extension._agent_tools_callback import BoundToolFactory
from egis_agentkit.extension._demo_report_tool_impl import deploy_demo_report_tools
from egis_agentkit.extension._resource_hubs_impl import LocalMCPHub, LocalSkillHub
from egis_agentkit.extension._session_lock_override import (
    ExecutionCoordinator,
    ScopedMessageBus,
)
from egis_agentkit.extension._session_middlewares_callback import (
    SessionMiddlewareFactory,
)
from egis_agentkit.extension._session_storage_override import ExecutionStorage
from egis_agentkit.extension._workspace_manager_impl import EgisLocalWorkspaceManager
from egis_agentkit.service._composer import ComposerService
from egis_agentkit.service._model import ModelService
from egis_agentkit.service._project import ProjectService
from egis_agentkit.service._session import SessionService
from egis_agentkit.service._user import UserService
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._expert_definition import ExpertDefinition
from egis_agentkit.service.agent._memory import MemoryService
from egis_agentkit.service.agent._personal_definition import PersonalDefinition
from egis_agentkit.service.agent._query import AgentQuery
from egis_agentkit.service.agent._remote import RemoteAgentService
from egis_agentkit.service.agent._remote_definition import RemoteDefinition
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill_import import SkillImportService
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.service.workspace._changes import FileChangeService
from egis_agentkit.service.workspace._files import WorkspaceService
from egis_agentkit.service.workspace._preview import OfficeToPdfConverter
from egis_agentkit.storage import AgentKitStorage
from egis_agentkit.storage._workspace import WorkspaceFiles

from ._services import AgentKitServices


@dataclass(frozen=True, slots=True)
class AgentKit:
    services: AgentKitServices
    native_storage: ExecutionStorage
    bus: ScopedMessageBus
    workspace: EgisLocalWorkspaceManager
    skill_hub: LocalSkillHub
    mcp_hub: LocalMCPHub
    tools: BoundToolFactory
    middlewares: SessionMiddlewareFactory
    remote_agents: RemoteAgentService
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
) -> AgentKit:
    snapshot = config.snapshot
    postgres = snapshot.backend.postgres
    workspace = snapshot.workspace
    if workspace.runtime.mode != "local":
        raise ValueError(
            "当前工作区目录/产物 Adapter 为 local；配置 sandbox 前需要接入 NAS 路径映射"
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
    bus = ScopedMessageBus(engine=engine, kafka=snapshot.backend.kafka)
    remote_agents = RemoteAgentService(storage.remote_runs, bus, config=config)
    root = Path(workspace.runtime.root)
    policies = LivePolicies(config)
    users = UserService(storage)
    experts = ExpertDefinition(storage, policies)
    agent_query = AgentQuery(storage, policies, config_provider=config)
    models = ModelService(storage, config)
    memory = (
        MemoryService(storage, root, config)
        if snapshot.agent.personal.memory.is_enabled
        else None
    )
    files = WorkspaceFiles(root, config_provider=config)
    file_changes = FileChangeService(storage, root, files=files, config_provider=config)
    projects = ProjectService(
        storage,
        root,
        files=files,
        config_provider=config,
    )
    tasks = SessionService(storage, root, models, config, projects=projects)
    bus.coordinator = ExecutionCoordinator(
        storage,
        root,
        config,
        models=models,
        projects=projects,
        users=users,
    )

    resources = ResourceService(storage, root, policies=policies, config=config)
    personal = PersonalDefinition(
        storage,
        policies,
        workspace_root=root,
        config=config,
        installations=resources.installations,
    )
    deploy_demo_report_tools(resources.tool_registry, files)
    workspace_service = WorkspaceService(
        files,
        projects,
        tasks,
        file_changes,
        OfficeToPdfConverter(config_provider=config),
    )
    skill_imports = SkillImportService(storage, resources)
    services = AgentKitServices(
        users=users,
        agent_query=agent_query,
        personal_definition=personal,
        expert_definition=experts,
        remote_definition=RemoteDefinition(storage, policies, config=config),
        teams=TeamDefinition(storage, policies, config_provider=config),
        grants=GrantService(storage),
        sharing=ShareService(storage),
        models=models,
        projects=projects,
        resources=resources,
        skill_imports=skill_imports,
        sessions=tasks,
        workspace=workspace_service,
        composer=ComposerService(
            storage, agent_query, resources, workspace_service, config_provider=config
        ),
    )
    return AgentKit(
        services=services,
        native_storage=native_storage,
        bus=bus,
        workspace=EgisLocalWorkspaceManager(root, resources),
        skill_hub=resources.skill_hub,
        mcp_hub=resources.mcp_hub,
        tools=BoundToolFactory(resources=resources, users=users),
        middlewares=SessionMiddlewareFactory(
            storage,
            users=users,
            personal=personal,
            file_changes=file_changes,
            memory=memory,
            resources=resources,
            remote_agents=remote_agents,
        ),
        remote_agents=remote_agents,
        _storage=storage,
        _config=config,
        _owned_client=client if owns_client else None,
        _skill_imports=skill_imports,
        _memory=memory,
        _owns_engine=owns_engine,
    )
