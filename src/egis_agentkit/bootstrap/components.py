"""Application-wide AgentKit dependencies, independent of HTTP."""

from contextlib import AbstractAsyncContextManager, AsyncExitStack, asynccontextmanager
from dataclasses import dataclass
from pathlib import Path

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from egis_agentkit.config import NacosClient, NacosConfigProvider
from egis_agentkit.extensions.agent.factory import (
    BoundToolFactory,
    SessionMiddlewareFactory,
)
from egis_agentkit.extensions.compatibility.execution import (
    ExecutionCoordinator,
    ScopedMessageBus,
)
from egis_agentkit.extensions.compatibility.storage import ExecutionStorage
from egis_agentkit.extensions.hub import NacosMCPHub, NacosSkillHub
from egis_agentkit.extensions.package import PackageLoader
from egis_agentkit.extensions.workspace import EgisLocalWorkspaceManager
from egis_agentkit.integration.nacos import NacosHubManagement
from egis_agentkit.service.access import PermissionService, ShareService
from egis_agentkit.service.agent import AgentService
from egis_agentkit.service.agent.detail import DefinitionDetailService
from egis_agentkit.service.agent.package import PackageService
from egis_agentkit.service.model import ModelService
from egis_agentkit.service.project import ProjectService
from egis_agentkit.service.resource import ResourceService
from egis_agentkit.service.resource.imports import SkillImportService
from egis_agentkit.service.session import TaskService
from egis_agentkit.service.session.composer import ComposerService
from egis_agentkit.service.team import TeamService
from egis_agentkit.service.user import UserService
from egis_agentkit.service.user.memory import MemoryService
from egis_agentkit.service.user.preferences import PreferenceService
from egis_agentkit.service.workspace.changes import FileChangeService
from egis_agentkit.service.workspace.files import WorkspaceFiles
from egis_agentkit.service.workspace.preview import DocumentPreviewService
from egis_agentkit.service.workspace.service import WorkspaceService
from egis_agentkit.storage import AgentKitStorage

from .services import AgentKitServices


class _LivePolicies:
    """Read the latest validated Nacos policy at each service operation."""

    def __init__(self, provider: NacosConfigProvider):
        self.provider = provider

    def __getattr__(self, name):
        return getattr(self.provider.snapshot.policies, name)


@dataclass(frozen=True, slots=True)
class AgentKitComponents:
    services: AgentKitServices
    native_storage: ExecutionStorage
    bus: ScopedMessageBus
    workspace: EgisLocalWorkspaceManager
    skill_hub: NacosSkillHub
    mcp_hub: NacosMCPHub
    tools: BoundToolFactory
    middlewares: SessionMiddlewareFactory
    loader: PackageLoader
    _storage: AgentKitStorage
    _config: NacosConfigProvider
    _owned_client: NacosClient | None
    _skill_imports: SkillImportService
    _memory: MemoryService | None
    _owns_engine: bool

    async def check_database(self) -> None:
        async with self._storage.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))

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
            # Restore immutable packages before the native dispatcher drains wakeups.
            await self.services.packages.restore_loaded()
            await stack.enter_async_context(native_lifespan)
            stack.push_async_callback(self._skill_imports.close)
            await self._skill_imports.start()
            yield


def build_components(
    config: NacosConfigProvider,
    client: NacosClient,
    database_url: str,
    *,
    engine: AsyncEngine | None = None,
    owns_client: bool = True,
) -> AgentKitComponents:
    snapshot = config.snapshot
    settings = snapshot.runtime
    if settings.workspace_mode != "local":
        raise ValueError(
            "当前工作区目录/产物 Adapter 为 local；配置 sandbox 前需要接入 NAS 路径映射"
        )
    owns_engine = engine is None
    if engine is None:
        engine = create_async_engine(
            database_url,
            pool_pre_ping=True,
            pool_size=10,
            max_overflow=10,
            hide_parameters=True,
        )
    storage = AgentKitStorage(database_url, engine)
    native_storage = ExecutionStorage(database_url, engine)
    bus = ScopedMessageBus(
        deployment_mode=settings.deployment_mode,
        engine=engine,
        kafka_bootstrap_servers=settings.kafka_bootstrap_servers,
        topic_prefix=settings.kafka_topic_prefix,
    )
    root = Path(settings.workspace_root)
    loader = PackageLoader([Path(path) for path in settings.release_roots])
    packages = PackageService(storage, loader)
    policies = _LivePolicies(config)
    users = UserService(storage)
    agents = AgentService(storage, policies, packages=packages)
    permissions = PermissionService(storage, policies)
    models = ModelService(storage, config)
    preferences = PreferenceService(storage, root)
    file_changes = FileChangeService(storage, root)
    memory = (
        MemoryService(storage, root, config) if settings.memory.is_enabled else None
    )
    files = WorkspaceFiles(root, snapshot.policies.max_upload_bytes)
    # Project adoption takes the native session lock without binding a chat.
    projects = ProjectService(
        storage,
        root,
        bus,
        acquire_session_lock=super(ScopedMessageBus, bus).acquire_lock,
    )
    tasks = TaskService(storage, root, models, config, projects=projects)
    bus.coordinator = ExecutionCoordinator(
        storage,
        root,
        config,
        loader=loader,
        models=models,
        packages=packages,
        projects=projects,
        users=users,
    )

    async def authorize_hub(user_id, resource_type, resource_id, operation):
        return await resources.authorize_hub(
            user_id, resource_type, resource_id, operation
        )

    skill_hub = NacosSkillHub(client, authorize=authorize_hub)
    mcp_hub = NacosMCPHub(client, authorize=authorize_hub)
    resources = ResourceService(
        storage, skill_hub, mcp_hub, NacosHubManagement(client), root, policies=policies
    )
    workspace_service = WorkspaceService(
        files, projects, tasks, file_changes, DocumentPreviewService()
    )
    skill_imports = SkillImportService(storage, resources)
    services = AgentKitServices(
        users=users,
        agents=agents,
        teams=TeamService(storage, policies),
        permissions=permissions,
        sharing=ShareService(storage),
        models=models,
        preferences=preferences,
        projects=projects,
        packages=packages,
        resources=resources,
        skill_imports=skill_imports,
        tasks=tasks,
        workspace=workspace_service,
        definition_details=DefinitionDetailService(storage, agents, permissions),
        composer=ComposerService(storage, agents, resources, workspace_service),
    )
    return AgentKitComponents(
        services=services,
        native_storage=native_storage,
        bus=bus,
        workspace=EgisLocalWorkspaceManager(root, resources),
        skill_hub=skill_hub,
        mcp_hub=mcp_hub,
        tools=BoundToolFactory(),
        middlewares=SessionMiddlewareFactory(
            storage,
            users=users,
            preferences=preferences,
            file_changes=file_changes,
            memory=memory,
            resources=resources,
            config_provider=config,
        ),
        loader=loader,
        _storage=storage,
        _config=config,
        _owned_client=client if owns_client else None,
        _skill_imports=skill_imports,
        _memory=memory,
        _owns_engine=owns_engine,
    )
