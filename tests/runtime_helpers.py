"""为轻量运行测试装配真实 Service 回调，替身只负责存储与外部依赖。"""

from functools import partial
from pathlib import Path

from egis_agentkit.bootstrap._middlewares import build_middlewares
from egis_agentkit.middleware.session._session_callback_middleware_impl import (
    SessionCallbackMiddleware,
)
from egis_agentkit.middleware.workspace._workspace_read_guard_middleware_impl import (
    ReadToolGuardMiddleware,
)
from egis_agentkit.service.project._file_manager import ProjectFileManager
from egis_agentkit.service.project._project_manager import ProjectManager
from egis_agentkit.service.session._session import SessionService


def managed_files(changes, root):
    if isinstance(changes, ProjectFileManager):
        return changes
    manager = ProjectFileManager(None, root)
    for name in ("observe_safe", "track_changes", "finalize_reply"):
        if hasattr(changes, name):
            setattr(manager, name, getattr(changes, name))
    return manager


def file_middleware(changes, scope):
    files = managed_files(changes, ".")
    return SessionCallbackMiddleware(
        reply=partial(files.on_reply, scope), acting=partial(files.on_acting, scope)
    )


def project_prompt(scope):
    root = scope.workdir.parent
    files = ProjectFileManager(None, root)
    projects = ProjectManager(None, root)
    return SessionCallbackMiddleware(
        system_prompt=partial(projects.on_system_prompt, scope, files)
    )


def session_middlewares(storage, *, users, file_changes, root=Path("."), **kwargs):
    sessions = SessionService(storage, root)
    return partial(
        build_middlewares,
        sessions=sessions,
        users=users,
        file_changes=managed_files(file_changes, root),
        **kwargs,
    )


def tools_callback(resources=None, users=None):
    """直接绑定 Service 方法，测试与真实 App 使用相同的工具装配能力。"""
    return partial(
        SessionService(None, Path(".")).build_tools, resources=resources, users=users
    )


def stub_sandbox_provider(monkeypatch):
    """隔离远程 SDK 创建边界；核心绑定、原生工具和回复流程仍使用真实实现。"""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from agentscope.tool import LocalBackend

    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )

    async def initialize(workspace):
        workspace.workdir = str(workspace.egis_files.user_root(workspace.egis_user_id))
        Path(workspace.workdir).mkdir(parents=True, exist_ok=True)
        workspace._backend = LocalBackend()
        workspace.is_alive = True
        workspace.check_ready = AsyncMock()
        workspace.client._sandbox = SimpleNamespace(
            id="test-instance", renew=AsyncMock(), pause=AsyncMock(), close=AsyncMock()
        )

    monkeypatch.setattr(EgisSandboxWorkspace, "initialize", initialize)


def sandbox_manager(root, monkeypatch):
    """用真实 Manager/Core 验证工具、绑定和关闭链，不创建远程沙箱。"""
    from egis_agentkit.client.nacos._settings import OpenSandboxConfig
    from egis_agentkit.service.workspace._egis_sandbox_manager import EgisSandboxManager

    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")
    stub_sandbox_provider(monkeypatch)
    from egis_agentkit.client.sandbox._opensandbox import OpenSandboxClient

    settings = OpenSandboxConfig(
        domain="127.0.0.1:8090", api_key_env="TEST_SANDBOX_KEY"
    )
    return EgisSandboxManager(
        root,
        settings,
        client_factory=partial(OpenSandboxClient, settings, api_key="test-only"),
        read_guard_factory=ReadToolGuardMiddleware,
        **workspace_dependencies(),
    )


def workspace_dependencies():
    """轻量单节点测试明确使用原生内存总线，集群正确性由独立 PG 用例验证。"""
    import sys

    from egis_agentkit.client.nacos._settings import (
        WorkspaceLockConfig,
        WorkspaceProbeConfig,
    )
    from egis_agentkit.core.message_bus import EgisMessageBus

    return dict(
        bus=EgisMessageBus(deployment_mode="standalone"),
        probe=WorkspaceProbeConfig(
            storage_python=sys.executable,
            runtime_commands=[[sys.executable, "-c", "import sys"]],
        ),
        lock=WorkspaceLockConfig(),
    )
