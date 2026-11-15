"""为轻量运行测试装配真实 Service 回调，替身只负责存储与外部依赖。"""

from functools import partial
from pathlib import Path

from egis_agentkit.extension.runtime._runtime_callback_middleware_impl import (
    RuntimeCallbackMiddleware,
)
from egis_agentkit.service._session import SessionService
from egis_agentkit.service.project._file_manager import ProjectFileManager
from egis_agentkit.service.project._project_manager import ProjectManager


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
    return RuntimeCallbackMiddleware(
        reply=partial(files.on_reply, scope), acting=partial(files.on_acting, scope)
    )


def project_prompt(scope):
    root = scope.workdir.parent
    files = ProjectFileManager(None, root)
    projects = ProjectManager(None, root)
    return RuntimeCallbackMiddleware(
        system_prompt=partial(projects.on_system_prompt, scope, files)
    )


def session_middlewares(storage, *, users, file_changes, root=Path("."), **kwargs):
    sessions = SessionService(storage, root)
    return partial(
        sessions.build_middlewares,
        users=users,
        file_changes=managed_files(file_changes, root),
        **kwargs,
    )


def stub_sandbox_provider(monkeypatch):
    """隔离远程 SDK 创建边界；核心绑定、原生工具和回复流程仍使用真实实现。"""
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from agentscope.tool import LocalBackend
    from agentscope.workspace import OpenSandboxWorkspace

    async def initialize(workspace):
        workspace.workdir = str(workspace.egis_files.user_root(workspace.egis_user_id))
        Path(workspace.workdir).mkdir(parents=True, exist_ok=True)
        workspace._backend = LocalBackend()
        workspace.is_alive = True
        workspace._sandbox = SimpleNamespace(
            renew=AsyncMock(), pause=AsyncMock(), close=AsyncMock()
        )

    monkeypatch.setattr(OpenSandboxWorkspace, "initialize", initialize)


def sandbox_manager(root, monkeypatch):
    """用真实 Manager/Core 验证工具、绑定和关闭链，不创建远程沙箱。"""
    from egis_agentkit.client.nacos._settings import OpenSandboxConfig
    from egis_agentkit.service.workspace._egis_sandbox_manager import EgisSandboxManager

    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")
    stub_sandbox_provider(monkeypatch)
    return EgisSandboxManager(
        root,
        OpenSandboxConfig(
            domain="127.0.0.1:8090", api_key_env="TEST_SANDBOX_KEY", host_root=str(root)
        ),
    )
