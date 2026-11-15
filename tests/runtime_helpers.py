"""为轻量运行测试装配真实 Service 回调，替身只负责存储与外部依赖。"""

from functools import partial
from pathlib import Path

from egis_agentkit.extension._runtime_callback_middleware_impl import (
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
