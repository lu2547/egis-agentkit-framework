"""工作区仅负责执行环境，会话在原生工具集收集前装载受权资源。"""

from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from egis_agentkit.bootstrap._middlewares import build_middlewares
from egis_agentkit.client.nacos._settings import WorkspaceRuntimeConfig
from egis_agentkit.core.session._context import session_execution_context
from egis_agentkit.service.session._session import SessionService


def test_workspace_has_one_sandbox_implementation_and_no_local_branch(tmp_path):
    root = Path(__file__).parents[1] / "src/egis_agentkit"
    assert {
        path.name
        for path in (root / "service/workspace").glob("*.py")
        if path.name != "__init__.py"
    } == {
        "_egis_sandbox_manager.py",
    }
    assert {
        path.name
        for path in (root / "core/workspace").glob("*.py")
        if path.name != "__init__.py"
    } == {
        "_identity.py",
        "_sandbox_backend.py",
        "_egis_sandbox_workspace.py",
    }
    with pytest.raises(ValidationError):
        WorkspaceRuntimeConfig(root=str(tmp_path), mode="local")


@pytest.mark.asyncio
async def test_session_prepares_authorized_workspace_before_returning_toolkit_inputs(
    tmp_path,
):
    from test_runtime_middleware_components import scope_at

    scope = scope_at(tmp_path)
    user = SimpleNamespace(user_id=scope.user_id)
    workspace = object()
    resources = SimpleNamespace(prepare_workspace=AsyncMock(), tool_registry=None)
    sessions = SessionService(None, tmp_path)
    token = session_execution_context.set(scope)
    try:
        await build_middlewares(
            scope.user_id,
            scope.agent_id,
            scope.session_id,
            workspace,
            sessions=sessions,
            users=SimpleNamespace(get=AsyncMock(return_value=user)),
            file_changes=SimpleNamespace(on_reply=AsyncMock(), on_acting=AsyncMock()),
            resources=resources,
        )
    finally:
        session_execution_context.reset(token)
    resources.prepare_workspace.assert_awaited_once_with(user, scope, workspace)
