"""验证观测接入不会改变回复流，日志可关联会话并保留异常语义。"""

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest
from agentscope.event import ReplyEndEvent
from agentscope.middleware import TracingMiddleware
from test_runtime_middleware_components import scope_at

from egis_agentkit.bootstrap._init import AgentKit
from egis_agentkit.bootstrap._middlewares import build_middlewares
from egis_agentkit.client._logging import LoggingClient
from egis_agentkit.client.nacos._settings import LoggingConfig
from egis_agentkit.core.session._context import session_execution_context
from egis_agentkit.service.session._session import SessionService


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_reply_logs_status_without_swallowing_stream_or_cancellation(
    tmp_path, capsys, cancel
):
    service = SessionService(None, tmp_path)
    workspace = SimpleNamespace(
        egis_user_id="alice",
        egis_project_id="project",
        egis_session_id="session",
        egis_agent_id="agent",
    )
    released = []

    @asynccontextmanager
    async def renew(workspace):
        try:
            yield
        finally:
            released.append(True)

    end = ReplyEndEvent(
        reply_id="reply", session_id="session", finished_reason="completed"
    )

    async def handler(*args):
        if cancel:
            raise asyncio.CancelledError()
        yield end

    logging = LoggingClient(LoggingConfig())
    logging.start()
    try:
        stream = service.on_workspace_reply(
            SimpleNamespace(renew_workspace=renew), workspace, handler, None, {}, None
        )
        if cancel:
            with pytest.raises(asyncio.CancelledError):
                await anext(stream)
        else:
            assert [item async for item in stream] == [end]
    finally:
        logging.close()
    assert released == [True]
    records = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [row["message"] for row in records] == ["reply.started", "reply.finished"]
    assert records[-1]["context"]["session_id"] == "session"
    assert records[-1]["context"]["reason"] == (
        "interrupted" if cancel else "completed"
    )
    assert records[-1]["context"]["duration_ms"] >= 0


@pytest.mark.asyncio
async def test_native_tracing_wraps_business_middlewares_only_when_enabled(tmp_path):
    scope = scope_at(tmp_path)
    token = session_execution_context.set(scope)
    try:
        services = dict(
            sessions=SessionService(None, tmp_path),
            users=SimpleNamespace(
                get=AsyncMock(return_value=SimpleNamespace(user_id=scope.user_id))
            ),
            file_changes=SimpleNamespace(on_reply=AsyncMock(), on_acting=AsyncMock()),
        )
        enabled = await build_middlewares(
            scope.user_id,
            scope.agent_id,
            scope.session_id,
            object(),
            tracing_enabled=True,
            **services,
        )
        disabled = await build_middlewares(
            scope.user_id, scope.agent_id, scope.session_id, object(), **services
        )
    finally:
        session_execution_context.reset(token)
    assert isinstance(enabled[0], TracingMiddleware)
    assert not any(isinstance(mw, TracingMiddleware) for mw in disabled)


@pytest.mark.asyncio
async def test_tracing_start_failure_still_releases_all_owned_clients():
    kit = SimpleNamespace(
        _logging=SimpleNamespace(start=Mock(), close=Mock()),
        _tracing=SimpleNamespace(
            start=Mock(side_effect=RuntimeError("provider invalid")), aclose=AsyncMock()
        ),
        _owns_engine=True,
        _storage=SimpleNamespace(engine=SimpleNamespace(dispose=AsyncMock())),
        _owned_client=SimpleNamespace(close=AsyncMock()),
        _config=SimpleNamespace(close=AsyncMock()),
        _memory=SimpleNamespace(close=AsyncMock()),
    )

    @asynccontextmanager
    async def native():
        pytest.fail("native app cannot start after tracing setup failed")
        yield

    with pytest.raises(RuntimeError, match="provider invalid"):
        async with AgentKit.lifespan(kit, native()):
            pytest.fail("startup should have failed")
    kit._storage.engine.dispose.assert_awaited_once()
    kit._owned_client.close.assert_awaited_once()
    kit._config.close.assert_awaited_once()
    kit._memory.close.assert_awaited_once()
    kit._tracing.aclose.assert_awaited_once()
    kit._logging.close.assert_called_once()
