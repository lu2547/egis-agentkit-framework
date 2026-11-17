"""运行根覆盖锁和执行上下文，验证并发隔离与原生失败桥接。"""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from agentscope.app._bus_ops import publish_session_event
from agentscope.app.message_bus import MessageBusKeys
from agentscope.event import ReplyEndEvent, ReplyFinishedReason
from opentelemetry import trace
from test_tracing_client import isolated_provider as isolated_provider
from test_tracing_client import memory_export

from egis_agentkit.client._tracing import TracingClient, trace_phase
from egis_agentkit.client.nacos._settings import TracingConfig
from egis_agentkit.core.session._context import session_setup_error
from egis_agentkit.errors import Forbidden
from egis_agentkit.service.session._message_bus_override import SessionMessageBus
from egis_agentkit.service.session._session import SessionService

pytestmark = pytest.mark.usefixtures("isolated_provider")


@pytest.mark.asyncio
async def test_session_lock_run_trace_isolates_concurrent_tasks(monkeypatch):
    exporter = memory_export(monkeypatch)
    tracing = TracingClient(TracingConfig(enabled=True))
    tracing.start()
    bus = SessionMessageBus(tracing=tracing)
    await bus.__aenter__()

    @asynccontextmanager
    async def bind(session_id):
        with trace_phase("session.prepare", {"session.id": session_id}):
            pass
        yield

    bus.coordinator = SimpleNamespace(bind=bind)
    ready = [asyncio.Event(), asyncio.Event()]

    async def execute(index):
        sid = str(index)
        async with bus.acquire_lock(MessageBusKeys.session_lock(sid)):
            ready[index].set()
            await ready[1 - index].wait()
            with trace_phase("runtime.body", {"session.id": sid}):
                await asyncio.sleep(0)
        assert not trace.get_current_span().is_recording()

    try:
        await asyncio.gather(execute(0), execute(1))
    finally:
        await bus.__aexit__(None, None, None)
        await tracing.aclose()
    spans = exporter.get_finished_spans()
    roots = [s for s in spans if s.name == "session.operation"]
    assert len(roots) == 2
    assert len({s.context.trace_id for s in roots}) == 2
    for root in roots:
        children = [
            s for s in spans if s.parent and s.parent.span_id == root.context.span_id
        ]
        assert {s.name for s in children} == {
            "session.lock.wait",
            "session.prepare",
            "runtime.body",
        }
        assert all(
            s.attributes.get("session.id", root.attributes["session.id"])
            == root.attributes["session.id"]
            for s in children
        )


@pytest.mark.asyncio
async def test_failed_prepare_preserves_native_error_bridge_and_records_error(
    monkeypatch,
):
    exporter = memory_export(monkeypatch)
    tracing = TracingClient(TracingConfig(enabled=True))
    tracing.start()
    bus = SessionMessageBus(tracing=tracing)
    await bus.__aenter__()

    @asynccontextmanager
    async def bind(session_id):
        raise Forbidden("sensitive response must not enter tracing")
        yield

    bus.coordinator = SimpleNamespace(bind=bind)
    try:
        async with bus.acquire_lock(MessageBusKeys.session_lock("failed")):
            assert isinstance(session_setup_error.get(), Forbidden)
        assert session_setup_error.get() is None
        assert not await bus.is_locked(MessageBusKeys.session_lock("failed"))
    finally:
        await bus.__aexit__(None, None, None)
        await tracing.aclose()
    run = next(
        s for s in exporter.get_finished_spans() if s.name == "session.operation"
    )
    assert run.status.status_code.name == "ERROR"
    assert run.attributes["error.type"] == "Forbidden"
    assert not run.events


@pytest.mark.asyncio
async def test_cancelled_runtime_releases_lock_and_trace_context(monkeypatch):
    exporter = memory_export(monkeypatch)
    tracing = TracingClient(TracingConfig(enabled=True))
    tracing.start()
    bus = SessionMessageBus(tracing=tracing)
    await bus.__aenter__()

    @asynccontextmanager
    async def bind(session_id):
        yield

    bus.coordinator = SimpleNamespace(bind=bind)
    try:
        with pytest.raises(asyncio.CancelledError):
            async with bus.acquire_lock(MessageBusKeys.session_lock("cancelled")):
                raise asyncio.CancelledError()
        assert not await bus.is_locked(MessageBusKeys.session_lock("cancelled"))
        assert not trace.get_current_span().is_recording()
        with trace_phase("outside"):
            pass
    finally:
        await bus.__aexit__(None, None, None)
        await tracing.aclose()
    spans = exporter.get_finished_spans()
    assert "outside" not in {s.name for s in spans}
    root = next(s for s in spans if s.name == "session.operation")
    assert root.attributes["error.type"] == "CancelledError"


@pytest.mark.asyncio
async def test_native_published_setup_failure_marks_root_and_keeps_replay(monkeypatch):
    exporter = memory_export(monkeypatch)
    tracing = TracingClient(TracingConfig(enabled=True))
    tracing.start()
    bus = SessionMessageBus(tracing=tracing)
    await bus.__aenter__()

    @asynccontextmanager
    async def bind(session_id):
        yield

    bus.coordinator = SimpleNamespace(bind=bind)
    event = ReplyEndEvent(
        session_id="failed", reply_id="reply", finished_reason=ReplyFinishedReason.ERROR
    )
    try:
        async with bus.acquire_lock(MessageBusKeys.session_lock("failed")):
            await publish_session_event(bus, "failed", event.model_dump(mode="json"))
        replay = await bus.log_read(MessageBusKeys.session_events("failed"))
        assert replay[0][1]["finished_reason"] == "error"
    finally:
        await bus.__aexit__(None, None, None)
        await tracing.aclose()
    root = next(
        s for s in exporter.get_finished_spans() if s.name == "session.operation"
    )
    assert root.status.status_code.name == "ERROR"


@pytest.mark.asyncio
async def test_native_caught_reply_exception_does_not_leave_runtime_success(
    monkeypatch, tmp_path
):
    exporter = memory_export(monkeypatch)
    tracing = TracingClient(TracingConfig(enabled=True))
    tracing.start()
    workspace = SimpleNamespace(
        egis_user_id="u", egis_project_id="p", egis_session_id="s", egis_agent_id="a"
    )

    @asynccontextmanager
    async def renew(workspace):
        yield

    async def handler(*args):
        raise RuntimeError("private-upstream-error")
        yield

    try:
        with tracing.span("chat.run", root=True):
            # 原生 ChatService 在会话锁内捕获此异常，并通过失败事件向用户报告。
            with pytest.raises(RuntimeError):
                async for _ in SessionService(None, tmp_path).on_workspace_reply(
                    SimpleNamespace(renew_workspace=renew),
                    workspace,
                    handler,
                    None,
                    {},
                    None,
                ):
                    pass
    finally:
        await tracing.aclose()
    root = exporter.get_finished_spans()[0]
    assert root.status.status_code.name == "ERROR"
    assert root.attributes["error.type"] == "RuntimeError"
    assert not root.events


@pytest.mark.asyncio
async def test_other_session_failure_event_does_not_mark_current_runtime(monkeypatch):
    exporter = memory_export(monkeypatch)
    tracing = TracingClient(TracingConfig(enabled=True))
    tracing.start()
    bus = SessionMessageBus(tracing=tracing)
    await bus.__aenter__()
    try:
        with tracing.span("chat.run", root=True, attributes={"session.id": "leader"}):
            event = ReplyEndEvent(
                session_id="worker",
                reply_id="r",
                finished_reason=ReplyFinishedReason.ERROR,
            )
            await publish_session_event(bus, "worker", event.model_dump(mode="json"))
    finally:
        await bus.__aexit__(None, None, None)
        await tracing.aclose()
    assert exporter.get_finished_spans()[0].status.status_code.name == "OK"
