"""验证原生 span 的真实 OTLP 传输、采样和独立资源生命周期。"""

import asyncio
import gc
import logging
import subprocess
import sys
import threading
import weakref
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from textwrap import dedent
from time import monotonic

import pytest
from agentscope.agent import Agent
from agentscope.formatter import OpenAIChatFormatter
from agentscope.message import TextBlock, ToolCallBlock, UserMsg
from agentscope.middleware import TracingMiddleware
from agentscope.model import ChatModelBase, ChatResponse
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import ToolBase, Toolkit
from opentelemetry import trace
from opentelemetry.proto.collector.trace.v1.trace_service_pb2 import (
    ExportTraceServiceRequest,
)
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import SimpleSpanProcessor
from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
from opentelemetry.sdk.trace.sampling import ALWAYS_OFF
from opentelemetry.util._once import Once

from egis_agentkit.client import _tracing
from egis_agentkit.client._tracing import TracingClient
from egis_agentkit.client.nacos._settings import TracingConfig


async def test_runtime_root_links_request_and_keeps_native_model_tool_tree(monkeypatch):
    exporter = memory_export(monkeypatch)
    client = TracingClient(TracingConfig(enabled=True))
    client.start()
    tracer = trace.get_tracer("http-test")
    try:
        with tracer.start_as_current_span(
            "POST /chat/", attributes={"http.route": "/chat/"}
        ) as request:
            request_id = request.get_span_context().trace_id
            with client.span(
                "session.operation", root=True, attributes={"session.id": "session-one"}
            ) as run:
                with _tracing.trace_phase("session.prepare"):
                    pass
                agent = Agent(
                    name="runtime-agent",
                    system_prompt="test",
                    model=StreamModel(),
                    toolkit=Toolkit(tools=[Weather()]),
                    middlewares=[TracingMiddleware()],
                )
                async for _ in agent.reply_stream(
                    UserMsg(name="user", content="上海天气")
                ):
                    pass
                run_id = run.get_span_context().trace_id
    finally:
        await client.aclose()
    spans = exporter.get_finished_spans()
    run = next(s for s in spans if s.name == "session.operation")
    assert run.parent is None and run.context.trace_id != request_id
    assert run.links[0].context.trace_id == request_id
    phase = next(s for s in spans if s.name == "session.prepare")
    reply = next(s for s in spans if s.name == "invoke_agent runtime-agent")
    assert phase.parent.span_id == reply.parent.span_id == run.context.span_id
    assert all(s.context.trace_id == run_id for s in spans if s.name != "POST /chat/")
    http = next(s for s in spans if s.name == "POST /chat/")
    assert (
        http.resource.attributes["openinference.project.name"] == "egis-agentkit-http"
    )
    assert reply.resource.attributes["openinference.project.name"] == "egis-agentkit"


async def test_runtime_phase_failure_is_safe_and_context_is_released(monkeypatch):
    exporter = memory_export(monkeypatch)
    client = TracingClient(TracingConfig(enabled=True))
    client.start()
    try:
        with client.span("session.operation", root=True):
            with pytest.raises(ValueError):
                with _tracing.trace_phase("resources.prepare"):
                    raise ValueError("must-not-export-this-password")
        with _tracing.trace_phase("outside-run"):
            pass
    finally:
        await client.aclose()
    spans = exporter.get_finished_spans()
    assert {s.name for s in spans} == {"session.operation", "resources.prepare"}
    assert all(s.status.status_code.name == "ERROR" for s in spans)
    assert all(not s.events for s in spans)
    assert "must-not-export-this-password" not in repr(
        [dict(s.attributes) for s in spans]
    )


async def test_disabled_runtime_does_not_create_spans(monkeypatch):
    exporter = memory_export(monkeypatch)
    client = TracingClient(TracingConfig(enabled=False))
    client.start()
    with client.span("session.operation", root=True):
        with _tracing.trace_phase("session.prepare"):
            pass
    await client.aclose()
    assert not exporter.get_finished_spans()


async def test_http_project_can_be_disabled_without_dropping_runtime(monkeypatch):
    exporter = memory_export(monkeypatch)
    client = TracingClient(TracingConfig(enabled=True, http_project_name=None))
    client.start()
    try:
        with trace.get_tracer("http-test").start_as_current_span("fastapi.endpoint"):
            pass
        with client.span("chat.run", root=True):
            pass
    finally:
        await client.aclose()
    assert [span.name for span in exporter.get_finished_spans()] == ["chat.run"]


async def test_standard_asgi_child_spans_follow_http_project(monkeypatch):
    exporter = memory_export(monkeypatch)
    client = TracingClient(TracingConfig(enabled=True))
    client.start()
    try:
        tracer = trace.get_tracer("opentelemetry.instrumentation.asgi")
        with tracer.start_as_current_span("GET /sessions http send"):
            pass
    finally:
        await client.aclose()
    assert (
        exporter.get_finished_spans()[0].resource.attributes[
            "openinference.project.name"
        ]
        == "egis-agentkit-http"
    )


async def test_memory_job_has_its_own_phase_and_does_not_fail_successful_reply(
    monkeypatch, tmp_path,
):
    from agentscope.middleware import ReMeMiddleware

    from egis_agentkit.middleware.agent._project_memory_middleware_override import (
        ProjectMemoryMiddlewareOverride,
    )

    async def job(self, name, **kwargs):
        if name == "auto_memory":
            raise RuntimeError("private-memory-response")
        return kwargs["query"]

    monkeypatch.setattr(ReMeMiddleware, "_run_job", job)
    exporter = memory_export(monkeypatch)
    client = TracingClient(TracingConfig(enabled=True))
    client.start()
    memory = ProjectMemoryMiddlewareOverride(workspace_dir=str(tmp_path))
    try:
        with client.span("chat.run", root=True):
            assert await memory._run_job("search", query="test query") == "test query"
            with pytest.raises(RuntimeError):
                await memory._run_job("auto_memory")
    finally:
        await client.aclose()
    spans = {span.name: span for span in exporter.get_finished_spans()}
    assert set(spans) == {"chat.run", "memory.search", "memory.auto_memory"}
    assert spans["chat.run"].status.status_code.name == "OK"
    assert spans["memory.auto_memory"].status.status_code.name == "ERROR"
    assert not spans["memory.auto_memory"].events


@pytest.fixture(autouse=True)
def isolated_provider(monkeypatch):
    # 测试隔离才重置 SDK 全局状态，生产代码只能使用公开注册接口。
    monkeypatch.setattr(trace, "_TRACER_PROVIDER", None)
    monkeypatch.setattr(trace, "_TRACER_PROVIDER_SET_ONCE", Once())
    yield
    provider = trace.get_tracer_provider()
    if isinstance(provider, TracerProvider):
        provider.shutdown()


@pytest.fixture
def client_type():
    return TracingClient


@pytest.fixture
def collector():
    requests = []

    class Handler(BaseHTTPRequestHandler):
        def do_POST(self):
            body = self.rfile.read(int(self.headers["Content-Length"]))
            requests.append(
                (self.path, {k.lower(): v for k, v in self.headers.items()}, body)
            )
            self.send_response(200)
            self.send_header("Content-Type", "application/x-protobuf")
            self.end_headers()

        def log_message(self, *_):
            pass

    server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/v1/traces", requests
    finally:
        server.shutdown()
        server.server_close()
        thread.join()


def memory_export(monkeypatch):
    exporter = InMemorySpanExporter()
    monkeypatch.setattr(
        "egis_agentkit.client._tracing.OTLPSpanExporter", lambda **_: exporter
    )
    return exporter


class StreamModel(ChatModelBase):
    formatter = OpenAIChatFormatter()

    def __init__(self):
        super().__init__(None, "tracing-test-model", self.Parameters(), stream=True)
        self.calls = 0

    async def __call__(self, messages, tools=None, tool_choice=None):
        self.calls += 1
        if self.calls == 1:
            response = ChatResponse(
                is_last=True,
                content=[
                    ToolCallBlock(id="call-1", name="Weather", input='{"city":"上海"}')
                ],
            )
        else:
            response = ChatResponse(is_last=True, content=[TextBlock(text="上海晴天")])

        async def stream():
            yield response

        return stream()


class Weather(ToolBase):
    name = "Weather"
    description = "查询天气"
    input_schema = {
        "type": "object",
        "properties": {"city": {"type": "string"}},
        "required": ["city"],
    }
    is_read_only = True
    is_concurrency_safe = True
    is_external_tool = False
    is_mcp = False

    async def check_permissions(self, tool_input, context):
        return PermissionDecision(behavior=PermissionBehavior.ALLOW, message="test")

    async def execute(self, city: str):
        return city + "晴天"


async def test_disabled_client_does_not_initialize_provider_or_transport(client_type):
    original = trace.get_tracer_provider()
    client = client_type(TracingConfig())
    client.start()
    await client.aclose()
    assert trace.get_tracer_provider() is original
    assert not trace.get_tracer("disabled").start_span("disabled").is_recording()


async def test_native_stream_reply_model_and_tool_reach_http_collector(
    client_type, collector
):
    endpoint, requests = collector
    client = client_type(
        TracingConfig(
            enabled=True,
            endpoint=endpoint,
            project_name="native-test-project",
            service_name="native-test-service",
            headers={"Authorization": "Bearer test-only"},
            max_queue_size=11,
            max_export_batch_size=2,
            schedule_delay_milliseconds=60_000,
        )
    )
    client.start()
    client.start()
    agent = Agent(
        name="traced-agent",
        system_prompt="天气测试助手",
        model=StreamModel(),
        toolkit=Toolkit(tools=[Weather()]),
        middlewares=[TracingMiddleware()],
    )
    try:
        async for _ in agent.reply_stream(UserMsg(name="user", content="上海天气")):
            assert not trace.get_current_span().is_recording()
    finally:
        await client.aclose()
    assert requests, "close must flush pending spans before the long batch delay"
    spans = []
    for path, headers, body in requests:
        assert path == "/v1/traces"
        assert headers["authorization"] == "Bearer test-only"
        assert headers["content-type"] == "application/x-protobuf"
        payload = ExportTraceServiceRequest.FromString(body)
        assert (
            sum(
                len(scope.spans)
                for resource_spans in payload.resource_spans
                for scope in resource_spans.scope_spans
            )
            <= 2
        )
        for resource_spans in payload.resource_spans:
            attrs = {
                item.key: item.value.string_value
                for item in resource_spans.resource.attributes
            }
            assert attrs["service.name"] == "native-test-service"
            assert attrs["openinference.project.name"] == "native-test-project"
            for scope in resource_spans.scope_spans:
                assert scope.scope.name == "agentscope"
                spans.extend(scope.spans)
    assert sorted(span.name for span in spans) == [
        "chat tracing-test-model",
        "chat tracing-test-model",
        "execute_tool Weather",
        "invoke_agent traced-agent",
    ]
    reply = next(span for span in spans if span.name.startswith("invoke_agent"))
    assert not reply.parent_span_id
    assert all(span.trace_id == reply.trace_id for span in spans)
    assert all(
        span.parent_span_id == reply.span_id for span in spans if span is not reply
    )
    assert all(span.status.code == 1 for span in spans)


async def test_host_provider_resources_and_exporter_remain_usable_after_close(
    client_type, monkeypatch
):
    host_exporter = InMemorySpanExporter()
    host = TracerProvider(resource=Resource.create({"service.name": "host-service"}))
    host.add_span_processor(SimpleSpanProcessor(host_exporter))
    trace.set_tracer_provider(host)
    own_exporter = memory_export(monkeypatch)
    client = client_type(TracingConfig(enabled=True, project_name="own-project"))
    client.start()
    with trace.get_tracer("host").start_as_current_span("before-close"):
        pass
    await client.aclose()
    await client.aclose()
    with trace.get_tracer("host").start_as_current_span("after-close"):
        pass
    assert trace.get_tracer_provider() is host
    assert [span.name for span in host_exporter.get_finished_spans()] == [
        "before-close",
        "after-close",
    ]
    own = own_exporter.get_finished_spans()
    assert [span.name for span in own] == ["before-close"]
    assert own[0].resource.attributes["openinference.project.name"] == "own-project"
    assert own[0].resource.attributes["service.name"] == "egis-agentkit"
    assert host.resource.attributes["service.name"] == "host-service"
    assert "openinference.project.name" not in host.resource.attributes


@pytest.mark.parametrize("reuse_client", [False, True])
async def test_restart_exports_once_with_a_fresh_owned_exporter(
    client_type, monkeypatch, reuse_client
):
    settings = TracingConfig(enabled=True, schedule_delay_milliseconds=60_000)
    client = client_type(settings)
    first = memory_export(monkeypatch)
    client.start()
    provider = trace.get_tracer_provider()
    tracer = trace.get_tracer("repeat")
    with tracer.start_as_current_span("first"):
        pass
    await client.aclose()
    second = memory_export(monkeypatch)
    if not reuse_client:
        client = client_type(settings)
    client.start()
    with tracer.start_as_current_span("second"):
        pass
    await client.aclose()
    assert trace.get_tracer_provider() is provider
    assert [span.name for span in first.get_finished_spans()] == ["first"]
    assert [span.name for span in second.get_finished_spans()] == ["second"]


@pytest.mark.parametrize("reuse_client", [False, True])
async def test_many_lifecycles_register_once_and_release_closed_subscribers(
    client_type, monkeypatch, reuse_client
):
    from egis_agentkit.client import _tracing

    subscribers = []

    class TrackedProcessor(_tracing._ExportProcessor):
        def __init__(self, *args):
            super().__init__(*args)
            subscribers.append(weakref.ref(self))

    class HostProvider(TracerProvider):
        def __init__(self):
            super().__init__()
            self.registrations = []

        def add_span_processor(self, processor):
            self.registrations.append(processor)
            super().add_span_processor(processor)

    monkeypatch.setattr(_tracing, "_ExportProcessor", TrackedProcessor)
    host = HostProvider()
    trace.set_tracer_provider(host)
    settings = TracingConfig(enabled=True, schedule_delay_milliseconds=60_000)
    client = client_type(settings)
    tracer = trace.get_tracer("many-lifecycles")
    for index in range(20):
        exporter = memory_export(monkeypatch)
        if not reuse_client:
            client = client_type(settings)
        client.start()
        with tracer.start_as_current_span(f"reply-{index}"):
            pass
        await client.aclose()
        assert [span.name for span in exporter.get_finished_spans()] == [
            f"reply-{index}"
        ]
    assert len(host.registrations) == 1
    gc.collect()
    assert len(subscribers) == 20
    assert all(reference() is None for reference in subscribers)


async def test_sampling_can_change_after_restart_without_replacing_global_provider(
    client_type, monkeypatch
):
    first = memory_export(monkeypatch)
    client = client_type(TracingConfig(enabled=True, sample_ratio=0.0))
    client.start()
    tracer = trace.get_tracer("sampling")
    with tracer.start_as_current_span("dropped"):
        pass
    await client.aclose()
    second = memory_export(monkeypatch)
    client = client_type(TracingConfig(enabled=True, sample_ratio=1.0))
    client.start()
    with tracer.start_as_current_span("recorded"):
        pass
    await client.aclose()
    assert first.get_finished_spans() == ()
    assert [span.name for span in second.get_finished_spans()] == ["recorded"]


async def test_host_sampler_is_not_replaced(client_type, monkeypatch):
    host = TracerProvider(sampler=ALWAYS_OFF)
    trace.set_tracer_provider(host)
    own = memory_export(monkeypatch)
    client = client_type(TracingConfig(enabled=True))
    client.start()
    with trace.get_tracer("host").start_as_current_span("host-dropped"):
        pass
    await client.aclose()
    assert host.sampler is ALWAYS_OFF
    assert own.get_finished_spans() == ()


async def test_lifecycle_logs_only_safe_tracing_context(
    client_type, monkeypatch, caplog
):
    memory_export(monkeypatch)
    client = client_type(
        TracingConfig(
            enabled=True,
            project_name="safe-project",
            sample_ratio=0.25,
            headers={"Authorization": "never-log-this-token"},
        )
    )
    with caplog.at_level(logging.INFO, logger="egis_agentkit.client._tracing"):
        client.start()
        client.start()
        await client.aclose()
        await client.aclose()
    records = [record for record in caplog.records if record.name.endswith("._tracing")]
    assert [record.message for record in records] == [
        "tracing.started",
        "tracing.stopped",
    ]
    assert all(record.context["project_name"] == "safe-project" for record in records)
    assert all(record.context["sample_ratio"] == 0.25 for record in records)
    assert "never-log-this-token" not in caplog.text
    assert "http://" not in caplog.text


async def test_concurrent_start_owns_only_one_exporter(client_type, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    exporters = []

    def create_exporter(**_):
        exporter = InMemorySpanExporter()
        exporters.append(exporter)
        entered.set()
        release.wait(1)
        return exporter

    monkeypatch.setattr(
        "egis_agentkit.client._tracing.OTLPSpanExporter", create_exporter
    )
    client = client_type(TracingConfig(enabled=True))
    first = asyncio.create_task(asyncio.to_thread(client.start))
    assert await asyncio.to_thread(entered.wait, 1)
    second = asyncio.create_task(asyncio.to_thread(client.start))
    try:
        await asyncio.sleep(0.02)
    finally:
        release.set()
    await asyncio.gather(first, second)
    await client.aclose()
    assert len(exporters) == 1


async def test_close_deadline_does_not_block_the_event_loop(client_type, monkeypatch):
    entered = threading.Event()
    release = threading.Event()
    stopped = threading.Event()

    class SlowExporter(InMemorySpanExporter):
        def export(self, spans):
            entered.set()
            release.wait(2)
            return super().export(spans)

        def shutdown(self):
            super().shutdown()
            stopped.set()

    exporter = SlowExporter()
    monkeypatch.setattr(
        "egis_agentkit.client._tracing.OTLPSpanExporter", lambda **_: exporter
    )
    client = client_type(
        TracingConfig(
            enabled=True,
            shutdown_timeout_milliseconds=30,
            schedule_delay_milliseconds=60_000,
        )
    )
    client.start()
    with trace.get_tracer("slow").start_as_current_span("queued"):
        pass
    start = monotonic()
    try:
        await client.aclose()
        assert entered.is_set()
        assert monotonic() - start < 0.5
    finally:
        release.set()
    assert await asyncio.to_thread(stopped.wait, 1)


async def test_close_removes_only_owned_subscriber_during_concurrent_span_end(
    client_type, monkeypatch
):
    from egis_agentkit.client import _tracing

    entered = threading.Event()
    release = threading.Event()
    stopped = threading.Event()

    class OwnedExporter(InMemorySpanExporter):
        def shutdown(self):
            super().shutdown()
            stopped.set()

    first_exporter = OwnedExporter()
    monkeypatch.setattr(_tracing, "OTLPSpanExporter", lambda **_: first_exporter)
    first = client_type(TracingConfig(enabled=True))
    first.start()
    second_exporter = memory_export(monkeypatch)
    second = client_type(TracingConfig(enabled=True))
    second.start()
    original_on_end = _tracing.BatchSpanProcessor.on_end

    def delayed_on_end(batch, span):
        if batch.span_exporter is first_exporter:
            entered.set()
            release.wait(2)
        original_on_end(batch, span)

    monkeypatch.setattr(_tracing.BatchSpanProcessor, "on_end", delayed_on_end)
    span = trace.get_tracer("race").start_span("concurrent-end")
    ending = asyncio.create_task(asyncio.to_thread(span.end))
    try:
        assert await asyncio.to_thread(entered.wait, 1)
        await first.aclose()
        assert stopped.is_set()
    finally:
        release.set()
        await ending
        await second.aclose()
    assert first_exporter.get_finished_spans() == ()
    assert [span.name for span in second_exporter.get_finished_spans()] == [
        "concurrent-end"
    ]


def test_cleanup_completion_is_safe_after_awaiting_event_loop_has_closed(
    client_type, monkeypatch, caplog
):
    release = threading.Event()
    stopped = threading.Event()

    class PausedExporter(InMemorySpanExporter):
        def export(self, spans):
            release.wait(2)
            return super().export(spans)

        def shutdown(self):
            super().shutdown()
            stopped.set()

    exporter = PausedExporter()
    monkeypatch.setattr(
        "egis_agentkit.client._tracing.OTLPSpanExporter", lambda **_: exporter
    )
    client = client_type(TracingConfig(enabled=True, shutdown_timeout_milliseconds=25))
    client.start()
    with trace.get_tracer("closed-loop").start_as_current_span("queued"):
        pass
    previous_threads = set(threading.enumerate())
    loop = asyncio.new_event_loop()
    try:
        loop.run_until_complete(client.aclose())
        worker = next(
            thread
            for thread in threading.enumerate()
            if thread.name == "egis-tracing-cleanup" and thread not in previous_threads
        )
    finally:
        loop.close()
        release.set()
    worker.join(timeout=1)
    assert not worker.is_alive()
    assert stopped.is_set()
    assert not any(record.exc_info for record in caplog.records)


def test_shutdown_deadline_also_bounds_asyncio_run_process_exit():
    program = dedent("""
        import asyncio
        from time import monotonic, sleep
        from opentelemetry import trace
        from opentelemetry.sdk.trace.export.in_memory_span_exporter import InMemorySpanExporter
        from egis_agentkit.client import _tracing
        from egis_agentkit.client.nacos._settings import TracingConfig

        class SlowExporter(InMemorySpanExporter):
            def export(self, spans):
                sleep(0.7)
                return super().export(spans)

        _tracing.OTLPSpanExporter = lambda **_: SlowExporter()

        async def main():
            client = _tracing.TracingClient(TracingConfig(
                enabled=True, shutdown_timeout_milliseconds=25,
                schedule_delay_milliseconds=60_000,
            ))
            client.start()
            with trace.get_tracer("subprocess").start_as_current_span("queued"):
                pass
            await client.aclose()

        print("ready", flush=True)
        started = monotonic()
        asyncio.run(main())
        print(monotonic() - started, flush=True)
    """)
    process = subprocess.Popen(
        [sys.executable, "-c", program],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True,
    )
    try:
        assert process.stdout.readline().strip() == "ready"
        started = monotonic()
        output, errors = process.communicate(timeout=2)
        assert process.returncode == 0, errors
        assert float(output.strip()) < 0.3
        assert monotonic() - started < 0.5
    finally:
        if process.poll() is None:
            process.kill()
            process.communicate()


def test_explicit_non_sdk_provider_is_preserved(client_type):
    host = trace.NoOpTracerProvider()
    trace.set_tracer_provider(host)
    client = client_type(TracingConfig(enabled=True))
    with pytest.raises(RuntimeError, match="SDK TracerProvider"):
        client.start()
    assert trace.get_tracer_provider() is host
