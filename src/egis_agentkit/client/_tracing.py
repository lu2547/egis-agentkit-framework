"""为原生 AgentScope span 提供 OTLP HTTP 导出和独立关闭能力。

核心能力
- 原生 TracingMiddleware 负责创建 span，本模块只配置导出、采样和批处理。
- 复用已有 SDK provider，只关闭本客户端创建的 processor 与 exporter。
- 导出副本携带 Phoenix 项目资源，不修改宿主 provider 或其他导出器的数据。
- span / trace_phase 为真实运行和阶段提供安全上下文，运行根以 Link 关联触发请求。
- annotate_runtime / mark_runtime_error 补充身份与终态；HTTP Span 分项目存放。
"""

from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import Future
from contextlib import contextmanager
from contextvars import ContextVar
from weakref import WeakKeyDictionary

from opentelemetry import trace
from opentelemetry.context import Context
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, ParentBased, TraceIdRatioBased
from opentelemetry.trace import Link, Span, StatusCode

from egis_agentkit.client.nacos._settings import TracingConfig

logger = logging.getLogger(__name__)
_provider_lock = threading.Lock()
_runtime_root: ContextVar[Span | None] = ContextVar(
    "agentkit_runtime_trace", default=None
)
_dispatchers: WeakKeyDictionary[TracerProvider, _DispatchProcessor] = (
    WeakKeyDictionary()
)


def annotate_runtime(*, name=None, attributes=None):
    """补充运行身份；只更新当前任务的运行根，不修改其他请求的 Trace。"""
    span = _runtime_root.get()
    if span is not None:
        if name is not None:
            span.update_name(name)
        if attributes:
            span.set_attributes(attributes)


def mark_runtime_error(error_type, *, session_id=None):
    """原生流程已捕获错误时，仍把安全错误类别记到运行根。"""
    span = _runtime_root.get()
    if span is not None and span.is_recording():
        if session_id is not None and span.attributes.get("session.id") != session_id:
            return
        span.set_attribute("error.type", error_type)
        span.set_status(StatusCode.ERROR)


@contextmanager
def _phase_span(name, attributes=None, *, root=False, required=True):
    """创建有界阶段，异常仅记录类别，避免上游错误文本泄漏凭据。"""
    parent = trace.get_current_span().get_span_context()
    links = [Link(parent)] if root and parent.is_valid else []
    with trace.get_tracer("egis_agentkit").start_as_current_span(
        name,
        context=Context() if root else None,
        links=links,
        attributes={"openinference.span.kind": "CHAIN", **(attributes or {})},
        record_exception=False,
        set_status_on_exception=False,
    ) as span:
        token = _runtime_root.set(span) if root else None
        try:
            yield span
        except BaseException as exc:
            span.set_attribute("error.type", type(exc).__name__)
            span.set_status(StatusCode.ERROR)
            if required:
                mark_runtime_error(type(exc).__name__)
            raise
        else:
            if span.is_recording() and span.status.status_code != StatusCode.ERROR:
                span.set_status(StatusCode.OK)
        finally:
            if token is not None:
                _runtime_root.reset(token)


@contextmanager
def trace_phase(name, attributes=None, *, required=True):
    """在已启用的运行根内记录阶段；管理请求及关闭 tracing 时不产生额外 Span。"""
    if _runtime_root.get() is None:
        yield trace.INVALID_SPAN
        return
    with _phase_span(name, attributes, required=required) as span:
        yield span


class _ExportProcessor(SpanProcessor):
    """只管理一次订阅自己的采样、资源和批处理器。"""

    def __init__(self, batch: BatchSpanProcessor, settings: TracingConfig):
        self._batch = batch
        self._lock = threading.Lock()
        self._sampler = TraceIdRatioBased(settings.sample_ratio)
        self._flush_timeout = settings.shutdown_timeout_milliseconds
        self._resource = Resource(
            {
                "service.name": settings.service_name,
                "openinference.project.name": settings.project_name,
            }
        )
        self._http_resource = (
            Resource(
                {
                    "service.name": settings.service_name,
                    "openinference.project.name": settings.http_project_name,
                }
            )
            if settings.http_project_name is not None
            else None
        )

    def on_end(self, span: ReadableSpan) -> None:
        batch = self._batch
        if batch is None or span.context is None:
            return
        # 按 trace id 一致抽样，避免拆散同一条 reply/model/tool 调用链。
        decision = self._sampler.should_sample(None, span.context.trace_id, span.name)
        if not decision.decision.is_sampled():
            return
        # 页面轮询与接口内部 Span 单列项目，保留排查入口并避免淹没 Agent 运行记录。
        scope = span.instrumentation_scope.name if span.instrumentation_scope else ""
        is_http = (
            "http.route" in (span.attributes or {})
            or span.name.startswith("fastapi.")
            or scope
            in {
                "opentelemetry.instrumentation.fastapi",
                "opentelemetry.instrumentation.asgi",
            }
        )
        if is_http and self._http_resource is None:
            return
        resource = self._http_resource if is_http else self._resource
        exported = ReadableSpan(
            name=span.name,
            context=span.context,
            parent=span.parent,
            resource=span.resource.merge(resource),
            attributes=span.attributes,
            events=span.events,
            links=span.links,
            kind=span.kind,
            status=span.status,
            start_time=span.start_time,
            end_time=span.end_time,
            instrumentation_scope=span.instrumentation_scope,
        )
        batch.on_end(exported)

    def detach(self) -> BatchSpanProcessor | None:
        """停止接收新 span，把已排队的数据交还关闭流程。"""
        with self._lock:
            batch, self._batch = self._batch, None
        return batch

    def shutdown(self) -> None:
        batch = self.detach()
        if batch is not None:
            batch.shutdown()

    def force_flush(self, timeout_millis: int | None = None) -> bool:
        batch = self._batch
        return batch is None or batch.force_flush(
            self._flush_timeout if timeout_millis is None else timeout_millis
        )


class _DispatchProcessor(SpanProcessor):
    """每个 provider 只注册一次，只转发当前活动的导出订阅。"""

    def __init__(self):
        self._subscribers: set[_ExportProcessor] = set()
        self._lock = threading.Lock()

    def subscribe(self, processor: _ExportProcessor) -> None:
        with self._lock:
            self._subscribers.add(processor)

    def unsubscribe(self, processor: _ExportProcessor) -> None:
        with self._lock:
            self._subscribers.discard(processor)

    def _snapshot(self) -> tuple[_ExportProcessor, ...]:
        with self._lock:
            return tuple(self._subscribers)

    def on_end(self, span: ReadableSpan) -> None:
        for processor in self._snapshot():
            processor.on_end(span)

    def shutdown(self) -> None:
        with self._lock:
            subscribers = tuple(self._subscribers)
            self._subscribers.clear()
        for processor in subscribers:
            processor.shutdown()

    def force_flush(self, timeout_millis: int | None = None) -> bool:
        results = [
            processor.force_flush(timeout_millis) for processor in self._snapshot()
        ]
        return all(results)


class TracingClient:
    """连接原生全局 SDK provider，持有可重复启动的导出资源。"""

    def __init__(self, settings: TracingConfig):
        self.settings = settings
        self._processor: _ExportProcessor | None = None
        self._dispatcher: _DispatchProcessor | None = None
        self._closing_futures: set[Future[None]] = set()

    @contextmanager
    def span(self, name, *, attributes=None, root=False):
        """为真实运行入口建立独立根并链接请求；阶段共用同一 Trace。"""
        if not self.settings.enabled:
            token = _runtime_root.set(None) if root else None
            try:
                yield trace.INVALID_SPAN
            finally:
                if token is not None:
                    _runtime_root.reset(token)
            return
        with _phase_span(name, attributes, root=root) as span:
            yield span

    def start(self) -> None:
        """启用时才配置 SDK；已有 provider 的采样和资源保持原状。"""
        if not self.settings.enabled or self._processor is not None:
            return
        with _provider_lock:
            if self._processor is not None:
                return
            provider = trace.get_tracer_provider()
            if isinstance(provider, trace.ProxyTracerProvider):
                # provider 进程级只注册一次；导出采样由各客户端独立控制。
                trace.set_tracer_provider(
                    TracerProvider(sampler=ParentBased(ALWAYS_ON))
                )
                provider = trace.get_tracer_provider()
            if not isinstance(provider, TracerProvider):
                raise RuntimeError(
                    "Tracing requires an OpenTelemetry SDK TracerProvider"
                )
            dispatcher = _dispatchers.get(provider)
            if dispatcher is None:
                dispatcher = _DispatchProcessor()
                provider.add_span_processor(dispatcher)
                _dispatchers[provider] = dispatcher

            exporter = OTLPSpanExporter(
                endpoint=self.settings.endpoint,
                headers={
                    key: value.get_secret_value()
                    for key, value in self.settings.headers.items()
                },
                timeout=self.settings.export_timeout_seconds,
            )
            try:
                batch = BatchSpanProcessor(
                    exporter,
                    max_queue_size=self.settings.max_queue_size,
                    max_export_batch_size=self.settings.max_export_batch_size,
                    schedule_delay_millis=self.settings.schedule_delay_milliseconds,
                    export_timeout_millis=self.settings.export_timeout_seconds * 1000,
                )
            except BaseException:
                exporter.shutdown()
                raise
            processor = _ExportProcessor(batch, self.settings)
            dispatcher.subscribe(processor)
            self._processor = processor
            self._dispatcher = dispatcher
        logger.info(
            "tracing.started",
            extra={
                "context": {
                    "project_name": self.settings.project_name,
                    "sample_ratio": self.settings.sample_ratio,
                }
            },
        )

    async def aclose(self) -> None:
        """限时等待 flush，慢速导出的资源关闭继续在线程中执行。"""
        with _provider_lock:
            processor, self._processor = self._processor, None
            if processor is None:
                return
            dispatcher, self._dispatcher = self._dispatcher, None
            if dispatcher is not None:
                dispatcher.unsubscribe(processor)
            batch = processor.detach()
        if batch is None:
            return

        completion: Future[None] = Future()

        def close_owned() -> None:
            try:
                try:
                    batch.force_flush(self.settings.shutdown_timeout_milliseconds)
                finally:
                    batch.shutdown()
            except BaseException as error:
                completion.set_exception(error)
            else:
                completion.set_result(None)

        # 不占用默认 executor，避免 asyncio.run 退出时再次等待慢速导出。
        with _provider_lock:
            self._closing_futures.add(completion)
        completion.add_done_callback(self._close_finished)
        wrapped = asyncio.wrap_future(completion)
        wrapped.add_done_callback(
            lambda result: None if result.cancelled() else result.exception()
        )
        threading.Thread(
            target=close_owned, name="egis-tracing-cleanup", daemon=True
        ).start()
        try:
            await asyncio.wait_for(
                asyncio.shield(wrapped),
                self.settings.shutdown_timeout_milliseconds / 1000,
            )
        except TimeoutError:
            logger.warning(
                "tracing.shutdown_timeout",
            )

    def _close_finished(self, completion: Future[None]) -> None:
        with _provider_lock:
            self._closing_futures.discard(completion)
        if completion.exception() is not None:
            logger.error(
                "tracing.cleanup_failed",
            )
        else:
            logger.info(
                "tracing.stopped",
                extra={
                    "context": {
                        "project_name": self.settings.project_name,
                        "sample_ratio": self.settings.sample_ratio,
                    }
                },
            )
