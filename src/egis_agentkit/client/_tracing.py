"""为原生 AgentScope span 提供 OTLP HTTP 导出和独立关闭能力。

核心能力
- 原生 TracingMiddleware 负责创建 span，本模块只配置导出、采样和批处理。
- 复用已有 SDK provider，只关闭本客户端创建的 processor 与 exporter。
- 导出副本携带 Phoenix 项目资源，不修改宿主 provider 或其他导出器的数据。
"""

from __future__ import annotations

import asyncio
import logging
import threading
from concurrent.futures import Future
from weakref import WeakKeyDictionary

from opentelemetry import trace
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter
from opentelemetry.sdk.resources import Resource
from opentelemetry.sdk.trace import ReadableSpan, SpanProcessor, TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.trace.sampling import ALWAYS_ON, ParentBased, TraceIdRatioBased

from egis_agentkit.client.nacos._settings import TracingConfig

logger = logging.getLogger(__name__)
_provider_lock = threading.Lock()
_dispatchers: WeakKeyDictionary[TracerProvider, _DispatchProcessor] = (
    WeakKeyDictionary()
)


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

    def on_end(self, span: ReadableSpan) -> None:
        batch = self._batch
        if batch is None or span.context is None:
            return
        # 按 trace id 一致抽样，避免拆散同一条 reply/model/tool 调用链。
        decision = self._sampler.should_sample(None, span.context.trace_id, span.name)
        if not decision.decision.is_sampled():
            return
        exported = ReadableSpan(
            name=span.name,
            context=span.context,
            parent=span.parent,
            resource=span.resource.merge(self._resource),
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
