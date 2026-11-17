"""仅管理 Framework/App 标准日志命名空间的控制台输出。

结构化字段通过 ``extra={"context": {...}}`` 显式传入；敏感键递归脱敏，
不序列化任意 LogRecord 字段或对象。调用方仍须避免在消息及普通字符串字段
中传入凭据。异常仅输出类型与栈位置，不输出异常文本、源码或局部变量。
"""

from __future__ import annotations

import json
import logging
import math
import sys
from datetime import datetime, timezone
from pathlib import Path
from threading import RLock
from traceback import walk_tb

from opentelemetry import trace

from .nacos._settings import LoggingConfig

_NAMESPACES = ("egis_agentkit", "egis_agentkit_app")
_SENSITIVE_FIELDS = (
    "password",
    "token",
    "apikey",
    "secret",
    "dsn",
    "authorization",
    "credential",
)
_lock = RLock()
_clients: list[LoggingClient] = []
_original: dict[logging.Logger, tuple[list[logging.Handler], int, bool]] = {}


def _safe_context(value, seen: set[int]):
    """保留 JSON 数据，避免对象 repr、循环引用和敏感字段进入日志。"""
    if value is None or type(value) in (str, bool, int):
        return value
    if type(value) is float:
        return value if math.isfinite(value) else "[UNSUPPORTED]"
    if type(value) not in (dict, list, tuple):
        return "[UNSUPPORTED]"
    if id(value) in seen:
        return "[CIRCULAR]"
    seen.add(id(value))
    try:
        if type(value) is dict:
            result = {}
            for key, item in value.items():
                if type(key) is not str:
                    continue
                normalized = "".join(char.lower() for char in key if char.isalnum())
                result[key] = (
                    "[REDACTED]"
                    if any(field in normalized for field in _SENSITIVE_FIELDS)
                    else _safe_context(item, seen)
                )
            return result
        return [_safe_context(item, seen) for item in value]
    finally:
        seen.remove(id(value))


class _Formatter(logging.Formatter):
    def __init__(self, output_format: str):
        super().__init__()
        self._output_format = output_format

    def format(self, record: logging.LogRecord) -> str:
        event = {
            "time": datetime.fromtimestamp(record.created, timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        span_context = trace.get_current_span().get_span_context()
        if span_context.is_valid:
            event["trace_id"] = format(span_context.trace_id, "032x")
            event["span_id"] = format(span_context.span_id, "016x")
        context = getattr(record, "context", None)
        if isinstance(context, dict):
            try:
                event["context"] = _safe_context(context, set())
            except RecursionError:
                event["context"] = "[UNSUPPORTED]"
        if record.exc_info and record.exc_info[0] is not None:
            event["exception"] = {
                "type": record.exc_info[0].__name__,
                "frames": [
                    {
                        "file": Path(frame.f_code.co_filename).name,
                        "line": line,
                        "function": frame.f_code.co_name,
                    }
                    for frame, line in walk_tb(record.exc_info[2])
                ],
            }
        if self._output_format == "json":
            return json.dumps(event, ensure_ascii=False, allow_nan=False)
        text = f"{event['time']} {event['level']} {event['logger']} {event['message']}"
        details = {
            key: value
            for key, value in event.items()
            if key not in ("time", "level", "logger", "message")
        }
        if details:
            text += " " + json.dumps(details, ensure_ascii=False, allow_nan=False)
        return text


def _activate(client: LoggingClient) -> None:
    for name in _NAMESPACES:
        logger = logging.getLogger(name)
        logger.handlers[:] = [client._handler]
        logger.setLevel(client._settings.level)
        logger.propagate = False


class LoggingClient:
    """启动时接管两个日志命名空间，最后关闭时恢复宿主的原始配置。

    重复启动或关闭无副作用；多个实例并存时采用最后启动实例的配置，
    关闭该实例后恢复仍在运行的前一个实例。根日志与原生 Loguru 不受影响。
    """

    def __init__(self, settings: LoggingConfig):
        self._settings = settings
        self._handler: logging.StreamHandler | None = None

    def start(self) -> None:
        with _lock:
            if self._handler is not None:
                return
            handler = logging.StreamHandler(sys.stdout)
            handler.setLevel(self._settings.level)
            handler.setFormatter(_Formatter(self._settings.format))
            if not _clients:
                for name in _NAMESPACES:
                    logger = logging.getLogger(name)
                    _original[logger] = (
                        logger.handlers[:],
                        logger.level,
                        logger.propagate,
                    )
            self._handler = handler
            _clients.append(self)
            _activate(self)

    def close(self) -> None:
        with _lock:
            if self._handler is None:
                return
            _clients.remove(self)
            if _clients:
                _activate(_clients[-1])
            else:
                for logger, (handlers, level, propagate) in _original.items():
                    logger.handlers[:] = handlers
                    logger.setLevel(level)
                    logger.propagate = propagate
                _original.clear()
            self._handler.close()
            self._handler = None
