"""以标准 logging 配置 AgentKit、App 及进程运行时日志。

结构化字段通过 ``extra={"context": {...}}`` 显式传入；敏感键递归脱敏，
不序列化任意 LogRecord 字段或对象。调用方仍须避免在消息及普通字符串字段
中传入凭据。异常记录公开的业务说明、系统错误类型和堆栈位置。
"""

from __future__ import annotations

import json
import logging
import math
import os
import socket
import sys
from contextlib import ExitStack, contextmanager
from contextvars import ContextVar
from datetime import datetime, timezone
from logging.config import DictConfigurator, dictConfig
from pathlib import Path
from threading import RLock
from traceback import walk_tb

from opentelemetry import trace

from egis_agentkit.errors import AgentKitError

from .nacos._settings import LoggingConfig

_NAMESPACES = ("egis_agentkit", "egis_agentkit_app")
_RUNTIME_NAMESPACES = ("as", "uvicorn", "uvicorn.error")
_SENSITIVE_FIELDS = (
    "password",
    "token",
    "apikey",
    "secret",
    "dsn",
    "authorization",
    "credential",
    "cookie",
    "prompt",
)
_lock = RLock()
_clients: list[LoggingClient] = []
_original: dict[logging.Logger, tuple[list[logging.Handler], int, bool, bool]] = {}
http_log_context: ContextVar[tuple[str, dict] | None] = ContextVar(
    "agentkit_http_log_context", default=None
)
_session_id: ContextVar[str | None] = ContextVar(
    "agentkit_session_log_id", default=None
)


@contextmanager
def log_session(session_id: str | None):
    if session_id is None:
        yield
        return
    token = _session_id.set(session_id)
    try:
        yield
    finally:
        _session_id.reset(token)


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


def _location(frame, line):
    return {
        "package": frame.f_globals.get("__name__", ""),
        "file": Path(frame.f_code.co_filename).name,
        "line": line,
        "function": frame.f_code.co_qualname,
    }


def exception_origin(exc: BaseException) -> dict | None:
    """返回抛出位置，用于预期业务拒绝的单行日志。"""
    origin = None
    for frame, line in walk_tb(exc.__traceback__):
        origin = _location(frame, line)
    return origin


def exception_description(exc: BaseException) -> str | None:
    """只记录可公开的业务说明和系统定义的错误描述。"""
    if isinstance(exc, AgentKitError):
        return str(exc)
    if isinstance(exc, OSError) and exc.errno is not None:
        return os.strerror(exc.errno)
    return None


def _exception(exc: BaseException, tb, seen: set[int]) -> dict:
    result = {"type": type(exc).__name__}
    if id(exc) in seen or len(seen) >= 32:
        return result
    seen.add(id(exc))
    description = exception_description(exc)
    if description is not None:
        result["description"] = description
    if isinstance(exc, OSError) and exc.errno is not None:
        result["errno"] = exc.errno
    result["frames"] = [_location(frame, line) for frame, line in walk_tb(tb)]
    # Keep internal causes even when a public exception suppresses them.
    cause = exc.__cause__ or exc.__context__
    if cause is not None:
        result["cause"] = _exception(cause, cause.__traceback__, seen)
    if isinstance(exc, BaseExceptionGroup):
        result["exceptions"] = [
            _exception(child, child.__traceback__, seen)
            for child in exc.exceptions[:32]
        ]
    return result


def _record_package(record):
    if record.name == "as":
        path = Path(record.pathname)
        parts = [path.stem]
        for parent in path.parents:
            parts.append(parent.name)
            if parent.name == "agentscope":
                return ".".join(reversed(parts))
    return record.name


def _record_function(record: logging.LogRecord) -> str:
    # LogRecord.funcName is unqualified, but the synchronous caller is still on the stack.
    frame = sys._getframe(1)
    while frame is not None:
        code = frame.f_code
        if (
            code.co_filename == record.pathname
            and code.co_name == record.funcName
            and frame.f_lineno == record.lineno
        ):
            return code.co_qualname.rsplit(".<locals>.", 1)[-1]
        frame = frame.f_back
    return record.funcName or "<unknown>"


class _Formatter(logging.Formatter):
    def __init__(self, settings: LoggingConfig):
        super().__init__()
        self._settings = settings

    def format(self, record: logging.LogRecord) -> str:
        component = getattr(record, "component", f"{record.module}.{record.funcName}")
        timestamp = datetime.fromtimestamp(record.created, timezone.utc)
        event = {
            "time": timestamp.isoformat(timespec="milliseconds"),
            "level": record.levelname,
            "logger": record.name,
            "package": _record_package(record),
            "function": _record_function(record),
            "file": record.filename,
            "line": record.lineno,
            "component": component,
            # AgentScope can interpolate provider errors and message content.
            "message": (
                f"AgentScope runtime event in {component}"
                if record.name == "as"
                else record.getMessage()
            ),
        }
        session_id = _session_id.get()
        if session_id is not None:
            event["session_id"] = session_id
        request_context = http_log_context.get()
        if request_context is not None:
            request_id, state = request_context
            event["request_id"] = request_id
            user_id = getattr(state.get("egis_user"), "user_id", None)
            if isinstance(user_id, str):
                event["user_id"] = user_id
                event["identity_type"] = (
                    "virtual" if state.get("egis_virtual_user") else "user"
                )
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
        if record.exc_info and record.exc_info[1] is not None:
            exc = record.exc_info[1]
            if (
                record.name == "as"
                and isinstance(exc, AgentKitError)
                and exc.status_code < 500
            ):
                event["message"] = f"AgentScope rejected an operation in {component}"
                event["context"] = {
                    "category": "business",
                    "error_type": type(exc).__name__,
                    "origin": exception_origin(exc),
                    "error_description": exception_description(exc),
                }
            else:
                event["exception"] = _exception(exc, record.exc_info[2], set())
                if record.name == "as":
                    event["message"] = f"AgentScope operation failed in {component}"
                    event["context"] = {"category": "system"}
        if self._settings.format == "json":
            return json.dumps(event, ensure_ascii=False, allow_nan=False)
        if self._settings.text_date_format is not None:
            event["time"] = timestamp.strftime(self._settings.text_date_format)
        text = self._settings.text_pattern.format_map(event)
        text = text.replace("\r", "\\r").replace("\n", "\\n")
        details = {
            key: value
            for key, value in event.items()
            if key not in LoggingConfig.TEXT_FIELDS
        }
        if details:
            text += " " + json.dumps(details, ensure_ascii=False, allow_nan=False)
        return text


def _logging_config(settings: LoggingConfig) -> dict:
    handlers = {
        "console": {
            "class": "logging.StreamHandler",
            "stream": "ext://sys.stdout",
            "formatter": "agentkit",
            "level": settings.level,
        }
    }
    if settings.directory is not None:
        settings.directory.mkdir(parents=True, exist_ok=True)
        process_log_name = f"agentkit-{socket.gethostname()}-{os.getpid()}.log"
        handlers["file"] = {
            "class": "logging.FileHandler",
            "filename": str(settings.directory / process_log_name),
            "mode": "a",
            "encoding": "utf-8",
            "formatter": "agentkit",
            "level": settings.level,
        }
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {"agentkit": {"()": _Formatter, "settings": settings}},
        "handlers": handlers,
        "loggers": {
            name: {
                "handlers": list(handlers),
                "level": settings.level,
                "propagate": False,
            }
            for name in (*_NAMESPACES, *_RUNTIME_NAMESPACES)
        },
    }


def configure_process_logging(settings: LoggingConfig) -> None:
    """由 CLI 使用，统一配置其拥有的进程日志。"""
    config = _logging_config(settings)
    # Canonical access logs are emitted by RequestLogging with sanitized route templates.
    config["loggers"]["uvicorn.access"] = {"handlers": [], "propagate": False}
    dictConfig(config)


def _activate() -> None:
    for logger, original in list(_original.items()):
        client = next(
            (
                client
                for client in reversed(_clients)
                if logger.name in client._namespaces
            ),
            None,
        )
        if client is None:
            handlers, level, propagate, disabled = original
            del _original[logger]
        else:
            handlers, level, propagate, disabled = (
                client._handlers,
                client._settings.level,
                False,
                False,
            )
        logger.handlers[:] = handlers
        logger.setLevel(level)
        logger.propagate = propagate
        logger.disabled = disabled


class LoggingClient:
    """在嵌入宿主中仅管理指定命名空间，关闭后恢复宿主配置。

    重复启动或关闭无副作用；多个实例并存时采用最后启动实例的配置，
    关闭该实例后恢复仍在运行的前一个实例。
    """

    def __init__(self, settings: LoggingConfig, *, include_runtime: bool = False):
        self._settings = settings
        self._namespaces = (*_NAMESPACES, "as") if include_runtime else _NAMESPACES
        self._handlers: list[logging.Handler] = []

    def start(self) -> None:
        with _lock:
            if self._handlers:
                return
            # Full dictConfig closes all registered handlers, even unrelated ones.
            # Reuse its declarative formatter and handler definitions without
            # applying global configuration when running inside another host.
            configurator = DictConfigurator(_logging_config(self._settings))
            formatters = configurator.config["formatters"]
            formatters["agentkit"] = configurator.configure_formatter(
                formatters["agentkit"]
            )
            handlers = []
            handler_configs = configurator.config["handlers"]
            with ExitStack() as stack:
                for name in handler_configs:
                    handler = configurator.configure_handler(handler_configs[name])
                    stack.callback(handler.close)
                    handlers.append(handler)
                stack.pop_all()
            for name in self._namespaces:
                logger = logging.getLogger(name)
                if logger not in _original:
                    _original[logger] = (
                        logger.handlers[:],
                        logger.level,
                        logger.propagate,
                        logger.disabled,
                    )
            self._handlers = handlers
            _clients.append(self)
            _activate()

    def close(self) -> None:
        with _lock:
            if not self._handlers:
                return
            _clients.remove(self)
            _activate()
            for handler in self._handlers:
                handler.close()
            self._handlers.clear()
