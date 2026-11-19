"""为 ASGI 请求记录关联 ID、规范路由及安全异常信息。"""

import logging
import time
from uuid import uuid4

from starlette.responses import JSONResponse

from egis_agentkit.client._logging import (
    exception_description,
    exception_origin,
    http_log_context,
)

logger = logging.getLogger(__name__)

_METHODS = frozenset(
    ("GET", "HEAD", "POST", "PUT", "DELETE", "OPTIONS", "PATCH", "TRACE", "CONNECT")
)


def _request_fields(scope):
    route = getattr(scope.get("route"), "path", None)
    if route is None:
        # Rejected requests may never reach routing. Do not log the raw path.
        route = "<unmatched>"
    method = scope["method"]
    return {"method": method if method in _METHODS else "OTHER", "route": route}


def log_http_error(
    log,
    scope,
    description,
    status_code,
    *,
    component,
    exc=None,
    system=False,
    fields=None,
):
    """业务拒绝记录抛出点，系统故障保留完整的安全栈。"""
    system = system or status_code >= 500
    context = {
        **(fields or {}),
        **_request_fields(scope),
        "category": "system" if system else "business",
        "status_code": status_code,
    }
    if exc is not None:
        context["error_type"] = type(exc).__name__
        detail = exception_description(exc)
        if detail is not None:
            context["error_description"] = detail
        if not system:
            context["origin"] = exception_origin(exc)
    log.log(
        logging.ERROR if system else logging.WARNING,
        description,
        extra={"component": component, "context": context},
        exc_info=exc if system else None,
    )


class RequestLogging:
    """写入请求 ID 并观察原生 ASGI 消息，保持流式响应逐块传递。"""

    def __init__(self, app, *, access_log: bool):
        self.app = app
        self.access_log = access_log

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        started = time.monotonic()
        request_id = uuid4().hex
        token = http_log_context.set((request_id, scope.setdefault("state", {})))
        status = None
        failed = False

        async def send_response(message):
            nonlocal status
            if message["type"] == "http.response.start":
                status = message["status"]
                headers = [
                    (name, value)
                    for name, value in message.get("headers", [])
                    if name.lower() != b"x-request-id"
                ]
                headers.append((b"x-request-id", request_id.encode("ascii")))
                message = {**message, "headers": headers}
            await send(message)

        try:
            await self.app(scope, receive, send_response)
        except Exception as exc:
            failed = True
            log_http_error(
                logger,
                scope,
                "HTTP request failed",
                500,
                component="RequestLogging",
                exc=exc,
            )
            if status is not None:
                # The transport must terminate a failed stream once headers are sent.
                raise
            await JSONResponse(
                {"detail": "服务器内部错误"},
                status_code=500,
                headers={"Cache-Control": "no-store"},
            )(scope, receive, send_response)
        finally:
            if self.access_log and status is not None:
                logger.info(
                    "HTTP request finished",
                    extra={
                        "component": "RequestLogging",
                        "context": {
                            **_request_fields(scope),
                            "status_code": status,
                            "outcome": "failed" if failed else "responded",
                            "duration_ms": round((time.monotonic() - started) * 1000),
                        },
                    },
                )
            http_log_context.reset(token)
