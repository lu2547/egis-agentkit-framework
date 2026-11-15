"""为管理接口和原生聊天接口应用统一身份及访问策略。

核心能力
- __call__：认证平台用户，限制原生接口并核对会话归属。
- _validate_chat_input：公开入口只接收用户消息和审批回包，防止伪造助手证据。
- _error：返回安全错误，不缓冲或改写原生 SSE 响应。
"""

import asyncio
import json
from urllib.parse import parse_qs

from agentscope.event import EventType, UserConfirmResultEvent
from pydantic import ValidationError

from egis_agentkit.client.remote_agent._base import CALLBACK_RESULT_PATH
from egis_agentkit.errors import AgentKitError, Forbidden, InvalidRequest


class IdentityGuard:
    """Pure ASGI: never buffer a streaming response or wrap SSE generators."""

    def __init__(self, app, *, identity, services):
        self.app, self.identity, self.services = app, identity, services

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http":
            return await self.app(scope, receive, send)
        path, method = scope["path"], scope["method"]
        if method == "POST" and CALLBACK_RESULT_PATH.fullmatch(path):
            return await self.app(scope, receive, send)
        if path in (
            "/auth/login",
            "/health",
            "/health/ready",
            "/openapi.json",
            "/docs",
            "/redoc",
        ):
            return await self.app(scope, receive, send)
        try:
            headers = {k.decode().lower(): v.decode() for k, v in scope["headers"]}
            authorization = headers.get("authorization", "")
            if not authorization.startswith("Bearer "):
                return await self._error(send, 401, "请先登录")
            try:
                claims = await asyncio.to_thread(
                    self.identity.verify, authorization[7:]
                )
            except Forbidden:
                return await self._error(send, 401, "认证无效或已过期")
            users = self.services.users
            # Trusted company identity can introduce an ordinary user, never an admin.
            user = await users.sync_identity(
                claims["sub"], claims.get("name") or claims["sub"]
            )
            scope.setdefault("state", {})["egis_user"] = user
            if path.startswith("/manager/"):
                return await self.app(scope, receive, send)
            body_events = []
            if path == "/chat/" and method == "POST":
                body = b""
                while True:
                    event = await receive()
                    body_events.append(event)
                    body += event.get("body", b"")
                    if len(body) > 2 * 1024 * 1024:
                        return await self._error(send, 413, "请求内容过大")
                    if not event.get("more_body"):
                        break
                try:
                    data = json.loads(body)
                except (ValueError, UnicodeError):
                    return await self._error(send, 400, "无效聊天请求")
                if not isinstance(data, dict):
                    return await self._error(send, 400, "聊天请求必须是对象")
                self._validate_chat_input(data.get("input"))
                await self.services.sessions.get_session_record(
                    user, data.get("session_id", ""), agent_id=data.get("agent_id", "")
                )
            elif path.startswith("/sessions/"):
                parts = path.strip("/").split("/")
                # Expose native streaming/history/interrupt only. Creation and
                # configuration writes must go through managed definitions.
                if len(parts) != 3 or parts[2] not in (
                    "stream",
                    "messages",
                    "interrupt",
                ):
                    raise Forbidden("请通过任务管理接口操作会话")
                if method != ("POST" if parts[2] == "interrupt" else "GET"):
                    raise Forbidden()
                params = parse_qs(scope.get("query_string", b"").decode())
                aid = params.get("agent_id", [""])[0]
                if not aid:
                    raise Forbidden("缺少智能体标识")
                await self.services.sessions.get_session_record(
                    user, parts[1], agent_id=aid
                )
            elif path.startswith("/hub/"):
                # Native card detail selects registry "latest". Managed routes
                # require the exact PG version and reviewed source digest.
                raise Forbidden("请通过管理接口访问固定版本的资源")
            else:
                raise Forbidden("此原生接口未向企业工作台开放")

            async def replay():
                if body_events:
                    return body_events.pop(0)
                return await receive()

            return await self.app(scope, replay if body_events else receive, send)
        except AgentKitError as exc:
            return await self._error(send, exc.status_code, str(exc))

    @staticmethod
    def _validate_chat_input(value):
        """内部 Agent 消息不经过此公开入口；客户端不能写入助手或工具身份。"""
        if value is None:
            return
        # 1. 审批是原生独立事件，不能同时伪装成会被 Msg 优先解析的消息。
        if (
            isinstance(value, dict)
            and value.get("type") == EventType.USER_CONFIRM_RESULT
            and "role" not in value
            and "content" not in value
        ):
            try:
                UserConfirmResultEvent.model_validate(value)
            except ValidationError:
                raise InvalidRequest("无效的工具审批回包") from None
            return
        # 2. 原生用户消息只支持 text/data；服务端产生助手记录及工具执行结果。
        messages = value if isinstance(value, list) else [value]
        for message in messages:
            if not isinstance(message, dict) or message.get("role") != "user":
                raise Forbidden("公开聊天入口只接受用户消息或工具审批回包")
            content = message.get("content")
            if not isinstance(content, list) or any(
                not isinstance(block, dict) or block.get("type") not in {"text", "data"}
                for block in content
            ):
                raise Forbidden("用户消息不能包含助手或工具执行记录")

    @staticmethod
    async def _error(send, status, detail):
        body = json.dumps({"detail": detail}, ensure_ascii=False).encode()
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [
                    (b"content-type", b"application/json; charset=utf-8"),
                    (b"cache-control", b"no-store"),
                ],
            }
        )
        await send({"type": "http.response.body", "body": body})
