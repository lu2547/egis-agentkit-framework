"""为管理接口和原生聊天接口应用统一身份及访问策略。"""

import asyncio
import json
import logging
import re
from urllib.parse import parse_qs

from pydantic import SecretStr

from egis_agentkit.client.remote_agent._base import CALLBACK_RESULT_PATH
from egis_agentkit.core.session._context import (
    VerifiedChatAuthorization,
    verified_chat_authorization,
)
from egis_agentkit.errors import AgentKitError, Forbidden
from egis_agentkit.middleware.http._request_logging import log_http_error
from egis_agentkit.service._virtual_user import (
    EXTERNAL_SKILL_IMPORT_PATH,
    VIRTUAL_TOKEN_PREFIX,
    VirtualUserService,
)

logger = logging.getLogger(__name__)


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
            "/health",
            "/health/ready",
            "/openapi.json",
            "/docs",
            "/redoc",
        ):
            return await self.app(scope, receive, send)
        body_events = []
        chat_authorization = None
        try:
            headers = {
                k.decode("latin-1").lower(): v.decode("latin-1")
                for k, v in scope["headers"]
            }
            authorization = headers.get("authorization", "")
            if not authorization.startswith("Bearer "):
                raise AgentKitError("请先登录", 401)
            token = authorization[7:]
            if token.startswith(VIRTUAL_TOKEN_PREFIX):
                try:
                    user = await self.services.virtual_users.authenticate(token)
                except Forbidden as exc:
                    raise AgentKitError("虚拟用户凭证无效", 401) from exc
                state = scope.setdefault("state", {})
                state["egis_user"] = user
                state["egis_virtual_user"] = True
                if not VirtualUserService.allows_request(method, path):
                    raise Forbidden("虚拟用户只能使用技能导入接口")
            else:
                chat_authorization = await self._authenticate_user(
                    scope, method, path, token, receive, body_events
                )
                authorizations = getattr(self.services.sessions, "authorizations", None)
                if (
                    chat_authorization is not None
                    and authorizations is not None
                    and method == "POST"
                    and path.startswith("/chat/")
                ):
                    await authorizations.remember(chat_authorization)
        except AgentKitError as exc:
            log_http_error(
                logger,
                scope,
                "Identity or request policy rejected access",
                exc.status_code,
                component="IdentityGuard",
                exc=exc,
            )
            return await self._error(send, exc.status_code, str(exc))

        async def replay():
            if body_events:
                return body_events.pop(0)
            return await receive()

        marker = verified_chat_authorization.set(chat_authorization)
        try:
            return await self.app(scope, replay if body_events else receive, send)
        finally:
            verified_chat_authorization.reset(marker)

    async def _authenticate_user(
        self, scope, method, path, token, receive, body_events
    ):
        try:
            claims = await asyncio.to_thread(self.identity.verify, token)
        except Forbidden as exc:
            raise AgentKitError("认证无效或已过期", 401) from exc
        if path == EXTERNAL_SKILL_IMPORT_PATH or path.startswith(
            EXTERNAL_SKILL_IMPORT_PATH + "/"
        ):
            raise Forbidden("外部技能导入仅接受虚拟用户凭证")
        # Trusted company identity can introduce an ordinary user, never an admin.
        user = await self.services.users.sync_identity(
            claims["sub"], claims.get("name") or claims["sub"]
        )
        scope.setdefault("state", {})["egis_user"] = user
        if path.startswith("/manager/"):
            return
        queue_path = re.fullmatch(r"/chat/queue(?:/([^/]+)(/steer)?)?", path)
        if queue_path:
            item, steer = queue_path.groups()
            allowed = (
                {"GET"}
                if item is None
                else {"POST"}
                if steer
                else {"PATCH"}
                if item == "order"
                else {"PATCH", "DELETE"}
            )
            if method not in allowed:
                raise Forbidden("不支持此队列操作")
        if (path == "/chat/" and method == "POST") or queue_path:
            if method in {"GET", "DELETE"}:
                params = parse_qs(scope.get("query_string", b"").decode())
                if any(
                    len(params.get(key, [])) != 1 for key in ("agent_id", "session_id")
                ):
                    raise AgentKitError("智能体与会话标识必须各指定一次", 400)
                data = {key: values[0] for key, values in params.items()}
            else:
                data = await self._read_chat_body(receive, body_events)
            if any(
                not isinstance(data.get(key), str) or not data[key]
                for key in ("agent_id", "session_id")
            ):
                raise AgentKitError("缺少智能体或会话标识", 400)
            session = await self.services.sessions.get_session_record(
                user, data["session_id"], agent_id=data["agent_id"]
            )
            return VerifiedChatAuthorization(
                user.user_id,
                session.agent_id,
                session.id,
                SecretStr(token),
                session,
                asyncio.current_task(),
                expires_at=float(claims["exp"]),
            )
        elif path.startswith("/sessions/"):
            parts = path.strip("/").split("/")
            # Expose native streaming/history/status/interrupt only. Creation and
            # configuration writes must go through managed definitions.
            if len(parts) != 3 or parts[2] not in (
                "stream",
                "messages",
                "status",
                "interrupt",
            ):
                raise Forbidden("请通过任务管理接口操作会话")
            if method != ("POST" if parts[2] == "interrupt" else "GET"):
                raise Forbidden()
            params = parse_qs(scope.get("query_string", b"").decode())
            aid = params.get("agent_id", [""])[0]
            if not aid:
                raise Forbidden("缺少智能体标识")
            session = await self.services.sessions.get_session_record(
                user, parts[1], agent_id=aid
            )
            return VerifiedChatAuthorization(
                user.user_id,
                session.agent_id,
                session.id,
                SecretStr(token),
                session if parts[2] != "interrupt" else None,
                asyncio.current_task(),
                expires_at=float(claims["exp"]),
            )
        elif path.startswith("/hub/"):
            # Native card detail selects registry "latest". Managed routes
            # require the exact PG version and reviewed source digest.
            raise Forbidden("请通过管理接口访问固定版本的资源")
        else:
            raise Forbidden("此原生接口未向企业工作台开放")

    @staticmethod
    async def _read_chat_body(receive, body_events):
        body = b""
        while True:
            event = await receive()
            if event["type"] == "http.disconnect":
                raise AgentKitError("聊天请求已中断", 400)
            body_events.append(event)
            body += event.get("body", b"")
            if len(body) > 2 * 1024 * 1024:
                raise AgentKitError("请求内容过大", 413)
            if not event.get("more_body"):
                break
        try:
            data = json.loads(body)
        except (ValueError, UnicodeError) as exc:
            raise AgentKitError("无效聊天请求", 400) from exc
        if not isinstance(data, dict):
            raise AgentKitError("聊天请求必须是对象", 400)
        return data

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
