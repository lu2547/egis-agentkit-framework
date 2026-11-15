"""提交 HTTP 异步任务，并校验第三方按约定签名的回包。"""

from __future__ import annotations

import hashlib
import hmac
import os
import time
from typing import Mapping

import httpx
from pydantic import ValidationError

from egis_agentkit.client.nacos._settings import RemoteConfig
from egis_agentkit.errors import AgentKitError
from egis_agentkit.storage.model._remote_agent import RemoteRunRequest, RemoteRunResult
from egis_agentkit.storage.model.agent._remote_config import (
    RemoteAgentConfig,
    validate_endpoint,
)

from ._base import (
    CALLBACK_PATH,
    Progress,
    RemoteAgentClientBase,
    RemoteTransportError,
    send_http_cancel,
    send_http_request,
)


class CallbackHTTPRemoteAgentClient(RemoteAgentClientBase):
    """提交后返回接受标记；会话与回包的关联由运行接入负责。"""

    def __init__(
        self,
        connection_id: str,
        config: RemoteAgentConfig,
        settings: RemoteConfig,
        *,
        http: httpx.AsyncClient | None = None,
        callback_base_url: str | None = None,
    ) -> None:
        super().__init__(connection_id, config, settings)
        self.http, self.callback_base_url = http, callback_base_url

    async def _send_request(
        self, request: RemoteRunRequest, on_progress: Progress | None
    ) -> RemoteRunResult | None:
        callback = self.settings.callback
        callback_url = self.callback_base_url or callback.public_base_url
        if not callback_url and callback.public_base_url_env:
            callback_url = os.environ.get(callback.public_base_url_env)
        if not callback_url or not validate_endpoint(callback_url, {"http", "https"}):
            raise RemoteTransportError("configuration", "未配置远程专家回包地址")
        if (
            len(os.environ.get(self.config.callback_secret_ref or "", "").encode())
            < callback.min_secret_bytes
        ):
            raise RemoteTransportError("configuration", "远程专家回包凭证未正确配置")
        payload = {
            **request.model_dump(mode="json"),
            "callback_url": f"{callback_url.rstrip('/')}{CALLBACK_PATH}/{request.run_id}/result",
        }
        status, _ = await send_http_request(self, self.config.endpoint, payload)
        if status != httpx.codes.ACCEPTED:
            raise RemoteTransportError("protocol", "异步远程专家必须返回 HTTP 202")
        return None

    async def send_cancel_request(self, run_id: str) -> bool:
        return await send_http_cancel(self, run_id)

    @staticmethod
    def validate_callback(
        run_id: str,
        connection_id: str,
        config: RemoteAgentConfig,
        settings: RemoteConfig,
        headers: Mapping[str, str],
        body: bytes,
    ) -> RemoteRunResult:
        """验证连接身份、签名、时效和数据结构，返回结构化结果。"""
        connection = headers.get("x-egis-connection", "")
        timestamp = headers.get("x-egis-timestamp", "")
        signature = headers.get("x-egis-signature", "")
        callback = settings.callback
        unauthorized = AgentKitError("远程回包认证失败", httpx.codes.UNAUTHORIZED)
        latest = str(int(time.time() + callback.max_clock_skew_seconds))
        if (
            not timestamp.isascii()
            or not timestamp.isdecimal()
            or len(timestamp) > len(latest)
        ):
            raise unauthorized
        if (
            abs(time.time() - int(timestamp)) > callback.max_clock_skew_seconds
            or connection != connection_id
        ):
            raise unauthorized
        secret = os.environ.get(config.callback_secret_ref or "", "")
        if (
            config.transport != "http_callback"
            or len(secret.encode()) < callback.min_secret_bytes
        ):
            raise unauthorized
        signed = f"{timestamp}\n{connection}\n{run_id}\n".encode() + body
        expected = (
            "sha256=" + hmac.new(secret.encode(), signed, hashlib.sha256).hexdigest()
        )
        if not signature.isascii() or not hmac.compare_digest(signature, expected):
            raise unauthorized
        if len(body) > callback.max_bytes:
            raise AgentKitError("远程回包过大", httpx.codes.REQUEST_ENTITY_TOO_LARGE)
        try:
            result = RemoteRunResult.model_validate_json(body)
        except ValidationError:
            raise AgentKitError("远程回包格式错误", httpx.codes.BAD_REQUEST) from None
        if result.run_id != run_id:
            raise AgentKitError(
                "远程回包的任务标识与路径不一致", httpx.codes.BAD_REQUEST
            )
        return result
