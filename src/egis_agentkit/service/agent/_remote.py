"""选择远程专家协议，校验回包并通知等待的会话。

核心能力
- 按定义配置创建协议客户端。
- 校验异步回包的签名、时限与对应任务，并通知等待者。
"""

import hashlib
import hmac
import logging
import os
import time

import httpx
from pydantic import ValidationError

from egis_agentkit.backend.nacos._settings import RemoteConfig
from egis_agentkit.errors import AgentKitError, Conflict
from egis_agentkit.storage._remote_agent import RemoteRunStorage
from egis_agentkit.storage.model._remote_agent import (
    RemoteAgentConfig,
    RemoteRunResult,
    valid_endpoint,
)

from ._remote_base import RemoteAgentBase, _result_key
from ._remote_http_impl import CallbackHTTPRemoteAgent, SyncHTTPRemoteAgent
from ._remote_websocket_impl import WebSocketRemoteAgent

logger = logging.getLogger(__name__)


def resolve_connection(
    config: RemoteAgentConfig, settings: RemoteConfig
) -> RemoteAgentConfig:
    """应用平台默认值，并在同一套限额下验证专家级覆盖。"""
    timeout = (
        config.timeout_seconds
        if config.timeout_seconds is not None
        else settings.timeout.default_seconds
    )
    retries = (
        config.retries if config.retries is not None else settings.retry.default_retries
    )
    if timeout > settings.timeout.max_seconds or retries >= settings.retry.max_attempts:
        raise Conflict("远程专家的超时或重试次数超出平台限制")
    return config.model_copy(update={"timeout_seconds": timeout, "retries": retries})


class RemoteAgentService:
    def __init__(
        self,
        runs: RemoteRunStorage,
        bus,
        *,
        callback_base_url: str | None = None,
        http: httpx.AsyncClient | None = None,
        config=None,
    ):
        if callback_base_url and not valid_endpoint(
            callback_base_url, {"http", "https"}
        ):
            raise ValueError("远程专家回包地址必须为 HTTP(S) URL")
        self.runs, self.bus = runs, bus
        self.callback_base_url, self.http = callback_base_url, http
        self.config = config

    @property
    def settings(self) -> RemoteConfig:
        return (
            self.config.snapshot.agent.remote
            if self.config is not None
            else RemoteConfig()
        )

    def client(
        self,
        connection_id: str,
        config: RemoteAgentConfig,
        *,
        settings: RemoteConfig | None = None,
    ) -> "RemoteAgentBase":
        implementation = {
            "http_sync": SyncHTTPRemoteAgent,
            "http_callback": CallbackHTTPRemoteAgent,
            "websocket": WebSocketRemoteAgent,
        }[config.transport]
        settings = settings if settings is not None else self.settings
        return implementation(
            self, connection_id, resolve_connection(config, settings), settings
        )

    async def notify_result(self, run_id):
        try:
            await self.bus.publish(_result_key(run_id), {"run_id": run_id})
        except Exception:
            # The waiting run still observes the committed result on its next read.
            logger.warning("Remote callback wakeup could not be published")

    async def accept_callback(self, run_id: str, headers, body: bytes) -> str:
        connection = headers.get("x-egis-connection", "")
        timestamp = headers.get("x-egis-timestamp", "")
        signature = headers.get("x-egis-signature", "")
        unauthorized = AgentKitError("远程回包认证失败", 401)
        if not timestamp.isascii() or not timestamp.isdecimal() or len(timestamp) > 12:
            raise unauthorized
        callback = self.settings.callback
        if abs(time.time() - int(timestamp)) > callback.max_clock_skew_seconds:
            raise unauthorized
        row = await self.runs.get(run_id)
        if row is None or connection != row["connection_id"]:
            raise unauthorized
        config = RemoteAgentConfig.model_validate(row["connection_config"])
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
            raise AgentKitError("远程回包过大", 413)
        try:
            result = RemoteRunResult.model_validate_json(body)
        except ValidationError:
            raise AgentKitError("远程回包格式错误", 400) from None
        if result.run_id != run_id:
            raise AgentKitError("远程回包的任务标识与路径不一致", 400)
        outcome, _ = await self.runs.finish(result)
        if outcome not in {"accepted", "duplicate"}:
            raise Conflict("远程任务已结束或超时")
        await self.notify_result(run_id)
        return outcome
