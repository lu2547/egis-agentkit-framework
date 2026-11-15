"""约定远程通信，复用认证、请求限额和重试；客户端不访问任务数据库。"""

from __future__ import annotations

import asyncio
import os
import re
from abc import ABC, abstractmethod
from typing import Any, Awaitable, Callable, Literal

import httpx

from egis_agentkit.client.nacos._settings import RemoteConfig
from egis_agentkit.errors import Conflict
from egis_agentkit.storage.model._remote_agent import RemoteRunRequest, RemoteRunResult
from egis_agentkit.storage.model.agent._remote_config import RemoteAgentConfig

CALLBACK_PATH = "/callbacks/remote-agent/runs"
CALLBACK_RESULT_PATH = re.compile(rf"{CALLBACK_PATH}/[a-zA-Z0-9_-]{{1,64}}/result")
Progress = Callable[[str], Awaitable[None]]


class RemoteTransportError(Exception):
    """协议实现标记可否重试的通信错误。"""

    def __init__(self, code: str, message: str, *, retryable: bool = False) -> None:
        super().__init__(message)
        self.code, self.retryable = code, retryable


def build_connection_config(
    config: RemoteAgentConfig, settings: RemoteConfig
) -> RemoteAgentConfig:
    """应用平台默认值，并校验专家覆盖的超时与重试次数。"""
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


def build_failed_result(
    run_id: str,
    status: Literal["failed", "cancelled", "timed_out"],
    code: str,
    message: str,
) -> RemoteRunResult:
    """构造协议约定的非成功结果。"""
    return RemoteRunResult(
        run_id=run_id, status=status, error={"code": code, "message": message}
    )


async def send_http_request(
    client: RemoteAgentClientBase, endpoint: str, payload: dict[str, Any]
) -> tuple[int, bytes]:
    """两种 HTTP 协议共用的有界响应读取，禁止凭据随重定向转发。"""

    async def send(http: httpx.AsyncClient) -> tuple[int, bytes]:
        async with http.stream(
            "POST",
            endpoint,
            json=payload,
            headers=client._get_headers(),
            follow_redirects=False,
        ) as response:
            body = bytearray()
            async for chunk in response.aiter_bytes():
                body.extend(chunk)
                if len(body) > client.settings.callback.max_bytes:
                    raise RemoteTransportError("protocol", "远程专家响应过大")
            if response.status_code >= httpx.codes.INTERNAL_SERVER_ERROR:
                raise RemoteTransportError(
                    "transport", "远程专家服务暂不可用", retryable=True
                )
            return response.status_code, bytes(body)

    try:
        if client.http is not None:
            return await send(client.http)
        async with httpx.AsyncClient(timeout=None, trust_env=False) as http:
            return await send(http)
    except httpx.TransportError:
        raise RemoteTransportError(
            "transport", "无法连接远程专家", retryable=True
        ) from None


async def send_http_cancel(client: RemoteAgentClientBase, run_id: str) -> bool:
    """调用配置中同源的 HTTP 取消端点。"""
    if client.config.cancel_endpoint is None:
        return False
    status, _ = await send_http_request(
        client, client.config.cancel_endpoint, {"run_id": run_id}
    )
    return status in {httpx.codes.OK, httpx.codes.ACCEPTED, httpx.codes.NO_CONTENT}


class RemoteAgentClientBase(ABC):
    """三种远程协议共用的通信契约和公共方法。"""

    http: httpx.AsyncClient | None

    def __init__(
        self, connection_id: str, config: RemoteAgentConfig, settings: RemoteConfig
    ) -> None:
        self.connection_id = connection_id
        self.settings = settings
        self.config = build_connection_config(config, settings)

    def _get_headers(self) -> dict[str, str]:
        """按引用读取请求凭据，拒绝控制字符和空白。"""
        credential = os.environ.get(self.config.credential_ref)
        if (
            not credential
            or not credential.isascii()
            or not credential.isprintable()
            or any(char.isspace() for char in credential)
        ):
            raise RemoteTransportError("configuration", "远程专家未配置有效的执行凭证")
        return {
            "Authorization": f"Bearer {credential}",
            "X-Egis-Connection": self.connection_id,
            "X-Egis-Protocol": self.config.protocol_version,
        }

    async def send_request(
        self, request: RemoteRunRequest, on_progress: Progress | None = None
    ) -> RemoteRunResult | None:
        """完成有大小和时限约束的协议调用，异步提交成功时返回 None。"""
        if len(request.model_dump_json().encode()) > self.settings.request.max_bytes:
            return build_failed_result(
                request.run_id, "failed", "request_size", "远程专家请求过大"
            )
        try:
            async with asyncio.timeout(self.config.timeout_seconds):
                return await self._retry(
                    lambda: self._send_request(request, on_progress)
                )
        except TimeoutError:
            return build_failed_result(
                request.run_id, "timed_out", "timeout", "远程专家未在配置时限内返回"
            )
        except RemoteTransportError as exc:
            return build_failed_result(request.run_id, "failed", exc.code, str(exc))

    async def _retry(
        self, send_request: Callable[[], Awaitable[RemoteRunResult | None]]
    ) -> RemoteRunResult | None:
        """仅重试协议实现明确标记为可重试的通信错误。"""
        for attempt in range(self.config.retries + 1):
            try:
                return await send_request()
            except RemoteTransportError as exc:
                if not exc.retryable or attempt == self.config.retries:
                    raise
                await asyncio.sleep(self.settings.retry.backoff_seconds * (attempt + 1))

    @abstractmethod
    async def _send_request(
        self, request: RemoteRunRequest, on_progress: Progress | None
    ) -> RemoteRunResult | None:
        """发送协议请求；异步接受请求后返回 None。"""

    @abstractmethod
    async def send_cancel_request(self, run_id: str) -> bool:
        """返回远端是否确认取消。"""
