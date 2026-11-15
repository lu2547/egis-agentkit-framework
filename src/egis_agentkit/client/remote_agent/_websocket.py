"""用 WebSocket 实现双向远程专家协议。

核心能力
- 在同一连接发送请求、接收进度和结果。
- 取消沿现有连接发送，重连沿用原请求标识。
"""

from __future__ import annotations

import asyncio
import json

from pydantic import ValidationError
from websockets.asyncio.client import ClientConnection, connect
from websockets.exceptions import WebSocketException

from egis_agentkit.client.nacos._settings import RemoteConfig
from egis_agentkit.client.remote_agent._base import (
    RemoteAgentClientBase,
    RemoteTransportError,
)
from egis_agentkit.storage.model._remote_agent import (
    RemoteRunRequest,
    RemoteRunResult,
)
from egis_agentkit.storage.model.agent._remote_config import RemoteAgentConfig

from ._base import Progress


class _EndpointWebSocket(connect):
    """Keep credentials bound to the registered endpoint through the handshake."""

    def process_redirect(self, exc: Exception) -> Exception:
        return exc


class WebSocketRemoteAgentClient(RemoteAgentClientBase):
    def __init__(
        self, connection_id: str, config: RemoteAgentConfig, settings: RemoteConfig
    ) -> None:
        super().__init__(connection_id, config, settings)
        self._connections: dict[str, tuple[ClientConnection, asyncio.Event]] = {}

    async def _send_request(
        self, request: RemoteRunRequest, on_progress: Progress | None
    ) -> RemoteRunResult:
        return await self._exchange(request, on_progress)

    async def _exchange(
        self, request: RemoteRunRequest, on_progress: Progress | None
    ) -> RemoteRunResult:
        run_id = request.run_id
        try:
            async with _EndpointWebSocket(
                self.config.endpoint,
                additional_headers=self._get_headers(),
                max_size=self.settings.callback.max_bytes,
                open_timeout=None,
                close_timeout=self.settings.timeout.websocket_close_seconds,
                proxy=None,
            ) as socket:
                cancelled = asyncio.Event()
                self._connections[run_id] = socket, cancelled
                await socket.send(
                    json.dumps({"event": "run.start", **request.model_dump()})
                )
                while True:
                    try:
                        event = json.loads(await socket.recv())
                    except (ValueError, UnicodeDecodeError):
                        raise RemoteTransportError(
                            "protocol", "远程专家事件格式错误"
                        ) from None
                    if not isinstance(event, dict) or event.get("run_id") != run_id:
                        raise RemoteTransportError(
                            "protocol", "远程专家事件与当前任务不匹配"
                        )
                    event_name = event.pop("event", None)
                    if not isinstance(event_name, str):
                        raise RemoteTransportError("protocol", "远程专家事件类型无效")
                    if event_name == "run.progress":
                        progress = event.get("message")
                        if (
                            not isinstance(progress, str)
                            or not 0
                            < len(progress)
                            <= self.settings.request.max_progress_characters
                        ):
                            raise RemoteTransportError(
                                "protocol", "远程专家进度格式错误"
                            )
                        if on_progress is not None:
                            await on_progress(progress)
                        continue
                    status = {
                        "run.completed": "completed",
                        "run.failed": "failed",
                        "run.cancelled": "cancelled",
                    }.get(event_name)
                    if status is None or event.get("status", status) != status:
                        raise RemoteTransportError("protocol", "远程专家事件类型无效")
                    try:
                        result = RemoteRunResult.model_validate(
                            {**event, "status": status}
                        )
                    except ValidationError:
                        raise RemoteTransportError(
                            "protocol", "远程专家结果格式错误"
                        ) from None
                    if status == "cancelled":
                        cancelled.set()
                    return result
        except (OSError, WebSocketException):
            raise RemoteTransportError(
                "transport", "远程专家 WebSocket 连接中断", retryable=True
            ) from None
        finally:
            self._connections.pop(run_id, None)

    async def send_cancel_request(self, run_id: str) -> bool:
        connection = self._connections.get(run_id)
        if connection is None:
            return False
        socket, cancelled = connection
        try:
            await socket.send(json.dumps({"event": "run.cancel", "run_id": run_id}))
            async with asyncio.timeout(self.settings.timeout.stop_seconds):
                await cancelled.wait()
        except (OSError, WebSocketException, TimeoutError):
            return False
        return True
