"""通过同步 HTTP/HTTPS 请求取得远程专家结果。"""

from __future__ import annotations

import httpx
from pydantic import ValidationError

from egis_agentkit.client.nacos._settings import RemoteConfig
from egis_agentkit.storage.model._remote_agent import RemoteRunRequest, RemoteRunResult
from egis_agentkit.storage.model.agent._remote_config import RemoteAgentConfig

from ._base import (
    Progress,
    RemoteAgentClientBase,
    RemoteTransportError,
    send_http_cancel,
    send_http_request,
)


class SyncHTTPRemoteAgentClient(RemoteAgentClientBase):
    """只负责同步 HTTP 协议，不持有任务存储。"""

    def __init__(
        self,
        connection_id: str,
        config: RemoteAgentConfig,
        settings: RemoteConfig,
        *,
        http: httpx.AsyncClient | None = None,
    ) -> None:
        super().__init__(connection_id, config, settings)
        self.http = http

    async def _send_request(
        self, request: RemoteRunRequest, on_progress: Progress | None
    ) -> RemoteRunResult:
        status, body = await send_http_request(
            self, self.config.endpoint, request.model_dump(mode="json")
        )
        if status != httpx.codes.OK and not httpx.codes.is_client_error(status):
            raise RemoteTransportError("protocol", "同步远程专家响应状态无效")
        try:
            result = RemoteRunResult.model_validate_json(body)
        except ValidationError:
            raise RemoteTransportError("protocol", "同步远程专家响应格式错误") from None
        if result.run_id != request.run_id or (
            status != httpx.codes.OK and result.status == "completed"
        ):
            raise RemoteTransportError("protocol", "同步远程专家响应与当前任务不匹配")
        return result

    async def send_cancel_request(self, run_id: str) -> bool:
        return await send_http_cancel(self, run_id)
