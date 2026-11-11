"""用同步 HTTP 和异步回包实现远程专家协议。

核心能力
- 同步 HTTP 直接解析本次响应。
- 异步 HTTP 提交后等待已经认证并保存的回包。
"""

import asyncio
import os
from contextlib import suppress

import httpx
from pydantic import ValidationError

from egis_agentkit.storage.model._remote_agent import (
    RemoteRunResult,
    valid_endpoint,
)

from ._remote_base import (
    CALLBACK_PATH,
    RemoteAgentBase,
    RemoteTransportError,
    _result_key,
)


class _HTTPRemoteAgent(RemoteAgentBase):
    async def _post(self, endpoint, payload):
        async def send(http):
            async with http.stream(
                "POST",
                endpoint,
                json=payload,
                headers=self._headers(),
                follow_redirects=False,
            ) as response:
                body = bytearray()
                async for chunk in response.aiter_bytes():
                    body.extend(chunk)
                    if len(body) > self.settings.callback.max_bytes:
                        raise RemoteTransportError("protocol", "远程专家响应过大")
                if response.status_code >= 500:
                    raise RemoteTransportError(
                        "transport", "远程专家服务暂不可用", retryable=True
                    )
                return response.status_code, bytes(body)

        try:
            if self.service.http is not None:
                return await send(self.service.http)
            async with httpx.AsyncClient(timeout=None, trust_env=False) as http:
                return await send(http)
        except httpx.TransportError:
            raise RemoteTransportError(
                "transport", "无法连接远程专家", retryable=True
            ) from None

    async def cancel(self, run_id: str) -> bool:
        if self.config.cancel_endpoint is None:
            return False
        status, _ = await self._post(self.config.cancel_endpoint, {"run_id": run_id})
        return status in {200, 202, 204}


class SyncHTTPRemoteAgent(_HTTPRemoteAgent):
    async def _execute(self, request, is_submitted, on_progress):
        status, body = await self._retry(
            lambda: self._post(self.config.endpoint, request.model_dump(mode="json"))
        )
        if status != 200 and not 400 <= status < 500:
            raise RemoteTransportError("protocol", "同步远程专家响应状态无效")
        try:
            result = RemoteRunResult.model_validate_json(body)
        except ValidationError:
            raise RemoteTransportError("protocol", "同步远程专家响应格式错误") from None
        if result.run_id != request.run_id or (
            status != 200 and result.status == "completed"
        ):
            raise RemoteTransportError("protocol", "同步远程专家响应与当前任务不匹配")
        return result


class CallbackHTTPRemoteAgent(_HTTPRemoteAgent):
    async def _execute(self, request, is_submitted, on_progress):
        callback = self.settings.callback
        callback_url = self.service.callback_base_url or callback.public_base_url
        if not callback_url and callback.public_base_url_env:
            callback_url = os.environ.get(callback.public_base_url_env)
        if not callback_url or not valid_endpoint(callback_url, {"http", "https"}):
            raise RemoteTransportError("configuration", "未配置远程专家回包地址")
        if (
            len(os.environ.get(self.config.callback_secret_ref or "", "").encode())
            < self.settings.callback.min_secret_bytes
        ):
            raise RemoteTransportError("configuration", "远程专家回包凭证未正确配置")
        run_id = request.run_id
        if not is_submitted:
            payload = {
                **request.model_dump(mode="json"),
                "callback_url": (
                    f"{callback_url.rstrip('/')}{CALLBACK_PATH}/{run_id}/result"
                ),
            }
            status, _ = await self._retry(
                lambda: self._post(self.config.endpoint, payload)
            )
            if status != 202:
                raise RemoteTransportError("protocol", "异步远程专家必须返回 HTTP 202")
            await self.service.runs.mark_submitted(run_id)

        async def receive():
            async for _ in self.service.bus.subscribe(_result_key(run_id)):
                changed.set()

        changed = asyncio.Event()
        listener = asyncio.create_task(receive())
        try:
            while True:
                changed.clear()
                row = await self.service.runs.get(run_id)
                if row["result"] is not None:
                    return RemoteRunResult.model_validate(row["result"])
                # Persisted state also covers publish failures or callbacks that
                # committed before the local subscription finished opening.
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        changed.wait(), timeout=self.settings.callback.poll_seconds
                    )
        finally:
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)
