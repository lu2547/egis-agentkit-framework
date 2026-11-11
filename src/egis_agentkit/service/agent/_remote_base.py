"""约定远程专家协议并统一处理超时、取消、重试与持久化结果。

核心能力
- 统一请求标识、超时、失败、取消和重试。
- 把传输和取消交给协议实现，把持久化结果交给 Storage。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
import os
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import TYPE_CHECKING, Awaitable, Callable

from egis_agentkit.backend.nacos._settings import RemoteConfig
from egis_agentkit.storage.model._remote_agent import (
    RemoteAgentConfig,
    RemoteRunRequest,
    RemoteRunResult,
)

if TYPE_CHECKING:
    from ._remote import RemoteAgentService

logger = logging.getLogger(__name__)

CALLBACK_PATH = "/callbacks/remote-agent/runs"

Progress = Callable[[str], Awaitable[None]]


class RemoteTransportError(Exception):
    def __init__(self, code: str, message: str, *, retryable: bool = False):
        super().__init__(message)
        self.code, self.retryable = code, retryable


def _result(run_id, status, code, message):
    return RemoteRunResult(
        run_id=run_id, status=status, error={"code": code, "message": message}
    )


def _result_key(run_id):
    return f"ak:remote-result:{run_id}"


class RemoteAgentBase(ABC):
    def __init__(
        self,
        service: RemoteAgentService,
        connection_id: str,
        config: RemoteAgentConfig,
        settings: RemoteConfig,
    ):
        self.service, self.connection_id, self.config = service, connection_id, config
        self.settings = settings

    def _headers(self) -> dict[str, str]:
        credential = os.environ.get(self.config.credential_ref)
        if not credential or any(
            not 32 < ord(character) < 127 for character in credential
        ):
            raise RemoteTransportError("configuration", "远程专家未配置有效的执行凭证")
        return {
            "Authorization": f"Bearer {credential}",
            "X-Egis-Connection": self.connection_id,
            "X-Egis-Protocol": self.config.protocol_version,
        }

    async def run(
        self, request: RemoteRunRequest, on_progress: Progress | None = None
    ) -> RemoteRunResult:
        run_id = request.run_id
        content = request.model_dump_json().encode()
        if len(content) > self.settings.request.max_bytes:
            return _result(run_id, "failed", "request_size", "远程专家请求过大")
        digest = hashlib.sha256(content).hexdigest()
        runs = self.service.runs
        row = await runs.begin(request, self.connection_id, self.config, digest)
        if row["result"] is not None:
            return RemoteRunResult.model_validate(row["result"])
        remaining = (row["deadline"] - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            _, result = await runs.finish(
                _result(run_id, "timed_out", "timeout", "远程专家未在配置时限内返回")
            )
            return result

        execution = asyncio.create_task(
            self._execute(request, row["is_submitted"], on_progress)
        )
        try:
            async with asyncio.timeout(remaining):
                result = await asyncio.shield(execution)
        except TimeoutError:
            result = _result(
                run_id, "timed_out", "timeout", "远程专家未在配置时限内返回"
            )
        except asyncio.CancelledError:
            try:
                await runs.finish(
                    _result(run_id, "cancelled", "cancelled", "远程专家任务已停止")
                )
                await self._stop_remote(run_id)
            finally:
                raise
        except RemoteTransportError as exc:
            result = _result(run_id, "failed", exc.code, str(exc))
        finally:
            if not execution.done():
                execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
        _, stored = await runs.finish(result)
        return stored

    async def _stop_remote(self, run_id):
        try:
            async with asyncio.timeout(self.settings.timeout.cancel_seconds):
                await self.cancel(run_id)
        except (TimeoutError, RemoteTransportError):
            logger.info("Remote cancellation could not be acknowledged")

    async def _retry(self, action):
        for attempt in range(self.config.retries + 1):
            try:
                return await action()
            except RemoteTransportError as exc:
                if not exc.retryable or attempt == self.config.retries:
                    raise
                await asyncio.sleep(self.settings.retry.backoff_seconds * (attempt + 1))

    @abstractmethod
    async def _execute(self, request, is_submitted, on_progress) -> RemoteRunResult: ...

    @abstractmethod
    async def cancel(self, run_id: str) -> bool:
        """Return whether the provider accepted the remote cancellation."""
