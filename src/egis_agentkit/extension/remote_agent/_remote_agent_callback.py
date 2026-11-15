"""为原生远程调用接入提供任务恢复、异步回包和持久化回调。

核心能力
- 创建独立的协议 Client，复用原生运行接入持有的依赖。
- 在调用边界恢复任务、等待已认证回包并提交结果。
- 回包入口先验证协议，再交给 Storage 去重并通知当前等待者。
"""

from __future__ import annotations

import asyncio
import hashlib
import logging
from contextlib import suppress
from datetime import datetime, timezone
from typing import Any, Mapping

import httpx

from egis_agentkit.client.nacos import NacosConfigProvider
from egis_agentkit.client.nacos._settings import AgentKitConfig, RemoteConfig
from egis_agentkit.client.remote_agent._base import (
    Progress,
    RemoteAgentClientBase,
    RemoteTransportError,
    build_failed_result,
)
from egis_agentkit.client.remote_agent._factory import (
    build_client,
    build_client_for_definition,
)
from egis_agentkit.client.remote_agent._http_callback import (
    CallbackHTTPRemoteAgentClient,
)
from egis_agentkit.core.message_bus import EgisMessageBus
from egis_agentkit.errors import AgentKitError, Conflict
from egis_agentkit.storage._remote_agent import RemoteRunStorage
from egis_agentkit.storage.model._remote_agent import RemoteRunRequest, RemoteRunResult
from egis_agentkit.storage.model.agent._remote_config import (
    RemoteAgentConfig,
    validate_endpoint,
)

logger = logging.getLogger(__name__)


def _build_result_key(run_id: str) -> str:
    return f"ak:remote-result:{run_id}"


class RemoteAgentRuntime:
    """将通信客户端的结果接回可恢复的原生会话运行。"""

    def __init__(
        self,
        runs: RemoteRunStorage,
        bus: EgisMessageBus,
        *,
        callback_base_url: str | None = None,
        http: httpx.AsyncClient | None = None,
        config: NacosConfigProvider | None = None,
    ) -> None:
        if callback_base_url and not validate_endpoint(
            callback_base_url, {"http", "https"}
        ):
            raise ValueError("远程专家回包地址必须为 HTTP(S) URL")
        self.runs, self.bus = runs, bus
        self.callback_base_url, self.http, self.config = callback_base_url, http, config

    @property
    def settings(self) -> RemoteConfig:
        return (
            self.config.snapshot.agent.remote
            if self.config is not None
            else RemoteConfig()
        )

    def get_client(
        self,
        connection_id: str,
        config: RemoteAgentConfig,
        *,
        settings: RemoteConfig | None = None,
    ) -> RemoteAgentClientBase:
        """按固定定义选择协议，客户端只获得通信配置。"""
        settings = settings if settings is not None else self.settings
        return build_client(
            connection_id,
            config,
            settings,
            http=self.http,
            callback_base_url=self.callback_base_url,
        )

    def get_client_for_definition(
        self, definition: dict[str, Any], *, configuration: AgentKitConfig | None = None
    ) -> RemoteAgentClientBase:
        """将客户端创建交给 Client 层，运行接入只提供本轮配置和回包地址。"""
        settings = (
            configuration.agent.remote if configuration is not None else self.settings
        )
        return build_client_for_definition(
            definition,
            settings,
            http=self.http,
            callback_base_url=self.callback_base_url,
        )

    async def send_request(
        self,
        client: RemoteAgentClientBase,
        request: RemoteRunRequest,
        on_progress: Progress | None = None,
    ) -> RemoteRunResult:
        """恢复已有结果，或在原任务时限内执行通信并保存终态。"""
        run_id = request.run_id
        content = request.model_dump_json().encode()
        if len(content) > client.settings.request.max_bytes:
            return build_failed_result(
                run_id, "failed", "request_size", "远程专家请求过大"
            )
        row = await self.runs.add_request(
            request,
            client.connection_id,
            client.config,
            hashlib.sha256(content).hexdigest(),
        )
        if row["result"] is not None:
            return RemoteRunResult.model_validate(row["result"])
        remaining = (row["deadline"] - datetime.now(timezone.utc)).total_seconds()
        if remaining <= 0:
            _, result = await self.runs.save_result(
                build_failed_result(
                    run_id, "timed_out", "timeout", "远程专家未在配置时限内返回"
                )
            )
            return result

        async def execute() -> RemoteRunResult:
            if client.config.transport == "http_callback" and row["is_submitted"]:
                return await self._get_result(run_id, client.settings)
            result = await client.send_request(request, on_progress)
            if result is None:
                await self.runs.update_submitted(run_id)
                return await self._get_result(run_id, client.settings)
            return result

        execution = asyncio.create_task(execute())
        try:
            async with asyncio.timeout(remaining):
                result = await asyncio.shield(execution)
        except TimeoutError:
            result = build_failed_result(
                run_id, "timed_out", "timeout", "远程专家未在配置时限内返回"
            )
        except asyncio.CancelledError:
            try:
                await self.runs.save_result(
                    build_failed_result(
                        run_id, "cancelled", "cancelled", "远程专家任务已停止"
                    )
                )
                await self._send_cancel(client, run_id)
            finally:
                raise
        finally:
            if not execution.done():
                execution.cancel()
            await asyncio.gather(execution, return_exceptions=True)
        _, stored = await self.runs.save_result(result)
        return stored

    async def _send_cancel(self, client: RemoteAgentClientBase, run_id: str) -> None:
        """取消原生运行时尝试通知第三方，远端不响应也能结束本地等待。"""
        try:
            async with asyncio.timeout(client.settings.timeout.cancel_seconds):
                await client.send_cancel_request(run_id)
        except (TimeoutError, RemoteTransportError):
            logger.info("Remote cancellation could not be acknowledged")

    async def _get_result(self, run_id: str, settings: RemoteConfig) -> RemoteRunResult:
        """订阅通知并复查持久化结果，覆盖订阅前已到达的回包。"""
        changed = asyncio.Event()

        async def receive() -> None:
            async for _ in self.bus.subscribe(_build_result_key(run_id)):
                changed.set()

        listener = asyncio.create_task(receive())
        try:
            while True:
                changed.clear()
                row = await self.runs.get_request(run_id)
                if row["result"] is not None:
                    return RemoteRunResult.model_validate(row["result"])
                with suppress(TimeoutError):
                    await asyncio.wait_for(
                        changed.wait(), timeout=settings.callback.poll_seconds
                    )
        finally:
            listener.cancel()
            await asyncio.gather(listener, return_exceptions=True)

    async def save_callback_result(
        self, run_id: str, headers: Mapping[str, str], body: bytes
    ) -> str:
        """校验回包后原子落库，重复回包不改变已有结果。"""
        row = await self.runs.get_request(run_id)
        if row is None:
            raise AgentKitError("远程回包认证失败", 401)
        config = RemoteAgentConfig.model_validate(row["connection_config"])
        result = CallbackHTTPRemoteAgentClient.validate_callback(
            run_id, row["connection_id"], config, self.settings, headers, body
        )
        outcome, _ = await self.runs.save_result(result)
        if outcome not in {"accepted", "duplicate"}:
            raise Conflict("远程任务已结束或超时")
        try:
            await self.bus.publish(_build_result_key(run_id), {"run_id": run_id})
        except Exception:
            logger.warning("Remote callback wakeup could not be published")
        return outcome
