"""根据远程配置选择协议客户端，集中维护连接身份和通信参数。"""

from __future__ import annotations

from hashlib import sha256
from typing import Any

import httpx

from egis_agentkit.client.nacos._settings import RemoteConfig
from egis_agentkit.errors import Forbidden
from egis_agentkit.storage.model.agent._remote_config import RemoteAgentConfig

from ._base import RemoteAgentClientBase
from ._http_callback import CallbackHTTPRemoteAgentClient
from ._http_sync import SyncHTTPRemoteAgentClient
from ._websocket import WebSocketRemoteAgentClient


def build_client(
    connection_id: str,
    config: RemoteAgentConfig,
    settings: RemoteConfig,
    *,
    http: httpx.AsyncClient | None = None,
    callback_base_url: str | None = None,
) -> RemoteAgentClientBase:
    """仅依赖通信配置创建客户端，不访问业务数据库。"""
    if config.transport == "http_sync":
        return SyncHTTPRemoteAgentClient(connection_id, config, settings, http=http)
    if config.transport == "http_callback":
        return CallbackHTTPRemoteAgentClient(
            connection_id,
            config,
            settings,
            http=http,
            callback_base_url=callback_base_url,
        )
    return WebSocketRemoteAgentClient(connection_id, config, settings)


def build_client_for_definition(
    definition: dict[str, Any],
    settings: RemoteConfig,
    *,
    http: httpx.AsyncClient | None = None,
    callback_base_url: str | None = None,
) -> RemoteAgentClientBase:
    """读取远程定义中的连接参数，生成稳定连接身份并创建协议客户端。"""
    if definition["agent_type"] != "remote":
        raise Forbidden("远程客户端需要远程专家定义")
    config = RemoteAgentConfig.model_validate(definition["payload"]["remote_config"])
    connection_id = sha256(
        f"{definition['id']}:{config.model_dump_json()}".encode()
    ).hexdigest()
    return build_client(
        connection_id, config, settings, http=http, callback_base_url=callback_base_url
    )
