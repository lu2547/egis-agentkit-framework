"""远程专家随定义版本保存的连接配置。"""

from __future__ import annotations

from typing import Literal, Self
from urllib.parse import urlsplit

from pydantic import Field, model_validator

from .._base import Model


class RemoteAgentConfig(Model):
    transport: Literal["http_sync", "http_callback", "websocket"]
    protocol_version: Literal["v1"] = "v1"
    endpoint: str = Field(min_length=1, max_length=2048)
    credential_ref: str = Field(pattern=r"^EGIS_REMOTE_AGENT_[A-Z][A-Z0-9_]{0,109}$")
    callback_secret_ref: str | None = Field(
        default=None, pattern=r"^EGIS_REMOTE_AGENT_[A-Z][A-Z0-9_]{0,109}$"
    )
    cancel_endpoint: str | None = Field(default=None, max_length=2048)
    timeout_seconds: float | None = Field(default=None, gt=0)
    retries: int | None = Field(default=None, ge=0)

    @model_validator(mode="after")
    def validate_connection(self) -> Self:
        websocket = self.transport == "websocket"
        schemes = {"ws", "wss"} if websocket else {"http", "https"}
        if not validate_endpoint(self.endpoint, schemes):
            raise ValueError("远程端点必须包含有效的协议和主机")
        if (self.transport == "http_callback") != bool(self.callback_secret_ref):
            raise ValueError("仅 HTTP 回调模式需要回包签名凭证引用")
        if self.callback_secret_ref == self.credential_ref:
            raise ValueError("远程请求与回包签名必须使用独立凭证")
        if self.cancel_endpoint is not None and (
            websocket or not validate_endpoint(self.cancel_endpoint, schemes)
        ):
            raise ValueError("HTTP 取消端点必须包含有效的协议和主机")
        if self.cancel_endpoint and (
            urlsplit(self.cancel_endpoint).netloc != urlsplit(self.endpoint).netloc
            or urlsplit(self.cancel_endpoint).scheme != urlsplit(self.endpoint).scheme
        ):
            raise ValueError("HTTP 取消端点须与执行端点同源")
        return self


def validate_endpoint(endpoint: str, schemes: set[str]) -> bool:
    try:
        url = urlsplit(endpoint)
        port = url.port
    except ValueError:
        return False
    return (
        url.scheme in schemes
        and bool(url.hostname)
        and url.username is None
        and url.password is None
        and not url.fragment
        and not url.query
        and (port is None or port > 0)
        and not any(character.isspace() for character in endpoint)
    )
