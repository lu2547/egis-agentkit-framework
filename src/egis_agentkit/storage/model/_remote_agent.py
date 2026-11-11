"""远程专家连接配置和 v1 任务记录。"""

from typing import Literal
from urllib.parse import urlsplit

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RemoteModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


class RemoteAgentConfig(RemoteModel):
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
    def validate_connection(self):
        websocket = self.transport == "websocket"
        schemes = {"ws", "wss"} if websocket else {"http", "https"}
        if not valid_endpoint(self.endpoint, schemes):
            raise ValueError("远程端点必须包含有效的协议和主机")
        if (self.transport == "http_callback") != bool(self.callback_secret_ref):
            raise ValueError("仅 HTTP 回调模式需要回包签名凭证引用")
        if self.callback_secret_ref == self.credential_ref:
            raise ValueError("远程请求与回包签名必须使用独立凭证")
        if self.cancel_endpoint is not None and (
            websocket or not valid_endpoint(self.cancel_endpoint, schemes)
        ):
            raise ValueError("HTTP 取消端点必须包含有效的协议和主机")
        if self.cancel_endpoint and (
            urlsplit(self.cancel_endpoint).netloc != urlsplit(self.endpoint).netloc
            or urlsplit(self.cancel_endpoint).scheme != urlsplit(self.endpoint).scheme
        ):
            raise ValueError("HTTP 取消端点须与执行端点同源")
        return self


def valid_endpoint(endpoint, schemes):
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


class RemoteText(RemoteModel):
    text: str = Field(min_length=1, max_length=128_000)


class RemoteAttachmentRef(RemoteModel):
    ref: str = Field(min_length=1, max_length=2048)
    name: str = Field(min_length=1, max_length=255)
    media_type: str = Field(min_length=1, max_length=128)


class RemoteRunRequest(RemoteModel):
    run_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    conversation_id: str = Field(min_length=1, max_length=128)
    input: RemoteText
    attachments: list[RemoteAttachmentRef] = Field(default_factory=list, max_length=20)


class RemoteRunError(RemoteModel):
    code: str = Field(min_length=1, max_length=64)
    message: str = Field(min_length=1, max_length=2000)


class RemoteRunResult(RemoteModel):
    run_id: str = Field(min_length=1, max_length=64, pattern=r"^[a-zA-Z0-9_-]+$")
    status: Literal["completed", "failed", "cancelled", "timed_out"]
    output: RemoteText | None = None
    error: RemoteRunError | None = None

    @model_validator(mode="after")
    def validate_result(self):
        if (self.status == "completed") != (self.output is not None):
            raise ValueError("仅已完成结果包含 output")
        if (self.status != "completed") != (self.error is not None):
            raise ValueError("非成功结果必须包含结构化 error")
        return self
