"""远程专家连接配置和 v1 任务记录。"""

from __future__ import annotations

from typing import Literal, Self

from pydantic import BaseModel, ConfigDict, Field, model_validator


class RemoteModel(BaseModel):
    model_config = ConfigDict(extra="forbid")


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
    def validate_result(self) -> Self:
        if (self.status == "completed") != (self.output is not None):
            raise ValueError("仅已完成结果包含 output")
        if (self.status != "completed") != (self.error is not None):
            raise ValueError("非成功结果必须包含结构化 error")
        return self
