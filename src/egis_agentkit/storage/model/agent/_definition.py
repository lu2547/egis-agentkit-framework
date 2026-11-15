"""提供 Agent 编辑与读取使用的定义数据。"""

from __future__ import annotations

from typing import Any, Self

from pydantic import Field, model_validator

from ._definition_summary import AgentDefinitionSummary


class AgentDefinition(AgentDefinitionSummary):
    """组合汇总身份和选中版本的配置，写入以 owner_version 检查并发。"""

    agent_display_name: str = Field(max_length=128)
    version: int = Field(default=1, ge=1)
    payload: dict[str, Any]

    @model_validator(mode="after")
    def validate_payload(self) -> Self:
        from ._payload import AgentPayload

        payload = AgentPayload.model_validate(self.payload)
        self.payload = payload.model_dump(mode="json")
        return self
