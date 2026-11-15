"""组合专家团汇总身份和指定版本配置，供管理与运行读取。"""

from typing import Any, Self

from pydantic import Field, model_validator

from .._definition import TeamPayload
from ._definition_summary import TeamDefinitionSummary


class TeamDefinitionRecord(TeamDefinitionSummary):
    """写入用 owner_version 检查并发，读取用 version 表达选中版本。"""

    team_display_name: str = Field(max_length=128)
    version: int = Field(default=1, ge=1)
    payload: dict[str, Any]

    @model_validator(mode="after")
    def validate_payload(self) -> Self:
        self.payload = TeamPayload.model_validate(self.payload).model_dump(mode="json")
        return self
