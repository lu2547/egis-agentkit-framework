"""描述专家团稳定身份、归属和当前版本指针。"""

from datetime import datetime
from typing import Literal

from pydantic import Field

from .._base import Model, new_id


class TeamDefinitionSummary(Model):
    """每个专家团唯一的汇总记录，版本和删除语义与 Agent 一致。"""

    id: str = Field(default_factory=new_id, max_length=64)
    definition_type: Literal["team"] = "team"
    owner_type: Literal["user", "system"] = "user"
    owner_id: str = Field(min_length=1, max_length=128)
    team_name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    owner_version: int = Field(default=1, ge=1)
    shared_version: int | None = Field(default=None, ge=1)
    deleted_at: datetime | None = None
