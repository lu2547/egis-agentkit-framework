"""描述 Agent 的稳定身份、归属和当前版本指针。"""

from datetime import datetime
from typing import Literal

from pydantic import Field

from .._base import Model, new_id


class AgentDefinitionSummary(Model):
    """每个 Agent 唯一的汇总记录。"""

    id: str = Field(default_factory=new_id, max_length=64)
    definition_type: Literal["agent"] = "agent"
    owner_type: Literal["user", "system"] = "user"
    owner_id: str = Field(min_length=1, max_length=128)
    agent_type: Literal["personal", "expert", "remote", "orchestrator"] = "expert"
    agent_name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    owner_version: int = Field(default=1, ge=1)
    shared_version: int | None = Field(default=None, ge=1)
    deleted_at: datetime | None = None
