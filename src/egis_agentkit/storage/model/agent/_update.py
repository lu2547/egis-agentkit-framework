"""限定专家更新可提交的配置，不接受身份、类型或归属字段。"""

from typing import Any

from pydantic import Field

from .._base import Model


class AgentUpdate(Model):
    """更新已有 Agent 的配置，版本用于并发修改检查。"""

    agent_name: str = Field(pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    agent_display_name: str = Field(max_length=128)
    payload: dict[str, Any]
    owner_version: int = Field(ge=1)
