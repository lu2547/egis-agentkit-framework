"""描述 Agent 的一个不可变配置版本。"""

from typing import Any

from pydantic import Field

from .._base import Model


class AgentDefinitionDetail(Model):
    """由定义 ID 和版本号唯一定位的配置明细。"""

    definition_id: str
    version: int = Field(ge=1)
    agent_name: str
    agent_display_name: str = Field(max_length=128)
    payload: dict[str, Any]
