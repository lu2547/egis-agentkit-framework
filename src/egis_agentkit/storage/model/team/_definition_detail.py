"""描述专家团的一份不可变版本配置。"""

from typing import Any

from pydantic import Field

from .._base import Model


class TeamDefinitionDetail(Model):
    """定义 ID 和版本号唯一定位名称、主理人及成员配置。"""

    definition_id: str
    version: int = Field(ge=1)
    team_name: str
    team_display_name: str = Field(max_length=128)
    payload: dict[str, Any]
