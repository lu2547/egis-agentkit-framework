"""Agent 和 Team 的定义配置与引用。"""

from typing import Any, Literal

from pydantic import Field, model_validator

from ._base import Model, new_id
from ._remote_agent import RemoteAgentConfig
from ._resource import ResourceRef

ViewType = Literal["private", "system", "shared"]
AgentType = Literal["personal", "expert", "remote", "orchestrator"]
EXPERT_TYPES = ("expert", "remote")


class DefinitionRef(Model):
    """已解析到固定版本的 Agent 引用。"""

    definition_id: str
    resource_view_type: ViewType
    resource_version: str = Field(pattern=r"^[1-9][0-9]{0,18}$")


class AgentPayload(Model):
    description: str = Field(default="", max_length=2000)
    category: str = Field(default="办公协同", max_length=64)
    tags: list[str] = Field(default_factory=list, max_length=20)
    avatar: str = Field(default="spark", max_length=64)
    system_prompt: str = Field(
        default="你是一个认真、可靠的企业工作助理。", max_length=64000
    )
    starter_prompts: list[str] = Field(default_factory=list, max_length=10)
    model_key: str | None = None
    bindings: list[ResourceRef] = Field(default_factory=list, max_length=100)
    remote_config: RemoteAgentConfig | None = None


class TeamPayload(Model):
    description: str = Field(default="", max_length=2000)
    category: str = Field(default="办公协同", max_length=64)
    tags: list[str] = Field(default_factory=list, max_length=20)
    avatar: str = Field(default="team", max_length=64)
    starter_prompts: list[str] = Field(default_factory=list, max_length=10)
    orchestrator: DefinitionRef
    members: list[DefinitionRef] = Field(min_length=1, max_length=20)


class Definition(Model):
    id: str = Field(default_factory=new_id, max_length=64)
    definition_type: Literal["agent", "team"] = "agent"
    owner_type: Literal["user", "system"] = "user"
    owner_id: str = Field(min_length=1, max_length=128)
    agent_type: AgentType | None = "expert"
    agent_name: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    agent_display_name: str | None = Field(default=None, max_length=128)
    team_name: str | None = Field(default=None, pattern=r"^[a-z][a-z0-9_-]{0,127}$")
    team_display_name: str | None = Field(default=None, max_length=128)
    is_enabled: bool = True
    config_version: int = Field(default=1, ge=1)
    payload: dict[str, Any]

    @model_validator(mode="after")
    def validate_payload(self) -> "Definition":
        if self.definition_type == "agent":
            if (
                not self.agent_name
                or not self.agent_display_name
                or not self.agent_type
            ):
                raise ValueError("Agent 必须填写英文名、展示名和类型")
            if self.team_name is not None or self.team_display_name is not None:
                raise ValueError("Agent 不能包含 Team 名称")
            agent_payload = AgentPayload.model_validate(self.payload)
            if (agent_payload.remote_config is not None) != (self.agent_type == "remote"):
                raise ValueError("仅远程专家需要配置远程连接")
            self.payload = agent_payload.model_dump(mode="json")
        else:
            if (
                not self.team_name
                or not self.team_display_name
                or self.agent_type is not None
            ):
                raise ValueError("Team 必须填写英文名、展示名，agent_type 必须为空")
            if self.agent_name is not None or self.agent_display_name is not None:
                raise ValueError("Team 不能包含 Agent 名称")
            self.payload = TeamPayload.model_validate(self.payload).model_dump(
                mode="json"
            )
        return self


def definition_reference(row: dict) -> dict:
    """从定义提取会话需要的业务身份、版本和授权来源。"""
    return {
        "definition_id": row["id"],
        "root_resource_type": row["definition_type"],
        "resource_view_type": row["resource_view_type"],
        "resource_version": row["resource_version"],
        **{
            field: row[field]
            for field in ("shared_review_id", "access_grant_id")
            if row.get(field)
        },
    }


def definition_options(reference: dict) -> dict:
    """从已保存的会话引用还原读取和权限查询参数。"""
    return {
        "version": reference["resource_version"],
        "review_id": reference.get("shared_review_id"),
        "grant_id": reference.get("access_grant_id"),
        "allow_pinned": True,
    }
