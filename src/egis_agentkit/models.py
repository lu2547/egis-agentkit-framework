"""Enterprise facts only. Native Agent/Session/Skill records stay native."""

from datetime import datetime, timezone
from typing import Any, Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, model_validator


def new_id() -> str:
    return uuid4().hex


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


class User(Model):
    user_id: str = Field(min_length=1, max_length=128, pattern=r"^[\w.@-]+$")
    username: str = Field(min_length=1, max_length=128)
    group_ids: list[str] = Field(default_factory=list, max_length=100)
    role_type: Literal["user", "admin"] = "user"
    is_enabled: bool = True


class Group(Model):
    group_id: str = Field(min_length=1, max_length=128, pattern=r"^[\w.-]+$")
    group_name: str = Field(min_length=1, max_length=128)
    is_enabled: bool = True


ViewType = Literal["private", "system", "shared"]


class DefinitionRef(Model):
    definition_id: str
    resource_view_type: ViewType


class ResourceRef(Model):
    resource_type: Literal["skill", "tool", "mcp", "middleware"]
    resource_id: str = Field(min_length=1, max_length=128)
    resource_version: str = Field(min_length=1, max_length=128)
    resource_view_type: ViewType = "system"


class Implementation(Model):
    assembly_ref: str
    assembly_version: str


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
    implementation: Implementation | None = None


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
    agent_type: Literal["personal", "expert", "orchestrator"] | None = "expert"
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
            self.payload = AgentPayload.model_validate(self.payload).model_dump(
                mode="json"
            )
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


class Asset(Model):
    resource_type: Literal["assembly", "skill", "tool", "middleware", "mcp"]
    resource_id: str = Field(min_length=1, max_length=128)
    resource_version: str = Field(min_length=1, max_length=128)
    owner_type: Literal["user", "system"]
    owner_id: str
    download_url: str = ""
    checksum_sha256: str = Field(default="", pattern=r"^([a-f0-9]{64})?$")
    entrypoint: str | None = None
    is_enabled: bool = True
    payload: dict[str, Any] = Field(default_factory=dict)


class Grant(Model):
    id: str = Field(default_factory=new_id)
    principal_type: Literal["user", "group", "all_users"]
    principal_id: str
    resource_type: Literal["team", "agent", "skill", "tool", "mcp", "middleware"]
    resource_id: str
    resource_view_type: Literal["system", "shared"]
    resource_owner_type: Literal["user", "system"]
    resource_owner_id: str
    grant_source_type: Literal["manual", "share"] = "manual"
    source_review_id: str | None = None
    revoked_at: datetime | None = None


class ShareReview(Model):
    review_id: str = Field(default_factory=new_id)
    resource_type: Literal["agent", "team", "skill", "mcp"]
    resource_id: str
    owner_id: str
    review_status_type: Literal["pending", "approved", "rejected", "cancelled"] = (
        "pending"
    )
    is_current: bool = False
    shared_version: int = 0
    share_scope_type: Literal["all_users"] = "all_users"
    source_versions: dict[str, int | str]
    snapshot: dict[str, Any]
    submitted_by: str
    reviewed_by: str | None = None
    review_reason: str = ""
    submitted_at: datetime = Field(default_factory=utcnow)
    reviewed_at: datetime | None = None
