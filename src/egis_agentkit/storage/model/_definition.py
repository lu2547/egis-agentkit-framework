"""Agent 和 Team 的定义配置与引用。"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import Field

from ._base import Model

ViewType = Literal["private", "system", "shared"]
AgentType = Literal["personal", "expert", "remote", "orchestrator"]
EXPERT_TYPES = ("expert", "remote")


class DefinitionRef(Model):
    """已解析到固定版本的 Agent 引用。"""

    definition_id: str
    resource_view_type: ViewType
    resource_version: str = Field(pattern=r"^[1-9][0-9]{0,18}$")


class TeamPayload(Model):
    description: str = Field(default="", max_length=2000)
    category: str = Field(default="办公协同", max_length=64)
    tags: list[str] = Field(default_factory=list, max_length=20)
    avatar: str = Field(default="team", max_length=64)
    starter_prompts: list[str] = Field(default_factory=list, max_length=10)
    orchestrator: DefinitionRef
    members: list[DefinitionRef] = Field(min_length=1, max_length=20)


def build_definition_reference(definition: dict[str, Any]) -> dict[str, Any]:
    """从定义提取会话需要的业务身份、版本和授权来源。"""
    return {
        "definition_id": definition["id"],
        "root_resource_type": definition["definition_type"],
        "resource_view_type": definition["resource_view_type"],
        "resource_version": definition["resource_version"],
        **{
            field: definition[field]
            for field in ("shared_review_id", "access_grant_id")
            if definition.get(field)
        },
    }


def build_definition_options(reference: dict[str, Any]) -> dict[str, Any]:
    """从已保存的会话引用还原读取和权限查询参数。"""
    return {
        "version": reference["resource_version"],
        "review_id": reference.get("shared_review_id"),
        "grant_id": reference.get("access_grant_id"),
        "allow_pinned": True,
    }
