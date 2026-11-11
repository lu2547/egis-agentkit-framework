"""资源版本、绑定与会话 Skill 激活记录。"""

from typing import Any, Literal

from pydantic import Field

from ._base import Model


class ResourceRef(Model):
    resource_type: Literal["skill", "tool", "mcp"]
    resource_id: str = Field(min_length=1, max_length=128)
    resource_version: str = Field(min_length=1, max_length=128)
    resource_view_type: Literal["private", "system", "shared"] = "system"


class SkillActivation(ResourceRef):
    """JSON-only skill activation stored in native AgentState.middle_context."""

    source_checksum_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    cache_checksum_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
    cache_dir: str = Field(min_length=1)
    skill_name: str = Field(min_length=1)

    def reference(self):
        return {key: getattr(self, key) for key in ResourceRef.model_fields}


class Asset(Model):
    resource_type: Literal["skill", "tool", "mcp"]
    resource_id: str = Field(min_length=1, max_length=128)
    resource_version: str = Field(min_length=1, max_length=128)
    publication_revision: int | None = Field(default=None, gt=0)
    owner_type: Literal["user", "system"]
    owner_id: str
    checksum_sha256: str = Field(default="", pattern=r"^([a-f0-9]{64})?$")
    entrypoint: str | None = None
    is_enabled: bool = True
    payload: dict[str, Any] = Field(default_factory=dict)


def asset_snapshot_matches(asset: dict, pinned: dict) -> bool:
    """Compare the current asset with its frozen snapshot fields."""
    return all(asset.get(key) == value for key, value in pinned.items())
