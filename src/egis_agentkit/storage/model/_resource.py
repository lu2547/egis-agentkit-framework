"""资源版本、绑定与会话 Skill 激活记录。"""

import re
from typing import Any, Literal

from packaging.version import Version
from pydantic import Field, model_serializer, model_validator

from ._base import Model

MCP_STORAGE_VERSION = "1.0.0"
MCP_MANAGED_FLAG = "managed_connector"
RESOURCE_IDENTIFIER = re.compile(r"[A-Za-z0-9][A-Za-z0-9_-]{0,100}")


def validate_resource_id(identifier):
    """校验可作为目录和 API 参数的资源标识。"""
    if (
        not isinstance(identifier, str)
        or RESOURCE_IDENTIFIER.fullmatch(identifier) is None
    ):
        raise ValueError("Invalid resource identifier")


def is_managed_connector(asset: dict) -> bool:
    """Recognize the system connection at its fixed storage key."""
    return (
        asset["resource_type"] == "mcp"
        and asset["owner_type"] == asset["owner_id"] == "system"
        and asset["resource_version"] == MCP_STORAGE_VERSION
        and asset["payload"].get(MCP_MANAGED_FLAG) is True
    )


def require_skill_publication_revision(revision: int | None) -> int:
    if revision is None or revision < 1:
        raise ValueError("Skill assets require a positive publication revision")
    return revision


def asset_version_order(asset: dict) -> int | Version:
    """同类资产按发布序号排序，工具使用包版本。"""
    kind = asset["resource_type"]
    if kind == "tool":
        return Version(asset["resource_version"])
    if kind == "skill":
        return require_skill_publication_revision(asset["publication_revision"])
    # MCP 以 ID 定位唯一配置，配置修订号仅用于并发写入校验。
    return 0


class ResourceRef(Model):
    resource_type: Literal["skill", "tool", "mcp"]
    resource_id: str = Field(min_length=1, max_length=128)
    resource_version: str | None = Field(None, min_length=1, max_length=128)
    resource_view_type: Literal["private", "system", "shared"] = "system"

    @model_validator(mode="after")
    def validate_version(self):
        if self.resource_type == "mcp":
            if self.resource_version is not None or self.resource_view_type != "system":
                raise ValueError("MCP bindings require a system connector ID without a version")
        elif self.resource_version is None:
            raise ValueError("Skill and tool bindings require an exact version")
        return self

    @model_serializer(mode="wrap")
    def serialize_reference(self, handler):
        data = handler(self)
        if self.resource_type == "mcp":
            data.pop("resource_version", None)
        return data


class SkillActivation(ResourceRef):
    """JSON-only skill activation stored in native AgentState.middle_context."""

    source_checksum_sha256: str = Field(pattern=r"^[a-f0-9]{64}$")
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

    @model_validator(mode="after")
    def require_skill_revision(self):
        if self.resource_type == "skill":
            require_skill_publication_revision(self.publication_revision)
        return self


def asset_snapshot_matches(asset: dict, pinned: dict) -> bool:
    """Compare the current asset with its frozen snapshot fields."""
    return all(asset.get(key) == value for key, value in pinned.items())
