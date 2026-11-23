"""Pure authorization decisions and resource classification shared by readers."""

from dataclasses import dataclass, field

from egis_agentkit.storage.model._resource import SYSTEM_DEFAULT_TOOL_SOURCES

DEFINITION_KINDS = ("agent", "team")
INTERNAL_AGENT_TYPES = ("personal", "orchestrator")


def is_system_default_tool(asset):
    payload = asset["payload"]
    return (
        asset.get("resource_type") == "tool"
        and asset["owner_type"] == "system"
        and payload.get("exposure") == "model"
        and payload.get("source") in SYSTEM_DEFAULT_TOOL_SOURCES
    )


@dataclass
class PermissionDecision:
    """单次权限查询返回的版本和生效路径。"""

    is_allowed: bool
    paths: list[list[dict]] = field(default_factory=list)
    review_id: str | None = None
    grant_id: str | None = None
    root_grant_id: str | None = None
    resource_version: str | None = None

    @property
    def source_options(self):
        return {
            key: value
            for key, value in (
                ("grant_id", self.grant_id),
                ("review_id", self.review_id),
            )
            if value is not None
        }


@dataclass(frozen=True)
class DefinitionAccess:
    """Selected definition view with its current editing and use decisions."""

    view: str
    is_editable: bool
    decision: PermissionDecision
