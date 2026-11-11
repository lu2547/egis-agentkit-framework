"""可被应用和运行时复用的持久化记录。"""

from ._access import Grant, ShareReview, ShareTarget
from ._base import Model, new_id, utcnow
from ._definition import (
    AgentPayload,
    AgentType,
    Definition,
    DefinitionRef,
    TeamPayload,
    ViewType,
)
from ._resource import Asset, ResourceRef, SkillActivation, asset_snapshot_matches
from ._user import Group, User

__all__ = [
    "AgentType",
    "AgentPayload",
    "Asset",
    "Definition",
    "DefinitionRef",
    "Grant",
    "Group",
    "Model",
    "ResourceRef",
    "ShareReview",
    "ShareTarget",
    "SkillActivation",
    "TeamPayload",
    "User",
    "ViewType",
    "asset_snapshot_matches",
    "new_id",
    "utcnow",
]
