"""定义共享对象、资源授权和固定发布版本的审核记录。"""

from datetime import datetime
from typing import Any, Literal

from pydantic import Field

from ._base import Model, new_id, utcnow

ManagedResourceType = Literal["team", "agent", "skill", "mcp"]


class ShareTarget(Model):
    """接受授权的单个用户或组织组。"""

    principal_type: Literal["user", "group"]
    principal_id: str = Field(min_length=1, max_length=128, pattern=r"^[^*]+$")


class Grant(ShareTarget):
    """管理员发放或共享审批发放的一条直接授权。"""

    id: str = Field(default_factory=new_id)
    resource_type: ManagedResourceType
    resource_id: str
    resource_view_type: Literal["system", "shared"]
    resource_owner_type: Literal["user", "system"]
    resource_owner_id: str
    grant_source_type: Literal["manual", "share"] = "manual"
    source_review_id: str | None = None


class ShareReview(Model):
    """固定资源版本与接收方的共享申请。"""

    review_id: str = Field(default_factory=new_id)
    resource_type: ManagedResourceType
    resource_id: str
    resource_version: str
    review_type: Literal["share", "update", "revoke"] = "share"
    original_review_id: str | None = None
    content_snapshot: dict[str, Any] | None = None
    owner_id: str
    review_status_type: Literal["pending", "approved", "rejected", "cancelled"] = (
        "pending"
    )
    is_current: bool = False
    share_scope_type: Literal["user", "group"] | None = None
    principal_id: str | None = None
    submitted_by: str
    reviewed_by: str | None = None
    review_reason: str = ""
    submitted_at: datetime = Field(default_factory=utcnow)
    reviewed_at: datetime | None = None
