"""持久化模型通用字段及标识生成。"""

from datetime import datetime, timezone
from uuid import uuid4

from pydantic import BaseModel, ConfigDict


def new_id() -> str:
    return uuid4().hex


def utcnow() -> datetime:
    return datetime.now(timezone.utc)


class Model(BaseModel):
    model_config = ConfigDict(extra="forbid")


PUBLICATION_REVISIONS_PER_MAJOR = 10


def publication_label(revision: int) -> str:
    """把递增的发布序号显示为 1.0 至 1.9，之后进到 2.0。"""
    major, minor = divmod(revision - 1, PUBLICATION_REVISIONS_PER_MAJOR)
    return f"{major + 1}.{minor}"


def resource_available(record: dict) -> bool:
    """按资源生命周期判断可用性；Agent 删除与安装项启停分别表达。"""
    if record.get("definition_type") == "agent":
        return record.get("deleted_at") is None
    return record["is_enabled"]
