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
