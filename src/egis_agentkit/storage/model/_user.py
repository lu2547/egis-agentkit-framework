"""用户和组织组记录。"""

from typing import Literal

from pydantic import Field

from ._base import Model


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
