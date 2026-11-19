"""管理列表的分页窗口与响应。"""

from dataclasses import dataclass
from typing import Generic, TypeVar

from pydantic import BaseModel

Item = TypeVar("Item")


class AdminPageResult(BaseModel, Generic[Item]):
    items: list[Item]
    page: int
    page_size: int
    has_next: bool


@dataclass(frozen=True, slots=True)
class AdminPage:
    """读取一条额外记录，用于判断是否还有下一页。"""

    number: int
    size: int

    @property
    def offset(self) -> int:
        return (self.number - 1) * self.size

    @property
    def fetch_limit(self) -> int:
        return self.size + 1

    def response(self, rows: list[Item]) -> dict:
        return {
            "items": rows[: self.size],
            "page": self.number,
            "page_size": self.size,
            "has_next": len(rows) > self.size,
        }
