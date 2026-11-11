"""Native storage and persisted business records."""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ._postgres import AgentKitStorage as AgentKitStorage
    from .model import _tables as tables

__all__ = ["AgentKitStorage", "tables"]


def __getattr__(name):
    if name == "tables":
        from .model import _tables

        return _tables
    if name == "AgentKitStorage":
        from ._postgres import AgentKitStorage

        return AgentKitStorage
    raise AttributeError(name)
