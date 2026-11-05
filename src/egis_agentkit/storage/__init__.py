"""Native Storage extension; deliberately no Repository layer."""

from .postgres import AgentKitStorage

__all__ = ["AgentKitStorage"]
