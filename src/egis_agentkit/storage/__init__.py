"""Native Storage extension; deliberately no Repository layer."""

from egis_agentkit.storage.postgres import AgentKitStorage

__all__ = ["AgentKitStorage"]
