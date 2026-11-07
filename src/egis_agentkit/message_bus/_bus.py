"""Compatibility imports; implementation lives in egis_agentkit.extensions.message_bus.bus."""

from egis_agentkit.extensions.message_bus.bus import (
    EgisMessageBus,
    LeaseLostError,
    logger,
)

__all__ = ['EgisMessageBus', 'LeaseLostError', 'logger']
