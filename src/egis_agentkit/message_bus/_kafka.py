"""Compatibility imports; implementation lives in egis_agentkit.integration.kafka."""

from egis_agentkit.integration.kafka import (
    KafkaTransport,
    _Rebalance,
    _Subscription,
    logger,
)

__all__ = ['KafkaTransport', '_Rebalance', '_Subscription', 'logger']
