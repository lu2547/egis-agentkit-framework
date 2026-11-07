"""Compatibility imports; implementation lives in egis_agentkit.extensions.compatibility.execution."""

from egis_agentkit.extensions.compatibility.execution import (
    ExecutionCoordinator,
    ExecutionScope,
    ScopedMessageBus,
    execution_scope,
    execution_setup_error,
    projected_agent,
    stream_subscription_ready,
)

__all__ = ['ExecutionCoordinator', 'ExecutionScope', 'ScopedMessageBus', 'execution_scope', 'execution_setup_error', 'projected_agent', 'stream_subscription_ready']
