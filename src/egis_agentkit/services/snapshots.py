"""Compatibility imports; implementation lives in egis_agentkit.service.access.snapshot."""

from egis_agentkit.service.access.snapshot import (
    SnapshotService,
    node_key,
    normalize_nodes,
)

__all__ = ['SnapshotService', 'node_key', 'normalize_nodes']
