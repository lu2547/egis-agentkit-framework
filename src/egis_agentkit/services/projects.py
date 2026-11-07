"""Compatibility imports; implementation lives in egis_agentkit.service.project.service."""

from egis_agentkit.service.project.service import ProjectService, purge_session_bus

__all__ = ['ProjectService', 'purge_session_bus']
