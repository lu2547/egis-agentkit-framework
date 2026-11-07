"""Compatibility imports; implementation lives in egis_agentkit.service.resource.service."""

from egis_agentkit.service.resource.service import (
    ResourceService,
    ResourceView,
    _archive_files,
    _files_digest,
    _verify_mcp_card,
    _zip,
    mcp_config_checksum,
    skill_archive_digest,
)

__all__ = ['ResourceService', 'ResourceView', '_archive_files', '_files_digest', '_verify_mcp_card', '_zip', 'mcp_config_checksum', 'skill_archive_digest']
