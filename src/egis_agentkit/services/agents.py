"""Compatibility imports; implementation lives in egis_agentkit.service.agent.definition."""

from egis_agentkit.service.agent.service import AgentService
from egis_agentkit.service.agent.views import (
    definition_data,
    require_current_shared_asset,
)

__all__ = ['AgentService', 'definition_data', 'require_current_shared_asset']
