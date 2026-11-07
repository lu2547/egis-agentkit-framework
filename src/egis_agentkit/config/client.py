"""Compatibility imports; implementation lives in egis_agentkit.integration.nacos.client."""

from egis_agentkit.integration.nacos.client import (
    NacosBootstrap,
    NacosClient,
    NacosError,
)

__all__ = ['NacosBootstrap', 'NacosClient', 'NacosError']
