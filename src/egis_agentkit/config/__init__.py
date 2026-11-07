"""Nacos bootstrap, REST transport and typed atomic configuration snapshots."""

from egis_agentkit.config.snapshot import (
    AuthConfig,
    ConfigSnapshot,
    ModelsConfig,
    ModelSpec,
    NacosConfigProvider,
    PoliciesConfig,
    RuntimeConfig,
)
from egis_agentkit.integration.nacos.client import (
    NacosBootstrap,
    NacosClient,
    NacosError,
)

# Singular aliases support callers naming the configuration domain.
ModelConfig = ModelsConfig
PolicyConfig = PoliciesConfig

__all__ = [
    "NacosBootstrap",
    "NacosClient",
    "NacosError",
    "AuthConfig",
    "RuntimeConfig",
    "ModelSpec",
    "ModelsConfig",
    "PoliciesConfig",
    "ModelConfig",
    "PolicyConfig",
    "ConfigSnapshot",
    "NacosConfigProvider",
]
