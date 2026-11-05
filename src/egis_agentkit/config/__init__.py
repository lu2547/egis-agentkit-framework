"""Nacos bootstrap, REST transport and typed atomic configuration snapshots."""

from .client import NacosBootstrap, NacosClient, NacosError
from .snapshot import (
    AuthConfig,
    ConfigSnapshot,
    ModelsConfig,
    ModelSpec,
    NacosConfigProvider,
    PoliciesConfig,
    RuntimeConfig,
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
