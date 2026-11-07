"""Compatibility imports; implementation lives in egis_agentkit.extensions.hub.nacos."""

from egis_agentkit.extensions.hub.nacos import (
    BASE,
    Authorization,
    NacosMCPHub,
    NacosSkillHub,
    _AuthorizedHub,
    _page,
    _version,
)

__all__ = ['Authorization', 'BASE', 'NacosMCPHub', 'NacosSkillHub', '_AuthorizedHub', '_page', '_version']
