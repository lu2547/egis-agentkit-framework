"""Compatibility imports; implementation lives in egis_agentkit.extensions.package.loader."""

from egis_agentkit.extensions.package.loader import (
    Assembly,
    PackageLoader,
    _Loaded,
    _tree_files,
    package_digest,
)

__all__ = ['Assembly', 'PackageLoader', '_Loaded', '_tree_files', 'package_digest']
