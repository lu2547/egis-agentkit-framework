"""Compatibility imports; implementation lives in egis_agentkit.extensions.package.loader."""

from egis_agentkit.extensions.package.loader import (
    PackageLoader,
    _Loaded,
    _tree_files,
    package_digest,
)
from egis_agentkit.packages.contracts import Assembly, AssemblyContext

__all__ = [
    "Assembly",
    "AssemblyContext",
    "PackageLoader",
    "_Loaded",
    "_tree_files",
    "package_digest",
]
