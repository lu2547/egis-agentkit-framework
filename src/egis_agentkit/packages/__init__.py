"""Trusted versioned assembly releases with native Tool/Middleware factories."""

from typing import TYPE_CHECKING

from .contracts import Assembly, AssemblyContext

if TYPE_CHECKING:
    from .loader import PackageLoader, package_digest

__all__ = ["Assembly", "AssemblyContext", "PackageLoader", "package_digest"]


def __getattr__(name: str):
    # Keep legacy loader imports available without loading the implementation
    # when a package or service imports only the public contracts.
    if name in ("PackageLoader", "package_digest"):
        from . import loader

        return getattr(loader, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
