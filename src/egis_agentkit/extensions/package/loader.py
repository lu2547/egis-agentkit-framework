"""Stage trusted immutable Python releases, then atomically expose exact versions."""

from __future__ import annotations

import asyncio
import hashlib
import importlib
import importlib.metadata
import importlib.util
import inspect
import json
import re
import shutil
import sys
import tempfile
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import MappingProxyType, ModuleType
from typing import Any

from agentscope.middleware import MiddlewareBase
from agentscope.tool import ToolBase
from packaging.specifiers import InvalidSpecifier, SpecifierSet

from egis_agentkit.models import Asset


@dataclass(frozen=True)
class Assembly:
    tools_factory: Callable[[Any], Awaitable[list[ToolBase]]] | None = None
    middlewares_factory: (
        Callable[[Any, Any], Awaitable[list[MiddlewareBase]]] | None
    ) = None

    def __post_init__(self):
        for factory in (self.tools_factory, self.middlewares_factory):
            if factory is not None and not (
                inspect.iscoroutinefunction(factory)
                or inspect.iscoroutinefunction(getattr(factory, "__call__", None))
            ):
                raise ValueError("Assembly factories must be asynchronous callables")


def _tree_files(path: Path) -> list[Path]:
    if not path.is_dir():
        raise ValueError("Package directory does not exist")
    result = []
    for child in sorted(path.rglob("*")):
        relative = child.relative_to(path)
        if (
            any(part in {"__pycache__", ".git"} for part in relative.parts)
            or child.suffix == ".pyc"
        ):
            continue
        if child.is_symlink():
            raise ValueError("Release packages cannot contain symlinks")
        if child.is_file():
            result.append(child)
        elif not child.is_dir():
            raise ValueError(
                "Release packages can contain only ordinary files and directories"
            )
    return result


def package_digest(path: Path) -> str:
    """Hash sorted relative names and bytes; canonicalize the manifest sans checksum."""
    path = Path(path)
    digest = hashlib.sha256()
    for child in _tree_files(path):
        name = child.relative_to(path).as_posix().encode("utf-8")
        content = child.read_bytes()
        if name == b"manifest.json":
            try:
                manifest = json.loads(content)
                manifest.pop("checksum_sha256", None)
                content = json.dumps(
                    manifest, sort_keys=True, separators=(",", ":"), ensure_ascii=False
                ).encode("utf-8")
            except (ValueError, AttributeError):
                raise ValueError("Invalid package manifest JSON") from None
        for value in (name, content):
            digest.update(len(value).to_bytes(8, "big"))
            digest.update(value)
    return digest.hexdigest()


@dataclass(frozen=True)
class _Loaded:
    assembly: Assembly
    asset: Asset
    package_path: Path
    namespace: str
    is_enabled: bool = True


class PackageLoader:
    """Only administrators may expose paths to this loader; it is not an HTTP API."""

    def __init__(self, release_roots: list[Path]):
        self.release_roots = tuple(Path(root).resolve() for root in release_roots)
        self._registry: Mapping[tuple[str, str], _Loaded] = MappingProxyType({})
        self._lock = asyncio.Lock()

    def _trusted_path(self, path: Path) -> Path:
        resolved = Path(path).resolve()
        if not any(resolved.is_relative_to(root) for root in self.release_roots):
            raise ValueError("Package path is outside configured release roots")
        if not resolved.is_dir():
            raise ValueError("Package directory does not exist")
        return resolved

    @staticmethod
    def _manifest(path: Path) -> dict:
        try:
            manifest = json.loads((path / "manifest.json").read_text("utf-8"))
            if not isinstance(manifest, dict):
                raise ValueError
            ref, version, entrypoint = (
                manifest[k] for k in ("assembly_ref", "version", "entrypoint")
            )
            if not isinstance(ref, str) or not re.fullmatch(
                r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", ref
            ):
                raise ValueError
            if not isinstance(version, str) or not re.fullmatch(
                r"\d+\.\d+\.\d+(?:[-+][A-Za-z0-9.-]+)?", version
            ):
                raise ValueError
            if (
                type(manifest.get("framework_api_version")) is not int
                or manifest["framework_api_version"] != 1
            ):
                raise ValueError
            if not isinstance(entrypoint, str):
                raise ValueError
            requirements = manifest.get("requirements", {})
            if not isinstance(requirements, dict):
                raise ValueError
            for name, spec in requirements.items():
                if not isinstance(name, str) or not isinstance(spec, str):
                    raise ValueError
                installed = importlib.metadata.version(name)
                if installed not in SpecifierSet(spec):
                    raise ValueError(
                        f"Installed package {name} does not satisfy release requirements"
                    )
            return manifest
        except (
            OSError,
            KeyError,
            TypeError,
            json.JSONDecodeError,
            importlib.metadata.PackageNotFoundError,
            InvalidSpecifier,
        ):
            raise ValueError(
                "Invalid package manifest or unsatisfied release requirements"
            ) from None

    @staticmethod
    def _entrypoint(entrypoint: str) -> tuple[str, str]:
        try:
            module, function = entrypoint.split(":")
        except (ValueError, AttributeError):
            raise ValueError("Entrypoint must be module:register") from None
        if (
            not all(part.isidentifier() for part in module.split("."))
            or not function.isidentifier()
        ):
            raise ValueError("Entrypoint must contain Python identifiers")
        return module, function

    async def load(
        self,
        package_path: Path,
        *,
        expected_sha256: str | None = None,
        entrypoint: str | None = None,
    ) -> Asset:
        async with self._lock:
            path = self._trusted_path(package_path)
            digest = package_digest(path)
            manifest = self._manifest(path)
            declared_digest = manifest.get("checksum_sha256")
            for expected in (expected_sha256, declared_digest):
                if expected is not None and expected != digest:
                    raise ValueError("Package checksum does not match expected SHA-256")
            selected = entrypoint or manifest["entrypoint"]
            module, function = self._entrypoint(selected)
            key = manifest["assembly_ref"], manifest["version"]
            previous = self._registry.get(key)
            if previous is not None:
                if (
                    previous.asset.checksum_sha256 != digest
                    or previous.asset.entrypoint != selected
                ):
                    raise ValueError("An existing package version is immutable")
                return previous.asset.model_copy(deep=True)
            identity = hashlib.sha256((key[0] + "\0" + key[1]).encode()).hexdigest()[
                :24
            ]
            staged = Path(tempfile.mkdtemp(prefix="egis-assembly-"))
            try:
                for original in _tree_files(path):
                    target = staged / original.relative_to(path)
                    target.parent.mkdir(parents=True, exist_ok=True)
                    target.write_bytes(original.read_bytes())
                if package_digest(staged) != digest or package_digest(path) != digest:
                    raise ValueError(
                        "Package changed while copying immutable staging bytes"
                    )
            except BaseException:
                shutil.rmtree(staged, ignore_errors=True)
                raise
            namespace = (
                f"_egis_assembly_{identity}_{digest}_{staged.name.replace('-', '_')}"
            )
            namespace_module = ModuleType(namespace)
            namespace_module.__path__ = [str(staged)]
            namespace_module.__package__ = namespace
            namespace_module.__spec__ = importlib.util.spec_from_loader(
                namespace, loader=None, is_package=True
            )
            # A unique candidate namespace stages all relative imports without touching sys.path.
            sys.modules[namespace] = namespace_module
            try:
                module_path = staged.joinpath(*module.split("."))
                is_package = module_path.is_dir()
                source_path = (
                    module_path / "__init__.py"
                    if is_package
                    else module_path.with_suffix(".py")
                )
                if not source_path.is_file():
                    raise ValueError("Package entrypoint source does not exist")
                qualified = namespace + "." + module
                spec = importlib.util.spec_from_file_location(
                    qualified,
                    source_path,
                    submodule_search_locations=[str(module_path)]
                    if is_package
                    else None,
                )
                if spec is None or spec.loader is None:
                    raise ValueError("Package entrypoint cannot be loaded")
                candidate_module = importlib.util.module_from_spec(spec)
                sys.modules[qualified] = candidate_module
                spec.loader.exec_module(candidate_module)
                register = getattr(candidate_module, function)
                candidate = register()
                if not isinstance(candidate, Assembly):
                    if inspect.iscoroutine(candidate):
                        candidate.close()
                    raise ValueError("Package register() must return Assembly")
                if package_digest(path) != digest or package_digest(staged) != digest:
                    raise ValueError("Package changed while it was being staged")
                asset = Asset(
                    resource_type="assembly",
                    resource_id=key[0],
                    resource_version=key[1],
                    owner_type="system",
                    owner_id="system",
                    checksum_sha256=digest,
                    entrypoint=selected,
                    payload={"package_path": str(path), "framework_api_version": 1},
                )
                updated = dict(self._registry)
                updated[key] = _Loaded(candidate, asset, path, namespace)
                self._registry = MappingProxyType(updated)
                return asset.model_copy(deep=True)
            except BaseException as exc:
                shutil.rmtree(staged, ignore_errors=True)
                for name in tuple(sys.modules):
                    if name == namespace or name.startswith(namespace + "."):
                        sys.modules.pop(name, None)
                if isinstance(
                    exc, (KeyboardInterrupt, SystemExit, asyncio.CancelledError)
                ):
                    raise
                raise ValueError(
                    "Package staging failed; existing versions remain available"
                ) from exc

    def get(self, assembly_ref: str, assembly_version: str) -> Assembly:
        loaded = self._registry.get((assembly_ref, assembly_version))
        if loaded is None or not loaded.is_enabled:
            raise KeyError("Assembly version is not enabled")
        return loaded.assembly

    def disable(self, assembly_ref: str, assembly_version: str) -> None:
        key = assembly_ref, assembly_version
        loaded = self._registry.get(key)
        if loaded is None:
            raise KeyError("Assembly version is not loaded")
        updated = dict(self._registry)
        updated[key] = _Loaded(
            loaded.assembly,
            loaded.asset.model_copy(update={"is_enabled": False}),
            loaded.package_path,
            loaded.namespace,
            False,
        )
        self._registry = MappingProxyType(updated)

    def status(self) -> list[dict]:
        return [
            {
                "assembly_ref": ref,
                "assembly_version": version,
                "checksum_sha256": entry.asset.checksum_sha256,
                "entrypoint": entry.asset.entrypoint,
                "is_enabled": entry.is_enabled,
                "package_path": str(entry.package_path),
            }
            for (ref, version), entry in sorted(self._registry.items())
        ]
