"""Stable contracts for trusted Agent implementation packages."""

import inspect
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Generic, TypedDict, TypeVar

from agentscope.middleware import MiddlewareBase
from agentscope.tool import ToolBase
from agentscope.workspace import WorkspaceBase


@dataclass(frozen=True, slots=True)
class AssemblyContext:
    """Read-only identity and paths for an API v2 package's locked Agent run.

    The tool and middleware factories receive the same instance. On resume
    or in a team member's run, they receive a context for that execution.
    """

    user_id: str
    agent_id: str
    session_id: str
    project_id: str
    workdir: Path
    artifacts_dir: Path


Context = TypeVar("Context")


@dataclass(frozen=True)
class Assembly(Generic[Context]):
    tools_factory: Callable[[Context], Awaitable[list[ToolBase]]] | None = None
    middlewares_factory: (
        Callable[[Context, WorkspaceBase], Awaitable[list[MiddlewareBase]]] | None
    ) = None

    def __post_init__(self):
        for factory in (self.tools_factory, self.middlewares_factory):
            if factory is not None and not (
                inspect.iscoroutinefunction(factory)
                or inspect.iscoroutinefunction(getattr(factory, "__call__", None))
            ):
                raise ValueError("Assembly factories must be asynchronous callables")


class PackageLoadState(TypedDict):
    assembly_ref: str
    assembly_version: str
    checksum_sha256: str | None
    entrypoint: str | None
    is_enabled: bool
    package_path: str
