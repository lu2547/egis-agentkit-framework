"""实现 MiddlewareBase，在回复和文件写入前后记录工作区变更及产物。"""

from contextlib import AbstractAsyncContextManager
from typing import Protocol

from agentscope.middleware import MiddlewareBase


class ProjectSession(Protocol):
    @property
    def user_id(self) -> str: ...

    @property
    def project_id(self) -> str: ...

    @property
    def session_id(self) -> str: ...


class FileChangeTracker(Protocol):
    async def observe_safe(self, user_id: str, project_id: str) -> None: ...

    def track_deliveries(
        self, user_id: str, project_id: str, session_id: str, reply_id: str
    ) -> AbstractAsyncContextManager[None]: ...


class FileChangesMiddleware(MiddlewareBase):
    def __init__(self, changes: FileChangeTracker, scope: ProjectSession):
        self.changes, self.scope = changes, scope

    async def on_reply(self, agent, input_kwargs, next_handler):
        await self.changes.observe_safe(self.scope.user_id, self.scope.project_id)
        try:
            async for event in next_handler(**input_kwargs):
                yield event
        finally:
            await self.changes.observe_safe(self.scope.user_id, self.scope.project_id)

    async def on_acting(self, agent, input_kwargs, next_handler):
        # Native on_acting wraps one toolkit.call_tool, never model generation.
        # Team tools may wait on another session and must never hold a file lock.
        tool = input_kwargs.get("tool_call")
        if tool is None or tool.name not in {
            "Write",
            "Edit",
            "Bash",
            "CreateDemoReport",
        }:
            async for event in next_handler(**input_kwargs):
                yield event
            return
        await self.changes.observe_safe(self.scope.user_id, self.scope.project_id)
        try:
            async with self.changes.track_deliveries(
                self.scope.user_id,
                self.scope.project_id,
                self.scope.session_id,
                agent.state.reply_id,
            ):
                async for event in next_handler(**input_kwargs):
                    yield event
        finally:
            await self.changes.observe_safe(self.scope.user_id, self.scope.project_id)
