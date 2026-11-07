from agentscope.middleware import MiddlewareBase


class FileChangesMiddleware(MiddlewareBase):
    def __init__(self, changes, scope):
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
        if tool is None or tool.name not in {"Write", "Edit", "Bash"}:
            async for event in next_handler(**input_kwargs):
                yield event
            return
        await self.changes.observe_safe(self.scope.user_id, self.scope.project_id)
        try:
            async with self.changes.track_deliveries(
                self.scope.user_id, self.scope.project_id,
                self.scope.session_id, agent.state.reply_id,
            ):
                async for event in next_handler(**input_kwargs):
                    yield event
        finally:
            await self.changes.observe_safe(self.scope.user_id, self.scope.project_id)
