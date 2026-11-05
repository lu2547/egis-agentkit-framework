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
            await self.changes.observe_safe(
                self.scope.user_id, self.scope.project_id, discover=True
            )

    async def on_acting(self, agent, input_kwargs, next_handler):
        await self.changes.observe_safe(self.scope.user_id, self.scope.project_id)
        try:
            async for event in next_handler(**input_kwargs):
                yield event
        finally:
            await self.changes.observe_safe(self.scope.user_id, self.scope.project_id)
