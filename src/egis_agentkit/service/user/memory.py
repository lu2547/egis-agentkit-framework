"""Per-user native ReMe middleware and its application lifecycle."""

import asyncio
import importlib.util
from pathlib import Path

from agentscope.app._service import ResourceAccessService, get_model
from agentscope.app.access import DenyAllResourceAccessPolicy
from agentscope.middleware import ReMeMiddleware

from egis_agentkit.service.model.service import ModelService
from egis_agentkit.service.workspace.files import WorkspaceFiles


class MemoryService:
    def __init__(self, storage, workspace_root, config_provider):
        if importlib.util.find_spec("reme") is None:
            raise RuntimeError(
                "启用长期记忆需要安装 reme-ai：pip install 'egis-agentkit-framework[memory]'"
            )
        self.files = WorkspaceFiles(Path(workspace_root))
        self.models = ModelService(storage, config_provider)
        self.access = ResourceAccessService(storage, DenyAllResourceAccessPolicy())
        self._middlewares = {}
        self._lock = asyncio.Lock()

    async def for_user(self, user):
        async with self._lock:
            if user.user_id not in self._middlewares:
                path = self.files.user_root(user.user_id) / "memory"
                config = await self.models.resolve(user)
                model = await get_model(user.user_id, config, self.access)
                self._middlewares[user.user_id] = ReMeMiddleware(
                    workspace_dir=str(path),
                    parameters=ReMeMiddleware.Parameters(chat_model=model),
                )
            return self._middlewares[user.user_id]

    async def close(self):
        async with self._lock:
            middlewares, self._middlewares = self._middlewares, {}
            await asyncio.gather(
                *(middleware.close() for middleware in middlewares.values())
            )
