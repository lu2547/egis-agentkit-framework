"""为启用长期记忆的个人助理创建、复用和关闭记忆组件。

核心能力
- 按用户装配并复用 AgentScope 记忆中间件。
- 停机时关闭已创建的记忆中间件。
"""

import asyncio
import importlib.util
from pathlib import Path

from agentscope.app._service import ResourceAccessService, get_model
from agentscope.app.access import DenyAllResourceAccessPolicy
from agentscope.middleware import ReMeMiddleware

from egis_agentkit.service._model import ModelService
from egis_agentkit.storage._files import FileStorage


class PersonalMemoryService:
    """按用户管理记忆组件的创建、复用和关闭，不承担对话钩子的具体实现。"""

    def __init__(self, storage, workspace_root, config_provider):
        if importlib.util.find_spec("reme") is None:
            raise RuntimeError(
                "启用长期记忆需要安装 reme-ai：pip install 'egis-agentkit-framework[memory]'"
            )
        self.files = FileStorage(Path(workspace_root))
        self.settings = config_provider.snapshot.agent.personal.memory
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
                    parameters=ReMeMiddleware.Parameters(
                        chat_model=model, top_k=self.settings.top_k
                    ),
                )
            return self._middlewares[user.user_id]

    async def close(self):
        async with self._lock:
            middlewares, self._middlewares = self._middlewares, {}
            await asyncio.gather(
                *(middleware.close() for middleware in middlewares.values())
            )
