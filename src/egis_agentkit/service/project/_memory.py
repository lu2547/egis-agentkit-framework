"""为本地 Agent 创建、复用和关闭项目记忆。

核心能力
- 按用户和项目装配并复用 AgentScope 记忆中间件。
- 停机时关闭已创建的记忆中间件。
"""

import asyncio
import importlib.util

from agentscope.app._service import ResourceAccessService, get_model
from agentscope.app.access import DenyAllResourceAccessPolicy
from agentscope.middleware import ReMeMiddleware

from egis_agentkit.service._model import ModelService
from egis_agentkit.storage.model import User
from egis_agentkit.storage.project._files import ProjectFileStorage


class ProjectMemoryService:
    """同一用户项目下的本地 Agent 共用记忆，作业由原生 ReMe 执行。"""

    def __init__(
        self, storage, workspace_root, config_provider, *, middleware_factory=None
    ):
        if importlib.util.find_spec("reme") is None:
            raise RuntimeError(
                "启用长期记忆需要安装 reme-ai：pip install 'egis-agentkit-framework[memory]'"
            )
        self.files = ProjectFileStorage(workspace_root)
        self.settings = config_provider.snapshot.project.memory
        self.models = ModelService(storage, config_provider)
        self.access = ResourceAccessService(storage, DenyAllResourceAccessPolicy())
        self._middlewares: dict[tuple[str, str], ReMeMiddleware] = {}
        self._lock = asyncio.Lock()
        self._middleware_factory = middleware_factory or ReMeMiddleware

    async def for_project(self, user: User, project_id: str) -> ReMeMiddleware:
        """用已校验的项目绑定选择同一份检索和回写存储。"""
        key = (user.user_id, project_id)
        async with self._lock:
            if key not in self._middlewares:
                path = await asyncio.to_thread(
                    self.files.memory_root, user.user_id, project_id
                )
                config = await self.models.resolve(user)
                model = await get_model(user.user_id, config, self.access)
                self._middlewares[key] = self._middleware_factory(
                    workspace_dir=str(path),
                    parameters=ReMeMiddleware.Parameters(
                        chat_model=model, top_k=self.settings.top_k
                    ),
                )
            return self._middlewares[key]

    async def close(self):
        async with self._lock:
            middlewares, self._middlewares = self._middlewares, {}
            await asyncio.gather(
                *(middleware.close() for middleware in middlewares.values())
            )
