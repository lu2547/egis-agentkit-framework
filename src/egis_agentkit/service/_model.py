"""LLM 目录、模型选择及连接配置。"""

import json
import os
from hashlib import sha256

from agentscope.app.storage import ChatModelConfig
from agentscope.credential import CredentialFactory

from egis_agentkit.client.nacos._settings import AgentKitConfig, ModelSpec
from egis_agentkit.errors import Forbidden, NotFound


class ModelService:
    """列出可选模型，解析会话配置并生成原生模型凭证。"""

    def __init__(self, storage, config_provider):
        self.storage, self.config = storage, config_provider

    def list(self):
        models = self.config.snapshot.model
        allowed = models.selection.allowed_keys
        return {
            "items": [
                {"key": spec.key, "display_name": spec.display_name}
                for spec in models.catalog.models
                if not allowed or spec.key in allowed
            ],
            "default_key": models.selection.default_key,
        }

    async def resolve(
        self,
        user,
        model_key=None,
        *,
        is_override=False,
        snapshot: AgentKitConfig | None = None,
    ):
        if snapshot is None:
            snapshot = self.config.snapshot
        selection = snapshot.model.selection
        if is_override and not selection.allow_override:
            raise Forbidden("此部署不允许覆盖默认模型")
        key = model_key or selection.default_key
        if selection.allowed_keys and key not in selection.allowed_keys:
            raise Forbidden("未开放此模型")
        spec = next(
            (item for item in snapshot.model.catalog.models if item.key == key), None
        )
        if spec is None:
            raise NotFound("模型配置不存在")
        return await self._resolve_provider(user, spec, snapshot.revision)

    async def _resolve_provider(
        self, user, spec: ModelSpec, config_version: int
    ) -> ChatModelConfig:
        api_key = os.environ.get(spec.api_key_env)
        if not api_key:
            raise Forbidden("模型服务凭证尚未配置")
        # A pending confirmation keeps its old provider/model parameters even
        # after another task adopts a newer Nacos model configuration.
        identity = json.dumps(
            [
                user.user_id,
                spec.model_dump(),
                config_version,
                sha256(api_key.encode()).hexdigest(),
            ],
            sort_keys=True,
            ensure_ascii=False,
        )
        identifier = sha256(identity.encode()).hexdigest()[:32]
        credential = CredentialFactory.from_dict(
            {
                "type": spec.credential_type,
                "id": identifier,
                "name": spec.display_name,
                "api_key": api_key,
                **({"base_url": spec.base_url} if spec.base_url else {}),
            }
        )
        await self.storage.upsert_credential(user.user_id, credential)
        return ChatModelConfig(
            type=spec.credential_type,
            credential_id=identifier,
            model=spec.model,
            model_class=spec.model_class,
            parameters=spec.parameters,
        )
