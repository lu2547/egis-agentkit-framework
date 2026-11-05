"""Resolve company model configuration through native Credential models."""

import json
import os
from hashlib import sha256

from agentscope.app.storage import ChatModelConfig
from agentscope.credential import CredentialFactory

from ..errors import Forbidden, NotFound


class ModelService:
    def __init__(self, storage, config_provider):
        self.storage, self.config = storage, config_provider

    def list(self):
        snapshot = self.config.snapshot
        allowed = snapshot.policies.allowed_model_keys
        return {
            "items": [
                {"key": m.key, "display_name": m.display_name}
                for m in snapshot.models.models
                if not allowed or m.key in allowed
            ],
            "default_key": snapshot.models.default_key,
        }

    async def resolve(self, user, model_key=None, *, is_override=False):
        snapshot = self.config.snapshot
        if is_override and not snapshot.policies.allow_model_override:
            raise Forbidden("此部署不允许覆盖默认模型")
        key = model_key or snapshot.models.default_key
        if (
            snapshot.policies.allowed_model_keys
            and key not in snapshot.policies.allowed_model_keys
        ):
            raise Forbidden("未开放此模型")
        spec = next((item for item in snapshot.models.models if item.key == key), None)
        if spec is None:
            raise NotFound("模型配置不存在")
        api_key = os.environ.get(spec.api_key_env)
        if not api_key:
            raise Forbidden("模型服务凭证尚未配置")
        # A pending confirmation keeps its old provider/model parameters even
        # after another task adopts a newer Nacos model configuration.
        identity = json.dumps(
            [
                user.user_id,
                spec.model_dump(),
                snapshot.config_version,
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
        model_class = {
            "OpenAIChatModel": "openai_chat",
            "OpenAIResponseModel": "openai_response",
        }.get(spec.model_class, spec.model_class)
        return ChatModelConfig(
            type=spec.credential_type,
            credential_id=identifier,
            model=spec.model,
            model_class=model_class,
            parameters=spec.parameters,
        )
