"""Resolve provider endpoints, parameters and native credentials."""

import json
import os
from hashlib import sha256

from agentscope.app.storage import ChatModelConfig
from agentscope.credential import CredentialFactory

from egis_agentkit.config.snapshot import ModelSpec
from egis_agentkit.errors import Forbidden


class ModelProvider:
    def __init__(self, storage):
        self.storage = storage

    async def resolve(
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
