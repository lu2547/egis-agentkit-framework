"""List and look up model specifications in a configuration snapshot."""

from egis_agentkit.config.snapshot import ConfigSnapshot, ModelSpec
from egis_agentkit.errors import NotFound


class ModelCatalog:
    def list(self, snapshot: ConfigSnapshot) -> dict:
        allowed = snapshot.policies.allowed_model_keys
        return {
            "items": [
                {"key": model.key, "display_name": model.display_name}
                for model in snapshot.models.models
                if not allowed or model.key in allowed
            ],
            "default_key": snapshot.models.default_key,
        }

    def get(self, snapshot: ConfigSnapshot, key: str) -> ModelSpec:
        spec = next((item for item in snapshot.models.models if item.key == key), None)
        if spec is None:
            raise NotFound("模型配置不存在")
        return spec
