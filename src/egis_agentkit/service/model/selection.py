"""Apply default-model and override policies to a model selection."""

from egis_agentkit.config.snapshot import ConfigSnapshot
from egis_agentkit.errors import Forbidden


class ModelSelection:
    def select(
        self,
        snapshot: ConfigSnapshot,
        model_key: str | None = None,
        *,
        is_override: bool = False,
    ) -> str:
        if is_override and not snapshot.policies.allow_model_override:
            raise Forbidden("此部署不允许覆盖默认模型")
        key = model_key or snapshot.models.default_key
        if (
            snapshot.policies.allowed_model_keys
            and key not in snapshot.policies.allowed_model_keys
        ):
            raise Forbidden("未开放此模型")
        return key
