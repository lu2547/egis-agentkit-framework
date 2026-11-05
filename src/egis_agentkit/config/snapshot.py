"""Validated configuration batches; a single pointer swap publishes a batch."""

import asyncio
from typing import Literal

import yaml
from agentscope.permission import PermissionMode
from pydantic import BaseModel, ConfigDict, Field, model_validator

from .client import NacosClient, NacosError


class ConfigModel(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class AuthConfig(ConfigModel):
    mode: Literal["jwt", "demo"]
    issuer: str
    audience: str
    algorithm: Literal["RS256", "HS256"]
    jwks_url: str | None = None
    signing_secret_env: str = "AGENTKIT_JWT_SECRET"
    demo_password_env: str = "AGENTKIT_DEMO_PASSWORD"


class MemoryConfig(ConfigModel):
    is_enabled: bool = False


class RuntimeConfig(ConfigModel):
    config_version: int = Field(ge=1)
    database_url_env: str = "AGENTKIT_DATABASE_URL"
    workspace_root: str = Field(min_length=1)
    release_roots: list[str]
    workspace_mode: Literal["local", "sandbox"] = "local"
    deployment_mode: Literal["standalone", "cluster"] = "standalone"
    kafka_bootstrap_servers: list[str] = Field(
        default_factory=lambda: ["127.0.0.1:9092"]
    )
    kafka_topic_prefix: str = "egis-agentkit"
    download_secret_env: str = "AGENTKIT_DOWNLOAD_SECRET"
    auth: AuthConfig
    sandbox: dict = Field(default_factory=dict)
    memory: MemoryConfig = Field(default_factory=MemoryConfig)


class ModelSpec(ConfigModel):
    key: str = Field(min_length=1)
    display_name: str
    model: str = Field(min_length=1)
    credential_type: str = "openai_credential"
    model_class: str = "OpenAIChatModel"
    base_url: str | None = None
    api_key_env: str = Field(min_length=1)
    parameters: dict = Field(default_factory=dict)


class ModelsConfig(ConfigModel):
    config_version: int = Field(ge=1)
    default_key: str
    models: list[ModelSpec]

    @model_validator(mode="after")
    def check_model_keys(self):
        keys = [m.key for m in self.models]
        if len(set(keys)) != len(keys) or self.default_key not in keys:
            raise ValueError("Model keys must be unique and contain default_key")
        return self


class PoliciesConfig(ConfigModel):
    config_version: int = Field(ge=1)
    allow_private_self_use: bool = True
    allow_personal_self_use: bool = True
    allowed_permission_modes: list[PermissionMode] = Field(
        default_factory=lambda: [
            PermissionMode.DEFAULT,
            PermissionMode.ACCEPT_EDITS,
            PermissionMode.EXPLORE,
            PermissionMode.DONT_ASK,
        ]
    )
    allow_model_override: bool = True
    allowed_model_keys: list[str] = Field(default_factory=list)
    max_upload_bytes: int = Field(default=20 * 1024 * 1024, gt=0)


class ConfigSnapshot(ConfigModel):
    config_version: int
    runtime: RuntimeConfig
    models: ModelsConfig
    policies: PoliciesConfig


class NacosConfigProvider:
    def __init__(self, client: NacosClient):
        self.client = client
        self._snapshot: ConfigSnapshot | None = None
        self.is_connected = False
        self.restart_required = False
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    @property
    def snapshot(self) -> ConfigSnapshot:
        if self._snapshot is None:
            raise RuntimeError("Nacos configuration has not been loaded")
        return self._snapshot.model_copy(deep=True)

    async def refresh(self) -> ConfigSnapshot:
        async with self._lock:
            try:
                contents = await asyncio.gather(
                    *(
                        self.client.get_config(f"agentkit-{name}.yaml")
                        for name in ("runtime", "models", "policies")
                    )
                )
                runtime, models, policies = [
                    kind.model_validate(yaml.safe_load(content))
                    for kind, content in zip(
                        (RuntimeConfig, ModelsConfig, PoliciesConfig), contents
                    )
                ]
                if (
                    len(
                        {
                            runtime.config_version,
                            models.config_version,
                            policies.config_version,
                        }
                    )
                    != 1
                ):
                    raise NacosError("Nacos configuration versions do not match")
                if set(policies.allowed_model_keys) - {m.key for m in models.models}:
                    raise NacosError("Policy references an unknown model")
                candidate = ConfigSnapshot(
                    config_version=runtime.config_version,
                    runtime=runtime,
                    models=models,
                    policies=policies,
                )
                if self._snapshot is not None:
                    if runtime.model_dump(
                        exclude={"config_version"}
                    ) != self._snapshot.runtime.model_dump(exclude={"config_version"}):
                        self.restart_required = True
                        raise NacosError(
                            "Runtime component configuration changed; restart required"
                        )
                    if candidate.config_version < self._snapshot.config_version:
                        raise NacosError(
                            "Configuration version rollback is not allowed"
                        )
                    if (
                        candidate.config_version == self._snapshot.config_version
                        and candidate != self._snapshot
                    ):
                        raise NacosError(
                            "Changed configuration requires a new batch version"
                        )
                self._snapshot = candidate
                self.is_connected = True
                self.restart_required = False
                return candidate.model_copy(deep=True)
            except Exception as exc:
                self.is_connected = False
                if isinstance(exc, NacosError):
                    raise
                raise NacosError("Nacos configuration validation failed") from None

    async def start(self, interval: float = 10) -> None:
        if interval <= 0:
            raise ValueError("Refresh interval must be positive")
        if self._task is not None:
            return
        await self.refresh()

        async def poll():
            while True:
                await asyncio.sleep(interval)
                try:
                    await self.refresh()
                except NacosError:
                    pass  # Retain the last validated snapshot; health is exposed separately.

        self._task = asyncio.create_task(poll(), name="nacos-config-refresh")

    async def close(self) -> None:
        if self._task is not None:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
            self._task = None
