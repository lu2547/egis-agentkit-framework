"""读取并发布经过验证的 Nacos 配置快照。"""

import asyncio
import logging

import yaml
from agentscope.permission import PermissionMode
from pydantic import ValidationError

from egis_agentkit.client.nacos._client import NacosClient, NacosError
from egis_agentkit.client.nacos._settings import (
    AgentKitConfig,
    ConfigModel,
)

AGENTKIT_DATA_ID = "agentkit.yaml"
logger = logging.getLogger(__name__)


class LivePolicies:
    """向权限服务提供读取当前配置的入口。"""

    def __init__(self, provider: "NacosConfigProvider"):
        self.provider = provider

    @property
    def allow_private_self_use(self) -> bool:
        return self.provider.snapshot.access.allow_private_self_use

    @property
    def allow_personal_self_use(self) -> bool:
        return self.provider.snapshot.access.allow_personal_self_use


class _Document(ConfigModel):
    agentkit: AgentKitConfig


class _DuplicateKeyError(yaml.YAMLError):
    pass


class _UniqueKeysLoader(yaml.SafeLoader):
    def construct_mapping(self, node, deep=False):
        self.flatten_mapping(node)
        pairs = self.construct_pairs(node, deep=deep)
        try:
            mapping = dict(pairs)
        except TypeError:
            raise yaml.YAMLError("invalid mapping key") from None
        if len(mapping) != len(pairs):
            raise _DuplicateKeyError
        return mapping


def load_agentkit_config(content: str) -> AgentKitConfig:
    """解析一份 Nacos 文档，验证类型和配置内部引用。"""
    try:
        document = yaml.load(content, Loader=_UniqueKeysLoader)
    except _DuplicateKeyError:
        raise NacosError(f"{AGENTKIT_DATA_ID}: duplicate YAML mapping key") from None
    except yaml.YAMLError:
        raise NacosError(f"{AGENTKIT_DATA_ID}: invalid YAML") from None
    if not isinstance(document, dict) or "agentkit" not in document:
        raise NacosError(f"{AGENTKIT_DATA_ID}: missing agentkit root")
    try:
        settings = _Document.model_validate(
            document, by_alias=True, by_name=False
        ).agentkit
    except ValidationError as exc:
        problems = []
        for issue in exc.errors(
            include_url=False, include_context=False, include_input=False
        ):
            location = issue["loc"]
            error_type = issue["type"]
            if error_type == "extra_forbidden":
                location = (*location[:-1], "<unknown>")
            path = ".".join(str(part) for part in location)
            problems.append(f"{path} ({error_type})")
        raise NacosError(f"{AGENTKIT_DATA_ID}: " + ", ".join(problems)) from None

    identity = settings.user.identity
    if identity.mode == "jwt":
        if identity.algorithm != "RS256":
            raise NacosError(
                "agentkit.user.identity.algorithm: JWT mode requires RS256"
            )
        if not identity.jwks_url:
            raise NacosError(
                "agentkit.user.identity.jwksUrl: JWT mode requires a JWKS URL"
            )
    elif identity.algorithm != "HS256":
        raise NacosError("agentkit.user.identity.algorithm: demo mode requires HS256")

    models = settings.model.catalog.models
    keys = {model.key for model in models}
    selection = settings.model.selection
    permissions = settings.session.permissions
    remote = settings.agent.remote
    if len(keys) != len(models):
        raise NacosError("agentkit.model.catalog.models: model keys must be unique")
    if selection.default_key not in keys or (
        selection.allowed_keys and selection.default_key not in selection.allowed_keys
    ):
        raise NacosError(
            "agentkit.model.selection.defaultKey: default model must be available"
        )
    if set(selection.allowed_keys) - keys:
        raise NacosError("agentkit.model.selection.allowedKeys: unknown model")
    if PermissionMode.DONT_ASK in permissions.allowed_modes:
        raise NacosError("agentkit.session.permissions.allowedModes: unsupported mode")
    if permissions.default_mode not in permissions.allowed_modes:
        raise NacosError(
            "agentkit.session.permissions.defaultMode: default mode must be allowed"
        )
    if remote.timeout.default_seconds > remote.timeout.max_seconds:
        raise NacosError(
            "agentkit.agent.remote.timeout.defaultSeconds: exceeds maximum"
        )
    if remote.retry.default_retries >= remote.retry.max_attempts:
        raise NacosError(
            "agentkit.agent.remote.retry.defaultRetries: exceeds retry budget"
        )
    if len(settings.project.defaults.name) > settings.project.defaults.max_name_length:
        raise NacosError("agentkit.project.defaults.name: exceeds maximum length")
    return settings


def _fixed_components(snapshot: AgentKitConfig) -> dict:
    return {
        "agentkit.agent.personal.memory": snapshot.agent.personal.memory,
        "agentkit.connector.oauth.credentialKeyEnv": snapshot.connector.oauth.credential_key_env,
        "agentkit.client.postgres": snapshot.client.postgres,
        "agentkit.client.kafka": snapshot.client.kafka,
        "agentkit.client.tracing": snapshot.client.tracing,
        "agentkit.logging": snapshot.logging,
        "agentkit.user.identity": snapshot.user.identity,
        "agentkit.workspace.runtime": snapshot.workspace.runtime,
        "agentkit.workspace.probe": snapshot.workspace.probe,
        "agentkit.workspace.lock": snapshot.workspace.lock,
        "agentkit.workspace.readGuard": snapshot.workspace.read_guard,
        "agentkit.skill.storage": snapshot.skill.storage,
        "agentkit.project.download": snapshot.project.download,
    }


class NacosConfigProvider:
    def __init__(self, client: NacosClient):
        self.client = client
        self._snapshot: AgentKitConfig | None = None
        self._platform_tool_ids: frozenset[str] | None = None
        self.is_connected = False
        self.restart_required = False
        self.last_error: str | None = None
        self._lock = asyncio.Lock()
        self._task: asyncio.Task | None = None

    @property
    def snapshot(self) -> AgentKitConfig:
        if self._snapshot is None:
            raise RuntimeError("Nacos configuration has not been loaded")
        return self._snapshot.model_copy(deep=True)

    def bind_platform_tool_ids(self, registered_ids: set[str]) -> None:
        """使用运行时已注册的工具标识校验当前及后续快照。"""
        registered = frozenset(registered_ids)
        if self._snapshot is not None:
            self._check_platform_tools(self._snapshot, registered)
        self._platform_tool_ids = registered

    @staticmethod
    def _check_platform_tools(
        candidate: AgentKitConfig, registered: frozenset[str]
    ) -> None:
        selected = candidate.tool.defaults.registered_ids
        if selected is not None and set(selected) - registered:
            raise NacosError(
                "agentkit.tool.defaults.registeredIds: unknown platform default tool"
            )

    async def refresh(self) -> AgentKitConfig:
        async with self._lock:
            try:
                try:
                    content = await self.client.get_config(AGENTKIT_DATA_ID)
                except NacosError as exc:
                    raise NacosError(
                        f"{AGENTKIT_DATA_ID}: {exc}",
                        exc.status_code,
                        operation_code=exc.operation_code,
                    ) from None
                candidate = load_agentkit_config(content)
                if self._platform_tool_ids is not None:
                    self._check_platform_tools(candidate, self._platform_tool_ids)
                current = self._snapshot
                if current is not None:
                    if candidate.revision < current.revision:
                        raise NacosError("agentkit.revision: rollback is not allowed")
                    if candidate.revision == current.revision and candidate != current:
                        raise NacosError(
                            "agentkit.revision: changed configuration requires a new revision"
                        )
                    fixed = _fixed_components(current)
                    changed = [
                        path
                        for path, value in _fixed_components(candidate).items()
                        if value != fixed[path]
                    ]
                    if changed:
                        self.restart_required = True
                        raise NacosError(", ".join(changed) + ": restart required")
            except NacosError as exc:
                # 仅在状态或错误改变时输出；轮询重试不重复刷屏。
                if self.is_connected or self.last_error != str(exc):
                    logger.warning(
                        "config.refresh_failed",
                        extra={
                            "context": {
                                "error": str(exc),
                                "restart_required": self.restart_required,
                            }
                        },
                    )
                self.is_connected = False
                self.last_error = str(exc)
                raise

            if self._snapshot != candidate or not self.is_connected:
                logger.info(
                    "config.loaded",
                    extra={
                        "context": {
                            "revision": candidate.revision,
                        }
                    },
                )
            self._snapshot = candidate
            self.is_connected = True
            self.restart_required = False
            self.last_error = None
            return candidate.model_copy(deep=True)

    async def start(self) -> None:
        if self._task is not None:
            return
        if self._snapshot is None:
            await self.refresh()
        if self._task is not None:
            return

        async def poll():
            while True:
                refresh = self._snapshot.client.nacos.refresh
                interval = (
                    refresh.interval_seconds
                    if self.is_connected
                    else refresh.failure_interval_seconds
                )
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
