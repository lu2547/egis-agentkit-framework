"""配置的领域分组、YAML 键和类型。"""

from typing import Annotated, Literal
from urllib.parse import urlsplit

from agentscope.permission import PermissionMode
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StringConstraints,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

PositiveInt = Annotated[int, Field(strict=True, gt=0)]
NonNegativeInt = Annotated[int, Field(strict=True, ge=0)]
Seconds = Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]
NonNegativeSeconds = Annotated[float, Field(strict=True, ge=0, allow_inf_nan=False)]
NonEmptyString = Annotated[str, Field(min_length=1)]
SecretRef = Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")]

DEFAULT_SKILL_ARCHIVE_MAX_BYTES = 50 * 1024 * 1024 - 1
DEFAULT_WORKSPACE_UPLOAD_BYTES = 20 * 1024 * 1024
DEFAULT_WORKSPACE_TREE_BYTES = 200 * 1024 * 1024


class ConfigModel(BaseModel):
    model_config = ConfigDict(
        frozen=True, extra="forbid", alias_generator=to_camel, populate_by_name=True
    )


class PaginationConfig(ConfigModel):
    default_page_size: PositiveInt = 50
    max_page_size: PositiveInt = 100

    @model_validator(mode="after")
    def check_page_size(self):
        if self.default_page_size > self.max_page_size:
            raise ValueError("defaultPageSize exceeds maxPageSize")
        return self


class AuthConfig(ConfigModel):
    mode: Literal["jwt", "demo"]
    issuer: NonEmptyString
    audience: NonEmptyString
    algorithm: Literal["RS256", "HS256"]
    jwks_url: str | None = None
    signing_secret_env: SecretRef = "AGENTKIT_JWT_SECRET"
    demo_password_env: SecretRef = "AGENTKIT_DEMO_PASSWORD"
    jwt_lifetime_seconds: PositiveInt = 3600
    jwt_clock_skew_seconds: NonNegativeSeconds = 10
    jwt_max_token_length: PositiveInt = 16_384
    jwt_min_signing_secret_length: Annotated[int, Field(strict=True, ge=32)] = 32
    jwks_cache_seconds: PositiveInt = 300
    jwks_timeout_seconds: Seconds = 10
    demo_min_password_length: Annotated[int, Field(strict=True, ge=16)] = 16


class IdentityConfig(ConfigModel):
    identity: AuthConfig


class MemoryConfig(ConfigModel):
    is_enabled: StrictBool = False
    top_k: PositiveInt = 5


class TemplateConfig(ConfigModel):
    agent: NonEmptyString = "agent.md"
    soul: NonEmptyString = "soul.md"
    user: NonEmptyString = "user.md"


class PersonalDocumentsConfig(ConfigModel):
    templates: TemplateConfig = Field(default_factory=TemplateConfig)
    max_bytes: PositiveInt = 64_000


class PersonalConfig(ConfigModel):
    documents: PersonalDocumentsConfig = Field(default_factory=PersonalDocumentsConfig)
    memory: MemoryConfig = Field(default_factory=MemoryConfig)


class RemoteRequestConfig(ConfigModel):
    max_bytes: PositiveInt = 1024 * 1024


class RemoteCallbackConfig(ConfigModel):
    public_base_url: str | None = None
    public_base_url_env: SecretRef | None = None
    max_bytes: PositiveInt = 1024 * 1024
    max_clock_skew_seconds: PositiveInt = 300
    min_secret_bytes: Annotated[int, Field(strict=True, ge=32)] = 32
    poll_seconds: Seconds = 1

    @field_validator("public_base_url")
    @classmethod
    def check_public_url(cls, value):
        if value is not None:
            url = urlsplit(value)
            if (
                url.scheme not in ("http", "https")
                or not url.hostname
                or url.username is not None
                or url.password is not None
                or url.query
                or url.fragment
            ):
                raise ValueError("HTTP(S) callback base URL required")
        return value


class RemoteTimeoutConfig(ConfigModel):
    default_seconds: Seconds = 60
    max_seconds: Seconds = 300
    cancel_seconds: Seconds = 1
    websocket_close_seconds: Seconds = 0.5
    stop_seconds: Seconds = 0.75


class RemoteRetryConfig(ConfigModel):
    default_retries: NonNegativeInt = 1
    max_attempts: PositiveInt = 4
    backoff_seconds: NonNegativeSeconds = 0.1


class RemoteConfig(ConfigModel):
    request: RemoteRequestConfig = Field(default_factory=RemoteRequestConfig)
    callback: RemoteCallbackConfig = Field(default_factory=RemoteCallbackConfig)
    timeout: RemoteTimeoutConfig = Field(default_factory=RemoteTimeoutConfig)
    retry: RemoteRetryConfig = Field(default_factory=RemoteRetryConfig)


class AgentCatalogConfig(PaginationConfig):
    candidate_batch_size: PositiveInt = 50


class AgentConfig(ConfigModel):
    catalog: AgentCatalogConfig = Field(default_factory=AgentCatalogConfig)
    personal: PersonalConfig = Field(default_factory=PersonalConfig)
    remote: RemoteConfig = Field(default_factory=RemoteConfig)


class TeamInviteConfig(ConfigModel):
    max_members: PositiveInt = 16
    max_request_id_length: PositiveInt = 128


class TeamRosterConfig(ConfigModel):
    max_members: PositiveInt = 20


class TeamConfig(ConfigModel):
    invite: TeamInviteConfig = Field(default_factory=TeamInviteConfig)
    roster: TeamRosterConfig = Field(default_factory=TeamRosterConfig)


class SessionPermissionsConfig(ConfigModel):
    default_mode: PermissionMode = PermissionMode.BYPASS
    allowed_modes: list[PermissionMode] = Field(
        default_factory=lambda: [
            PermissionMode.DEFAULT,
            PermissionMode.ACCEPT_EDITS,
            PermissionMode.EXPLORE,
            PermissionMode.BYPASS,
        ]
    )


class ComposerOptionsConfig(PaginationConfig):
    default_page_size: PositiveInt = 10
    max_page_size: PositiveInt = 10
    candidate_batch_size: PositiveInt = 50
    max_query_length: PositiveInt = 500
    max_cursor_length: PositiveInt = 2048


class SessionListingConfig(ConfigModel):
    max_sessions: PositiveInt = 200


class SessionNamingConfig(ConfigModel):
    default_name: NonEmptyString = "新对话"
    max_length: PositiveInt = 80
    create_max_length: PositiveInt = 200

    @model_validator(mode="after")
    def check_name_length(self):
        if len(self.default_name) > self.create_max_length:
            raise ValueError("defaultName exceeds createMaxLength")
        return self


class SessionConfig(ConfigModel):
    permissions: SessionPermissionsConfig = Field(
        default_factory=SessionPermissionsConfig
    )
    composer: ComposerOptionsConfig = Field(default_factory=ComposerOptionsConfig)
    listing: SessionListingConfig = Field(default_factory=SessionListingConfig)
    naming: SessionNamingConfig = Field(default_factory=SessionNamingConfig)


class SkillArchiveConfig(ConfigModel):
    max_bytes: Annotated[
        int, Field(strict=True, gt=0, le=DEFAULT_SKILL_ARCHIVE_MAX_BYTES)
    ] = DEFAULT_SKILL_ARCHIVE_MAX_BYTES
    max_expanded_bytes: PositiveInt | None = None

    @property
    def expanded_bytes_limit(self) -> int:
        return (
            self.max_expanded_bytes
            if self.max_expanded_bytes is not None
            else self.max_bytes
        )


class SkillImportConfig(ConfigModel):
    lease_seconds: Seconds = 90
    max_attempts: PositiveInt = 3
    poll_seconds: Seconds = 2
    retry_backoff_seconds: Seconds = 5
    max_retry_delay_seconds: Seconds = 60
    list_limit: PositiveInt = 100


class SkillConfig(ConfigModel):
    archive: SkillArchiveConfig = Field(default_factory=SkillArchiveConfig)
    import_settings: SkillImportConfig = Field(
        default_factory=SkillImportConfig, alias="import"
    )


class ConnectorOAuthConfig(ConfigModel):
    credential_key_env: SecretRef = "AGENTKIT_MCP_CREDENTIAL_KEY"
    redirect_uri: str | None = None
    redirect_uri_env: SecretRef | None = None
    timeout_seconds: Seconds = 10
    state_lifetime_seconds: Seconds = 600
    refresh_window_seconds: NonNegativeSeconds = 15
    token_lifetime_seconds: PositiveInt = 3600
    max_token_lifetime_seconds: PositiveInt = 31_536_000


class ConnectorConfig(ConfigModel):
    oauth: ConnectorOAuthConfig = Field(default_factory=ConnectorOAuthConfig)


class ToolDefaultsConfig(ConfigModel):
    registered_ids: list[NonEmptyString] | None = None


class ToolConfig(ConfigModel):
    defaults: ToolDefaultsConfig = Field(default_factory=ToolDefaultsConfig)


class AccessConfig(ConfigModel):
    allow_private_self_use: StrictBool = True
    allow_personal_self_use: StrictBool = True


class ProjectDefaultsConfig(ConfigModel):
    name: Annotated[str, StringConstraints(strip_whitespace=True, min_length=1)] = (
        "默认项目"
    )
    max_name_length: Annotated[int, Field(strict=True, gt=0, le=128)] = 128


class ProjectConfig(ConfigModel):
    defaults: ProjectDefaultsConfig = Field(default_factory=ProjectDefaultsConfig)


class WorkspaceRuntimeConfig(ConfigModel):
    root: NonEmptyString
    mode: Literal["local", "sandbox"] = "local"
    sandbox: dict = Field(default_factory=dict)


class WorkspaceUploadConfig(ConfigModel):
    max_bytes: PositiveInt


class WorkspaceScanConfig(ConfigModel):
    max_depth: PositiveInt = 32
    max_entries: PositiveInt = 10_000
    max_path_bytes: PositiveInt = 2048
    max_total_bytes: PositiveInt = DEFAULT_WORKSPACE_TREE_BYTES
    read_chunk_bytes: PositiveInt = 1024 * 1024


class WorkspacePreviewConfig(ConfigModel):
    timeout_seconds: Seconds = 45
    max_bytes: PositiveInt = DEFAULT_WORKSPACE_UPLOAD_BYTES
    max_concurrent: PositiveInt = 2
    max_pending: PositiveInt = 4


class WorkspaceDeliveryConfig(ConfigModel):
    max_entries: PositiveInt = 10_000
    max_total_bytes: PositiveInt = DEFAULT_WORKSPACE_TREE_BYTES


class WorkspaceDownloadConfig(ConfigModel):
    secret_env: SecretRef = "AGENTKIT_DOWNLOAD_SECRET"
    min_secret_length: Annotated[int, Field(strict=True, ge=32)] = 32


class WorkspaceChangesConfig(PaginationConfig):
    pass


class WorkspaceConfig(ConfigModel):
    changes: WorkspaceChangesConfig = Field(default_factory=WorkspaceChangesConfig)
    runtime: WorkspaceRuntimeConfig
    upload: WorkspaceUploadConfig
    scan: WorkspaceScanConfig = Field(default_factory=WorkspaceScanConfig)
    preview: WorkspacePreviewConfig = Field(default_factory=WorkspacePreviewConfig)
    delivery: WorkspaceDeliveryConfig = Field(default_factory=WorkspaceDeliveryConfig)
    download: WorkspaceDownloadConfig = Field(default_factory=WorkspaceDownloadConfig)


class ModelSpec(ConfigModel):
    key: NonEmptyString
    display_name: str
    model: NonEmptyString
    credential_type: str = "openai_credential"
    model_class: str = "openai_chat"
    base_url: str | None = None
    api_key_env: SecretRef
    parameters: dict = Field(default_factory=dict)


class ModelCatalogConfig(ConfigModel):
    models: list[ModelSpec] = Field(min_length=1)


class ModelSelectionConfig(ConfigModel):
    default_key: NonEmptyString
    allow_override: StrictBool = True
    allowed_keys: list[NonEmptyString] = Field(default_factory=list)


class ModelConfig(ConfigModel):
    catalog: ModelCatalogConfig
    selection: ModelSelectionConfig


class PostgresConnectionConfig(ConfigModel):
    url_env: SecretRef = "AGENTKIT_DATABASE_URL"


class PostgresPoolConfig(ConfigModel):
    size: PositiveInt = 10
    max_overflow: NonNegativeInt = 10


class PostgresConfig(ConfigModel):
    connection: PostgresConnectionConfig = Field(
        default_factory=PostgresConnectionConfig
    )
    pool: PostgresPoolConfig = Field(default_factory=PostgresPoolConfig)


class KafkaConnectionConfig(ConfigModel):
    deployment_mode: Literal["standalone", "cluster"] = "standalone"
    bootstrap_servers: list[NonEmptyString] = Field(
        default_factory=lambda: ["127.0.0.1:9092"], min_length=1
    )
    topic_prefix: NonEmptyString = "egis-agentkit"
    consumer_group: NonEmptyString = "egis-agentkit-wakeup-v1"


class KafkaTopicConfig(ConfigModel):
    partitions: PositiveInt = 8
    replication_factor: PositiveInt = 1


class KafkaTransportConfig(ConfigModel):
    buffer_size: PositiveInt = 256
    startup_timeout_seconds: Seconds = 45
    poll_timeout_milliseconds: PositiveInt = 200
    full_buffer_backoff_seconds: Seconds = 0.05
    retry_backoff_seconds: Seconds = 0.5
    expiry_sweep_seconds: Seconds = 60
    partition_fetch_max_bytes: PositiveInt = 1024 * 1024
    fetch_max_bytes: PositiveInt = 4 * 1024 * 1024


class KafkaBroadcastConfig(ConfigModel):
    inline_max_bytes: Annotated[int, Field(strict=True, gt=0, le=1024 * 1024)] = (
        512 * 1024
    )
    max_bytes: PositiveInt = 32 * 1024 * 1024
    max_key_bytes: PositiveInt = 64 * 1024
    payload_ttl_seconds: PositiveInt = 3600
    storage_timeout_seconds: Seconds = 30


class KafkaConfig(ConfigModel):
    connection: KafkaConnectionConfig = Field(default_factory=KafkaConnectionConfig)
    topics: KafkaTopicConfig = Field(default_factory=KafkaTopicConfig)
    transport: KafkaTransportConfig = Field(default_factory=KafkaTransportConfig)
    broadcast: KafkaBroadcastConfig = Field(default_factory=KafkaBroadcastConfig)


class NacosRefreshConfig(ConfigModel):
    interval_seconds: Seconds = 10
    failure_interval_seconds: Seconds = 10


class NacosConfig(ConfigModel):
    refresh: NacosRefreshConfig = Field(default_factory=NacosRefreshConfig)


class BackendConfig(ConfigModel):
    postgres: PostgresConfig = Field(default_factory=PostgresConfig)
    kafka: KafkaConfig = Field(default_factory=KafkaConfig)
    nacos: NacosConfig = Field(default_factory=NacosConfig)


class AgentKitConfig(ConfigModel):
    revision: PositiveInt
    agent: AgentConfig = Field(default_factory=AgentConfig)
    team: TeamConfig = Field(default_factory=TeamConfig)
    skill: SkillConfig = Field(default_factory=SkillConfig)
    session: SessionConfig = Field(default_factory=SessionConfig)
    connector: ConnectorConfig = Field(default_factory=ConnectorConfig)
    tool: ToolConfig = Field(default_factory=ToolConfig)
    access: AccessConfig = Field(default_factory=AccessConfig)
    user: IdentityConfig
    project: ProjectConfig = Field(default_factory=ProjectConfig)
    workspace: WorkspaceConfig
    model: ModelConfig
    backend: BackendConfig = Field(default_factory=BackendConfig)
