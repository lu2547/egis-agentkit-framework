"""定义知识库的连接、检索、范围授权和原文入口配置，统一由 Nacos 加载。

核心能力
- KnowledgeConnection：Java 地址、HMAC 环境凭证、超时和连接/响应限额。
- KnowledgeSearch：模型输入和检索返回的容量约束，不让模型修改鉴权范围。
- KnowledgeAccess：显式个人/启用组到知识库 ID 的映射，默认不授权。
- KnowledgeDocuments：浏览器访问 Java 用户认证原件接口的公开地址。
"""

from typing import Annotated, Literal
from urllib.parse import urlsplit

from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    field_validator,
    model_validator,
)
from pydantic.alias_generators import to_camel

PositiveInt = Annotated[int, Field(strict=True, gt=0)]
Seconds = Annotated[float, Field(strict=True, gt=0, allow_inf_nan=False)]
Identifier = Annotated[
    str, Field(min_length=1, max_length=32, pattern=r"^[A-Za-z0-9_-]+$")
]


class _Config(BaseModel):
    model_config = ConfigDict(
        frozen=True, extra="forbid", alias_generator=to_camel, populate_by_name=True
    )


def _http_base(value):
    if value is None:
        return value
    parsed = urlsplit(value)
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.netloc
        or parsed.username
        or parsed.password
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError("知识库地址必须是无凭证、查询参数和片段的 HTTP(S) 地址")
    return value.rstrip("/")


class KnowledgeConnection(_Config):
    base_url: str | None = None
    app_id: str | None = None
    secret_env: Annotated[str, Field(pattern=r"^[A-Za-z_][A-Za-z0-9_]*$")] = (
        "AGENTKIT_KNOWLEDGE_SECRET"
    )
    timeout_seconds: Seconds = 60.0
    connect_timeout_seconds: Seconds = 5.0
    max_connections: PositiveInt = 20
    max_response_bytes: PositiveInt = 2 * 1024 * 1024

    _validate_base = field_validator("base_url")(_http_base)


class KnowledgeSearch(_Config):
    top_k: Annotated[int, Field(strict=True, gt=0)] = 5
    threshold: Annotated[float, Field(ge=0, le=1, allow_inf_nan=False)] = 0.5
    sort: Literal["rank", "score"] = "rank"
    max_query_chars: PositiveInt = 4096
    max_sources: PositiveInt = 12
    max_excerpt_chars: PositiveInt = 4000
    max_answer_chars: PositiveInt = 16000


class KnowledgeGrant(_Config):
    principal_type: Literal["user", "group"]
    principal_id: str = Field(min_length=1, max_length=128)
    kb_ids: list[Identifier] = Field(min_length=1)


class KnowledgeAccess(_Config):
    grants: tuple[KnowledgeGrant, ...] = ()


class KnowledgeDocuments(_Config):
    public_base_url: str | None = None
    allowed_file_origins: tuple[str, ...] = ()

    @field_validator("public_base_url")
    @classmethod
    def validate_public_base(cls, value):
        if value is not None and value.startswith("/") and not value.startswith("//"):
            if "?" in value or "#" in value or ".." in value.split("/"):
                raise ValueError("知识库公开地址必须是限定路径")
            return value.rstrip("/")
        return _http_base(value)

    @field_validator("allowed_file_origins")
    @classmethod
    def validate_origins(cls, values):
        for value in values:
            _http_base(value)
            if urlsplit(value).path not in {"", "/"}:
                raise ValueError("文件源白名单只允许 HTTP(S) origin")
        return tuple(value.rstrip("/") for value in values)


class KnowledgeConfig(_Config):
    enabled: StrictBool = False
    label: str = Field(default="企业知识库", min_length=1, max_length=80)
    connection: KnowledgeConnection = Field(default_factory=KnowledgeConnection)
    search: KnowledgeSearch = Field(default_factory=KnowledgeSearch)
    access: KnowledgeAccess = Field(default_factory=KnowledgeAccess)
    documents: KnowledgeDocuments = Field(default_factory=KnowledgeDocuments)

    @model_validator(mode="after")
    def require_connection(self):
        if self.enabled and (
            not self.connection.base_url or not self.connection.app_id
        ):
            raise ValueError("启用知识库必须配置 Java 地址和 HMAC 应用 ID")
        return self
