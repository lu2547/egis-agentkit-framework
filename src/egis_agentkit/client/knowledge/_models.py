"""按 Java QueryResult 和 ReferenceItem 契约解析，不添加上游未提供的字段。"""

from pydantic import AliasChoices, BaseModel, ConfigDict, Field


class _WireModel(BaseModel):
    model_config = ConfigDict(extra="ignore", populate_by_name=True)


class KnowledgeBase(_WireModel):
    id: str = Field(min_length=1, max_length=32)
    name: str
    category: str = ""
    owner_id: str = Field(default="", alias="ownerId")
    description: str | None = None


class RecallItem(_WireModel):
    rank: int | None = None
    score: float | None = Field(default=None, ge=0, le=1, allow_inf_nan=False)
    kb: str = ""
    doc: str = ""
    ver: str = ""
    cpath: str = ""
    chunk: str = ""
    match: str = ""
    full: str = ""


class ReferenceItem(_WireModel):
    ref_index: int = Field(ge=1, validation_alias=AliasChoices("refIndex", "ref_index"))
    parent_id: str | None = Field(
        default=None, validation_alias=AliasChoices("parentId", "parent_id")
    )
    doc_id: str | None = Field(
        default=None, max_length=32, validation_alias=AliasChoices("docId", "doc_id")
    )
    doc_name: str = Field(
        default="", validation_alias=AliasChoices("docName", "doc_name")
    )
    kb: str = ""
    title_path: str = Field(
        default="", validation_alias=AliasChoices("titlePath", "title_path")
    )
    snippet: str = ""
    score: float | None = Field(default=None, ge=0, le=1, allow_inf_nan=False)


class QueryResult(_WireModel):
    total: int = Field(ge=0)
    high_relevance: int = Field(
        default=0,
        ge=0,
        validation_alias=AliasChoices("highRelevance", "high_relevance"),
    )
    cost_ms: int = Field(
        default=0, ge=0, validation_alias=AliasChoices("costMs", "cost_ms")
    )
    items: list[RecallItem] = Field(default_factory=list)
    answer: str = ""
    references: list[ReferenceItem] = Field(default_factory=list)
