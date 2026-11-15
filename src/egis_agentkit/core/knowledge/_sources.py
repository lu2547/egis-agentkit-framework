"""构建知识库引用并从原生消息恢复，保持检索和回答的可追溯关系。

核心能力
- normalize_result：保留真实文档身份，裁剪模型上下文；不猜测版本或页码。
- read_search_results：只接受同一助手消息中匹配的成功工具结果，忽略用户文本。
- KnowledgeSource：右侧文档展示和受会话保护的原件请求共用的引用契约。
"""

from typing import Literal
from uuid import uuid4

from pydantic import BaseModel, ConfigDict, Field, ValidationError

SEARCH_TOOL_NAME = "SearchKnowledge"
SEARCH_RESULT_KIND = "agentkit.knowledge_search.v1"


class KnowledgeSource(BaseModel):
    model_config = ConfigDict(extra="forbid")
    citation_id: str = Field(pattern=r"^[a-f0-9]{32}-[1-9][0-9]*$")
    index: int = Field(ge=1)
    document_id: str | None = None
    parent_id: str | None = None
    title: str
    knowledge_base: str = ""
    knowledge_base_id: str | None = None
    section: str = ""
    excerpt: str
    score: float | None = Field(default=None, ge=0, le=1, allow_inf_nan=False)
    version: str | None = None


class KnowledgeSearchResult(BaseModel):
    model_config = ConfigDict(extra="forbid")
    kind: Literal["agentkit.knowledge_search.v1"] = SEARCH_RESULT_KIND
    query: str
    label: str
    answer: str
    scope_kb_ids: list[str]
    sources: list[KnowledgeSource]
    cost_ms: int = Field(default=0, ge=0)
    citation_instructions: str = ""


def normalize_result(query, result, bases, limits, *, label):
    """引用按 Java 采纳顺序排序、按身份去重；模糊版本保持未知。"""
    sources, seen = [], set()
    search_id = uuid4().hex
    for reference in sorted(result.references, key=lambda value: value.ref_index):
        identity = (
            reference.doc_id,
            reference.parent_id,
            reference.doc_name,
            reference.title_path,
            reference.snippet,
        )
        if identity in seen:
            continue
        seen.add(identity)
        matching_bases = [base for base in bases if base.name == reference.kb]
        if not reference.kb and len(bases) == 1:
            matching_bases = bases
        kb_id = matching_bases[0].id if len(matching_bases) == 1 else None
        versions = {
            item.ver
            for item in result.items
            if item.doc == reference.doc_name and item.kb == reference.kb and item.ver
        }
        sources.append(
            KnowledgeSource(
                citation_id=f"{search_id}-{len(sources) + 1}",
                index=len(sources) + 1,
                document_id=reference.doc_id or None,
                parent_id=reference.parent_id,
                title=reference.doc_name or "知识库文档",
                knowledge_base=reference.kb,
                knowledge_base_id=kb_id,
                section=reference.title_path,
                excerpt=reference.snippet[: limits.max_excerpt_chars],
                score=reference.score,
                version=next(iter(versions)) if len(versions) == 1 else None,
            )
        )
        if len(sources) >= limits.max_sources:
            break
    return KnowledgeSearchResult(
        query=query,
        label=label,
        answer=result.answer[: limits.max_answer_chars] if sources else "",
        scope_kb_ids=[base.id for base in bases],
        sources=sources,
        cost_ms=result.cost_ms,
    )


def read_search_results(message):
    """返回 (tool_call_id, 检索结果)，供前端重放和后端原件授权读取同一记录。"""
    if message.role != "assistant":
        return []
    calls = {
        block.id
        for block in message.content
        if block.type == "tool_call" and block.name == SEARCH_TOOL_NAME
    }
    results = []
    for block in message.content:
        if (
            block.type != "tool_result"
            or block.id not in calls
            or block.state != "success"
        ):
            continue
        output = (
            block.output
            if isinstance(block.output, str)
            else "\n".join(item.text for item in block.output if item.type == "text")
        )
        try:
            payload = KnowledgeSearchResult.model_validate_json(output)
        except (ValidationError, ValueError, TypeError):
            continue
        results.append((block.id, payload))
    return results
