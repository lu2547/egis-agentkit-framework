"""知识库范围和可追溯回答来源，不实现检索引擎和知识库 CRUD。"""

from ._sources import (
    KnowledgeSearchResult,
    KnowledgeSource,
    normalize_result,
    read_search_results,
)

__all__ = [
    "KnowledgeSearchResult",
    "KnowledgeSource",
    "normalize_result",
    "read_search_results",
]
