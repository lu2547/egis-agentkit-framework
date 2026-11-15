"""引用仅来自已完成的知识库工具结果，版本、库和原件身份不允许猜测。"""

from types import SimpleNamespace


def test_references_preserve_document_identity_and_only_unambiguous_versions():
    from egis_agentkit.client.knowledge._models import KnowledgeBase, QueryResult
    from egis_agentkit.client.nacos._knowledge_settings import KnowledgeSearch
    from egis_agentkit.core.knowledge import normalize_result

    result = QueryResult.model_validate(
        {
            "total": 2,
            "answer": "答案",
            "items": [
                {"doc": "规定", "kb": "业务库", "ver": "v2"},
                {"doc": "规定", "kb": "业务库", "ver": "v3"},
            ],
            "references": [
                {
                    "refIndex": 1,
                    "docId": "doc_1",
                    "docName": "规定",
                    "kb": "业务库",
                    "parentId": "p1",
                    "snippet": "引用",
                }
            ],
        }
    )
    normalized = normalize_result(
        "问题",
        result,
        [KnowledgeBase(id="kb_1", name="业务库")],
        KnowledgeSearch(),
        label="知识库",
    )
    assert normalized.sources[0].document_id == "doc_1"
    assert normalized.sources[0].knowledge_base_id == "kb_1"
    assert normalized.sources[0].version is None
    assert "page" not in normalized.sources[0].model_dump()


def test_empty_retrieval_does_not_promote_an_unsupported_generated_answer():
    from egis_agentkit.client.knowledge._models import QueryResult
    from egis_agentkit.client.nacos._knowledge_settings import KnowledgeSearch
    from egis_agentkit.core.knowledge import normalize_result

    normalized = normalize_result(
        "问题",
        QueryResult(total=0, answer="无依据答案"),
        [],
        KnowledgeSearch(),
        label="知识库",
    )
    assert normalized.answer == "" and normalized.sources == []


def test_user_text_and_non_matching_or_failed_tool_results_cannot_become_sources():
    from egis_agentkit.core.knowledge import read_search_results

    output = '{"kind":"agentkit.knowledge_search.v1","query":"q","label":"知识库","answer":"a","scope_kb_ids":["kb_1"],"sources":[]}'
    call = SimpleNamespace(type="tool_call", name="SearchKnowledge", id="call-1")
    result = SimpleNamespace(
        type="tool_result", id="other-call", state="success", output=output
    )
    message = SimpleNamespace(role="assistant", content=[call, result])
    assert read_search_results(message) == []
    result.id = "call-1"
    result.state = "error"
    assert read_search_results(message) == []
    result.state = "success"
    message.role = "user"
    assert read_search_results(message) == []
    message.role = "assistant"
    assert read_search_results(message)[0][0] == "call-1"


def test_multiple_searches_have_distinct_citation_ids():
    from egis_agentkit.client.knowledge._models import QueryResult
    from egis_agentkit.client.nacos._knowledge_settings import KnowledgeSearch
    from egis_agentkit.core.knowledge import normalize_result

    result = QueryResult.model_validate(
        {
            "total": 1,
            "references": [{"refIndex": 1, "docId": "doc_1", "snippet": "原文"}],
        }
    )
    first = normalize_result("q1", result, [], KnowledgeSearch(), label="知识库")
    second = normalize_result("q2", result, [], KnowledgeSearch(), label="知识库")
    assert first.sources[0].citation_id != second.sources[0].citation_id
