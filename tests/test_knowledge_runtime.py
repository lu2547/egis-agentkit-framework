"""知识库使用原生 ToolBase/工具结果和额外工具回调，不改变模型调用循环。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock, Mock

import pytest


@pytest.mark.asyncio
async def test_tool_returns_native_chunks_with_grounded_sources_and_safe_failures():
    from agentscope.message import ToolResultState

    from egis_agentkit.core.knowledge import KnowledgeSearchResult
    from egis_agentkit.errors import Forbidden
    from egis_agentkit.extension.knowledge._knowledge_search_tool_impl import (
        SearchKnowledge,
    )

    manager = SimpleNamespace(
        search=AsyncMock(
            return_value=KnowledgeSearchResult(
                query="q", label="知识库", answer="", scope_kb_ids=["kb_1"], sources=[]
            )
        )
    )
    tool = SearchKnowledge(manager, "alice")
    result = await tool.call(query="q")
    assert result.state == ToolResultState.SUCCESS
    assert "agentkit.knowledge_search.v1" in result.content[0].text
    manager.search.assert_awaited_once_with("alice", "q")
    manager.search.side_effect = Forbidden("未授权")
    assert (await tool.call(query="q")).state == ToolResultState.ERROR
    assert tool.is_read_only and tool.is_concurrency_safe


@pytest.mark.asyncio
async def test_personal_tool_callback_exposes_knowledge_only_when_authorized(
    monkeypatch,
):
    from egis_agentkit.extension.runtime import _agent_tools_callback as module

    monkeypatch.setattr(
        module,
        "bound_execution_scope",
        lambda *args: SimpleNamespace(definition={"agent_type": "personal"}),
    )
    users = SimpleNamespace(
        get=AsyncMock(return_value=SimpleNamespace(user_id="alice"))
    )
    resources = SimpleNamespace(
        tool_registry=SimpleNamespace(
            resolve=AsyncMock(return_value=[]),
            is_default_tool_available=Mock(return_value=True),
        )
    )
    knowledge = SimpleNamespace(
        allowed_knowledge_bases=AsyncMock(return_value={"kb_1"})
    )
    factory = module.BoundToolFactory(
        resources=resources, users=users, knowledge=knowledge
    )
    tools = await factory("alice", "agent", "session")
    assert [tool.name for tool in tools] == ["SearchKnowledge"]
    knowledge.allowed_knowledge_bases.return_value = set()
    assert await factory("alice", "agent", "session") == []
    knowledge.allowed_knowledge_bases.return_value = {"kb_1"}
    resources.tool_registry.is_default_tool_available.return_value = False
    assert await factory("alice", "agent", "session") == []


@pytest.mark.asyncio
async def test_native_chunk_accumulation_remains_parseable_when_replayed():
    from agentscope.tool import ToolResponse

    from egis_agentkit.core.knowledge import KnowledgeSearchResult, read_search_results
    from egis_agentkit.extension.knowledge._knowledge_search_tool_impl import (
        SearchKnowledge,
    )

    manager = SimpleNamespace(
        search=AsyncMock(
            return_value=KnowledgeSearchResult(
                query="q", label="知识库", answer="", scope_kb_ids=["kb_1"], sources=[]
            )
        )
    )
    chunk = await SearchKnowledge(manager, "alice").call(query="q")
    response = ToolResponse().append_chunk(chunk)
    message = SimpleNamespace(
        role="assistant",
        content=[
            SimpleNamespace(type="tool_call", name="SearchKnowledge", id="c1"),
            SimpleNamespace(
                type="tool_result", id="c1", state="success", output=response.content
            ),
        ],
    )
    assert len(read_search_results(message)) == 1


@pytest.mark.asyncio
async def test_document_resolver_uses_persisted_source_and_current_session_authorization(
    storage, tmp_path
):
    from agentscope.message import (
        AssistantMsg,
        TextBlock,
        ToolCallBlock,
        ToolResultBlock,
    )
    from test_knowledge_access import peer, provider
    from test_message_actions import history

    from egis_agentkit.client.knowledge._models import KnowledgeBase, QueryResult
    from egis_agentkit.client.nacos._knowledge_settings import (
        KnowledgeAccess,
        KnowledgeDocuments,
        KnowledgeSearch,
    )
    from egis_agentkit.core.knowledge import normalize_result
    from egis_agentkit.errors import Forbidden, NotFound
    from egis_agentkit.service.knowledge import KnowledgeManager
    from egis_agentkit.storage.model import utcnow

    _, _, alice, bob, sessions, task, _ = await history(storage, tmp_path)
    config = provider(
        [{"principalType": "user", "principalId": "alice", "kbIds": ["kb_1"]}]
    )
    config.snapshot.knowledge = config.snapshot.knowledge.model_copy(
        update={"documents": KnowledgeDocuments(public_base_url="/knowledge")}
    )
    manager = KnowledgeManager(storage, config, peer(), sessions=sessions)
    payload = normalize_result(
        "q",
        QueryResult.model_validate(
            {
                "total": 1,
                "references": [{"refIndex": 1, "docId": "doc_1", "snippet": "来源"}],
            }
        ),
        [KnowledgeBase(id="kb_1", name="库")],
        KnowledgeSearch(),
        label="知识库",
    )
    message = AssistantMsg(
        "writer",
        [
            ToolCallBlock(id="c1", name="SearchKnowledge", input="{}"),
            ToolResultBlock(
                id="c1",
                name="SearchKnowledge",
                state="success",
                output=[TextBlock(text=payload.model_dump_json())],
            ),
        ],
        finished_at=utcnow().isoformat(),
    )
    await storage.upsert_message(alice.user_id, task["session_id"], message)
    result = await manager.document_resolver(
        "alice", task["session_id"], message.id, "c1", 1
    )
    assert result["resolver_url"] == "/knowledge/kb/kb_1/documents/doc_1/raw/preview"
    with pytest.raises(Forbidden):
        await manager.document_resolver(
            bob.user_id, task["session_id"], message.id, "c1", 1
        )
    with pytest.raises(NotFound):
        await manager.document_resolver(
            "alice", task["session_id"], message.id, "forged", 1
        )
    config.snapshot.knowledge = config.snapshot.knowledge.model_copy(
        update={"access": KnowledgeAccess()}
    )
    with pytest.raises(Forbidden):
        await manager.document_resolver(
            "alice", task["session_id"], message.id, "c1", 1
        )
