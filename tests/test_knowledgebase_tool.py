import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest
from agentscope.agent import Agent
from agentscope.app.deps import get_chat_service
from agentscope.app._service import ChatService
from agentscope.app.message_bus import MessageBusKeys
from agentscope.event import ToolResultEndEvent
from agentscope.formatter import OpenAIChatFormatter
from agentscope.message import TextBlock, ToolCallBlock, ToolResultState, UserMsg
from agentscope.model import ChatModelBase, ChatResponse
from agentscope.tool import FunctionTool, Toolkit
from egis_agentkit_app.bootstrap import build_app
from identity_helpers import configure_test_issuer, signed_token
from pydantic import SecretStr
from test_app_http import Config, app_snapshot
from test_domain import identities

from egis_agentkit.bootstrap._init import init_agentkit
from egis_agentkit.client.nacos import NacosBootstrap, NacosClient
from egis_agentkit.client.nacos._settings import KnowledgeBaseToolConfig
from egis_agentkit.core.session._context import verified_chat_authorization
from egis_agentkit.service.session._preparation import SessionPreparationService
from egis_agentkit.tool._knowledgebase_tool import KnowledgeBaseSearch


def sse(event, value):
    return f"event: {event}\ndata: {json.dumps(value, ensure_ascii=False)}\n\n"


async def test_agent_dispatches_knowledge_search_with_another_read_only_tool():
    class SearchModel(ChatModelBase):
        formatter = OpenAIChatFormatter()

        async def __call__(self, messages, tools=None, tool_choice=None):
            if any(message.has_content_blocks("tool_result") for message in messages):
                return ChatResponse(is_last=True, content=[TextBlock(text="检索完成")])
            return ChatResponse(
                is_last=True,
                content=[
                    ToolCallBlock(id=name, name=name, input='{"query":"缴费规则"}')
                    for name in ("KnowledgeBaseSearch", "memory_search")
                ],
            )

    async def memory_search(query: str) -> str:
        """Find memories about the query."""
        return "未找到相关记忆"

    def respond(request):
        assert request.headers["authorization"] == "Bearer test-user-jwt"
        return httpx.Response(
            200,
            text=(
                sse(
                    "result",
                    {
                        "source": "doc",
                        "status": "ok",
                        "answer": "缴费规则",
                        "items": [],
                    },
                )
                + sse("done", {"status": "ok"})
            ),
            headers={"Content-Type": "text/event-stream"},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        search = KnowledgeBaseSearch(
            KnowledgeBaseToolConfig(query_url="https://knowledge.example.com/query"),
            access_token=SecretStr("test-user-jwt"),
            http_client=client,
        )
        agent = Agent(
            name="search-agent",
            system_prompt="搜索测试助手",
            model=SearchModel(
                None, "search-test", SearchModel.Parameters(), stream=False
            ),
            toolkit=Toolkit(
                tools=[search, FunctionTool(memory_search, is_read_only=True)]
            ),
        )
        results = {
            event.tool_call_id: event.state
            async for event in agent.reply_stream(
                UserMsg(name="user", content="缴费规则")
            )
            if isinstance(event, ToolResultEndEvent)
        }
    assert results == {
        "KnowledgeBaseSearch": ToolResultState.SUCCESS,
        "memory_search": ToolResultState.SUCCESS,
    }


async def query_tool(stream, *, http_status=200):
    def respond(request):
        assert request.method == "POST"
        assert str(request.url) == "https://knowledge.example.com/knowledge/query"
        assert request.headers["accept"] == "text/event-stream"
        assert request.headers["authorization"] == "Bearer test-user-jwt"
        assert json.loads(request.content) == {
            "query": "缴费规则",
            "topK": 5,
            "threshold": 0.5,
            "scope": {"kbIds": ["pension"]},
        }
        return httpx.Response(
            http_status, text=stream, headers={"content-type": "text/event-stream"}
        )

    config = KnowledgeBaseToolConfig(
        query_url="https://knowledge.example.com/knowledge/query"
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        tool = KnowledgeBaseSearch(
            config, access_token=SecretStr("test-user-jwt"), http_client=client
        )
        return await tool.call(query="缴费规则", scope={"kbIds": ["pension"]})


async def test_hybrid_sse_combines_both_results_with_numbered_references():
    frames = [
        ": ping\n\n",
        sse("stage", {"source": "doc", "message": "正在召回"}),
        sse(
            "result",
            {"status": "ok", "source": "wiki", "answer": "Wiki 答案", "items": []},
        ),
        sse(
            "result",
            {
                "status": "ok",
                "source": "doc",
                "answer": "文档答案",
                "items": [
                    {
                        "kb": "年金库",
                        "kb_id": "pension",
                        "doc": "年金政策",
                        "doc_id": "document",
                        "node_id": "node",
                        "full": "缴费依据",
                    }
                ],
            },
        ),
        sse("done", {"status": "ok", "sources": ["doc", "wiki"]}),
    ]
    result = await query_tool("".join(frames))
    assert result.state == ToolResultState.SUCCESS
    assert "文档答案" in result.content[0].text
    assert "Wiki 答案" in result.content[0].text
    assert "[1] 年金政策" in result.content[0].text
    assert "正在召回" not in result.content[0].text
    assert result.metadata["knowledge_search"]["references"] == [
        {
            "index": 1,
            "source": "doc",
            "title": "年金政策",
            "kb": "年金库",
            "kb_id": "pension",
            "doc_id": "document",
            "node_id": "node",
            "snippet": "缴费依据",
        }
    ]


@pytest.mark.parametrize(
    "frames",
    [
        sse(
            "result",
            {"status": "failed", "source": "doc", "error_code": "SERVICE_UNAVAILABLE"},
        ),
        sse(
            "result",
            {"status": "ok", "source": "doc", "answer": "尚未结束", "items": []},
        ),
        sse("done", {"status": "failed", "sources": []}),
        sse("result", {"status": "ok", "source": "doc", "answer": "", "items": []})
        + sse("done", {"status": "ok", "sources": ["doc"]}),
    ],
)
async def test_incomplete_or_failed_stream_produces_a_terminal_error(frames):
    result = await query_tool(frames)
    assert result.state == ToolResultState.ERROR
    assert result.content[0].text


async def test_knowledgebase_authentication_failure_is_a_tool_error():
    result = await query_tool("", http_status=401)
    assert result.state == ToolResultState.ERROR
    assert "鉴权失败" in result.content[0].text


async def test_knowledgebase_never_sends_an_unsigned_request():
    def respond(request):
        pytest.fail("a knowledge base request requires a verified user JWT")

    config = KnowledgeBaseToolConfig(query_url="https://knowledge.example.com/query")
    async with httpx.AsyncClient(transport=httpx.MockTransport(respond)) as client:
        result = await KnowledgeBaseSearch(config, http_client=client).call(
            query="缴费规则"
        )
    assert result.state == ToolResultState.ERROR
    assert "登录凭据" in result.content[0].text


@pytest.mark.parametrize("queued", [False, True])
async def test_verified_chat_jwt_reaches_knowledge_search_from_native_background_run(
    storage, tmp_path, monkeypatch, queued
):
    monkeypatch.setenv(
        "TEST_DOWNLOAD", "download-secret-with-more-than-thirty-two-bytes"
    )
    monkeypatch.setenv("TEST_MODEL_KEY", "test-only-unused-model-key")
    configure_test_issuer(monkeypatch)
    _, _, alice, _ = await identities(storage)
    token = signed_token(alice.user_id, aud=["egis-agentkit"])
    bearer_verified = []

    def query(request):
        matched = request.headers.get("authorization") == f"Bearer {token}"
        bearer_verified.append(matched)
        if not matched:
            return httpx.Response(401)
        return httpx.Response(
            200,
            text=(
                sse(
                    "result",
                    {
                        "status": "ok",
                        "source": "doc",
                        "answer": "缴费依据",
                        "items": [],
                    },
                )
                + sse("done", {"status": "ok", "sources": ["doc"]})
            ),
            headers={"Content-Type": "text/event-stream"},
        )

    config = Config(
        app_snapshot(
            tmp_path,
            tool={
                "knowledge_base": {"query_url": "https://knowledge.example.com/query"}
            },
        )
    )
    completed = asyncio.get_running_loop().create_future()
    async with (
        NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos,
        httpx.AsyncClient(transport=httpx.MockTransport(query)) as knowledge_client,
    ):
        app = await build_app(
            config, nacos, engine=storage.engine, manage_logging=False
        )
        kit = app.state.egis_kit

        async def run(
            *,
            user_id,
            agent_id,
            session_id,
            input_msg,
            client_external_tools=None,
            client_builtin_tools=None,
            queue_item_id=None,
            input_persisted=False,
        ):
            # 后台 FIFO 消费者不继承提交 HTTP 请求的 ContextVar。
            marker = verified_chat_authorization.set(None) if queued else None
            try:
                async with kit.bus.acquire_lock(MessageBusKeys.session_lock(session_id)):
                    (tool,) = await kit.tools(user_id, agent_id, session_id)
                    tool.http_client = knowledge_client
                    completed.set_result(await tool.call(query="缴费规则"))
            except Exception as exc:
                completed.set_exception(exc)
            finally:
                if marker is not None:
                    verified_chat_authorization.reset(marker)

        app.dependency_overrides[get_chat_service] = lambda: SimpleNamespace(run=run)
        monkeypatch.setattr(ChatService, "run", lambda _self, **kwargs: run(**kwargs))
        async with app.router.lifespan_context(app):
            personal = await kit.services.personal.get_or_add_definition(alice)
            task = await kit.services.sessions.add_session(
                alice, personal["id"], "private"
            )
            payload = {
                "agent_id": task["agent_id"],
                "session_id": task["session_id"],
                "input": UserMsg(name="user", content="缴费规则").model_dump()
                if queued
                else None,
            }
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test"
            ) as browser:
                response = await browser.post("/chat/", json=payload)
                assert response.status_code == 401
                rejected = signed_token(alice.user_id, aud="untrusted-audience")
                response = await browser.post(
                    "/chat/",
                    json=payload,
                    headers={"Authorization": f"Bearer {rejected}"},
                )
                assert response.status_code == 401
                selection = await browser.post("/manager/tools/KnowledgeBaseSearch/install")
                assert selection.status_code == 401
                selection = await browser.post(
                    "/manager/tools/KnowledgeBaseSearch/install",
                    headers={"Authorization": f"Bearer {token}"},
                )
                assert selection.status_code == 200, selection.text
                assert selection.json()["is_enabled"]
                response = await browser.post(
                    "/chat/", json=payload, headers={"Authorization": f"Bearer {token}"}
                )
                assert response.status_code == 200, response.text
                result = await asyncio.wait_for(completed, timeout=5)
                assert result.state == ToolResultState.SUCCESS
                assert bearer_verified == [True]
            await kit.services.sessions.authorizations.discard(task["session_id"])
            async with kit.bus.coordinator.bind(task["session_id"]):
                (tool,) = await kit.tools(
                    alice.user_id, task["agent_id"], task["session_id"]
                )
                tool.http_client = knowledge_client
                assert (
                    await tool.call(query="后台执行")
                ).state == ToolResultState.ERROR
                assert bearer_verified == [True]


async def test_hybrid_sse_keeps_successful_source_when_the_other_fails():
    frames = (
        sse(
            "result",
            {"status": "failed", "source": "wiki", "error_code": "INTERNAL_ERROR"},
        )
        + sse(
            "result",
            {"status": "ok", "source": "doc", "answer": "文档答案", "items": []},
        )
        + sse("done", {"status": "ok", "sources": ["doc"]})
    )
    result = await query_tool(frames)
    assert result.state == ToolResultState.SUCCESS
    assert "文档答案" in result.content[0].text


async def test_bootstrap_exposes_configured_tool_after_personal_selection(
    storage, tmp_path, monkeypatch
):
    monkeypatch.setenv("TEST_MODEL_KEY", "test-only-unused-model-key")
    _, _, alice, _ = await identities(storage)
    snapshot = app_snapshot(
        tmp_path,
        tool={
            "knowledge_base": {"query_url": "https://knowledge.example.com/query"},
        },
    )
    config = SimpleNamespace(snapshot=snapshot)
    kit = init_agentkit(
        config,
        None,
        engine=storage.engine,
        manage_logging=False,
        sandbox_client_factory=lambda: pytest.fail(
            "tool registration does not provision a sandbox"
        ),
    )
    async with kit.services.sessions.storage:
        await kit.services.resources.seed_builtin_tools()
        personal = await kit.services.personal.get_or_add_definition(alice)
        task = await kit.services.sessions.add_session(alice, personal["id"], "private")
        await kit.services.resources.install(alice, "tool", "KnowledgeBaseSearch")
        async with SessionPreparationService(storage, tmp_path, config).bind(
            task["session_id"]
        ) as scope:
            (tool,) = await kit.tools(scope.user_id, scope.agent_id, scope.session_id)
            assert isinstance(tool, KnowledgeBaseSearch)
            assert await kit.services.resources.tool_registry.authorize(
                alice, scope, None, tool
            )
        config.snapshot = app_snapshot(tmp_path)
        async with SessionPreparationService(storage, tmp_path, config).bind(
            task["session_id"]
        ) as scope:
            assert (
                await kit.tools(scope.user_id, scope.agent_id, scope.session_id) == []
            )
