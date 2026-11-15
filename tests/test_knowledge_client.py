"""Java 文档第 8/11 节的请求、签名和失败边界；不连接业务库。"""

import hashlib
import hmac
import json

import httpx
import pytest


def connection():
    from egis_agentkit.client.nacos._knowledge_settings import KnowledgeConnection

    return KnowledgeConnection(
        base_url="https://kb.example/knowledge",
        app_id="agentkit",
        secret_env="TEST_KB_SECRET",
    )


@pytest.mark.asyncio
async def test_query_signs_exact_utf8_body_and_excludes_servlet_context():
    from egis_agentkit.client.knowledge import JavaKnowledgeClient

    observed = []

    def handler(request):
        observed.append(request)
        assert request.url.path == "/knowledge/openapi/v1/query"
        assert json.loads(request.content) == {
            "query": "缴费规则？",
            "topK": 5,
            "threshold": 0.5,
            "sort": "rank",
            "scope": {"kbIds": ["kb_1"]},
        }
        canonical = "\n".join(
            [
                "POST",
                "/openapi/v1/query",
                "",
                "1700000000",
                "fixed-nonce",
                hashlib.sha256(request.content).hexdigest(),
            ]
        )
        assert (
            request.headers["X-Signature"]
            == hmac.new(
                b"test-only-secret", canonical.encode(), hashlib.sha256
            ).hexdigest()
        )
        assert request.headers["X-App-Id"] == "agentkit"
        assert (
            "authorization" not in request.headers and "cookie" not in request.headers
        )
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": {
                    "total": 1,
                    "highRelevance": 1,
                    "costMs": 8,
                    "items": [],
                    "answer": "规则",
                    "references": [
                        {
                            "ref_index": 1,
                            "doc_id": "doc_1",
                            "doc_name": "规定",
                            "snippet": "原文",
                            "parent_id": "p1",
                            "score": 0.9,
                        }
                    ],
                },
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        client = JavaKnowledgeClient(
            connection(),
            secret="test-only-secret",
            http_client=http,
            clock=lambda: 1700000000,
            nonce=lambda: "fixed-nonce",
        )
        result = await client.query(
            "缴费规则？", ["kb_1"], top_k=5, threshold=0.5, sort="rank"
        )
        assert result.references[0].doc_id == "doc_1"
        assert result.references[0].ref_index == 1
        assert len(observed) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "response",
    [
        httpx.Response(
            200, json={"code": 1006, "message": "secret-private-upstream", "data": None}
        ),
        httpx.Response(200, text="<html>secret-private-upstream</html>"),
        httpx.Response(302, headers={"location": "https://evil.example"}),
        httpx.Response(
            200, json={"code": 0, "data": {"total": 1, "references": "invalid"}}
        ),
    ],
)
async def test_upstream_failures_are_safe_and_never_follow_redirects(response):
    from egis_agentkit.client.knowledge import JavaKnowledgeClient
    from egis_agentkit.errors import AgentKitError

    requests = []

    def handler(request):
        requests.append(request)
        return response

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(handler), follow_redirects=True
    ) as http:
        client = JavaKnowledgeClient(
            connection(), secret="test-only-secret", http_client=http
        )
        with pytest.raises(AgentKitError) as error:
            await client.query("问题", ["kb_1"], top_k=5, threshold=0.5, sort="rank")
        assert "secret-private-upstream" not in str(error.value)
        assert len(requests) == 1


@pytest.mark.asyncio
async def test_oversize_response_is_rejected_before_json_decode():
    from egis_agentkit.client.knowledge import JavaKnowledgeClient
    from egis_agentkit.errors import AgentKitError

    config = connection().model_copy(update={"max_response_bytes": 20})
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda _: httpx.Response(200, content=b"x" * 21))
    ) as http:
        client = JavaKnowledgeClient(
            config, secret="test-only-secret", http_client=http
        )
        with pytest.raises(AgentKitError):
            await client.list_knowledge_bases()


def test_enabled_knowledge_requires_a_bounded_authenticated_connection():
    from pydantic import ValidationError

    from egis_agentkit.client.nacos._knowledge_settings import (
        KnowledgeConfig,
        KnowledgeConnection,
    )

    with pytest.raises(ValidationError):
        KnowledgeConfig(enabled=True)
    with pytest.raises(ValidationError):
        KnowledgeConnection(base_url="https://user:password@kb.example/knowledge")
    with pytest.raises(ValidationError):
        KnowledgeConnection(base_url="file:///etc/passwd")


@pytest.mark.asyncio
async def test_java_nullable_description_and_empty_get_body_follow_contract():
    from egis_agentkit.client.knowledge import JavaKnowledgeClient

    def handler(request):
        assert request.method == "GET" and request.content == b""
        assert request.url.path == "/knowledge/openapi/v1/kb"
        return httpx.Response(
            200,
            json={
                "code": 0,
                "data": [{"id": "kb_1", "name": "知识库", "description": None}],
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as http:
        result = await JavaKnowledgeClient(
            connection(), secret="test-only-secret", http_client=http
        ).list_knowledge_bases()
        assert result[0].id == "kb_1"
