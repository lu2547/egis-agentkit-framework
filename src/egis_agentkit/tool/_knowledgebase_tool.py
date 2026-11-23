"""通过知识库的 hybrid SSE 接口检索文档和 Wiki。"""

import asyncio
import json
from collections.abc import AsyncIterator
from contextlib import nullcontext

import httpx
from agentscope.message import TextBlock, ToolResultState
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import ToolBase, ToolChunk
from pydantic import SecretStr

from egis_agentkit.client.nacos._settings import KnowledgeBaseToolConfig

_SOURCES = {"doc": "文档", "wiki": "Wiki"}
_ERRORS = {
    "SERVICE_UNAVAILABLE": "知识库检索服务暂不可用。",
    "INVALID_KB_IDS": "知识库范围无效或不可访问。",
}


async def _search_events(response: httpx.Response) -> AsyncIterator[tuple[str, dict]]:
    event = ""
    data = []
    response.encoding = "utf-8"
    async for line in response.aiter_lines():
        line = line.removeprefix("\ufeff")
        if not line:
            if event in ("result", "done") and data:
                value = json.loads("\n".join(data))
                if not isinstance(value, dict):
                    raise ValueError("Invalid knowledge base event")
                yield event, value
            event, data = "", []
            continue
        field, _, value = line.partition(":")
        value = value.removeprefix(" ")
        if field == "event":
            event = value
        elif field == "data":
            data.append(value)


def _final_result(results: dict[str, dict]) -> ToolChunk:
    sections = []
    references = []
    answers = []
    for source, label in _SOURCES.items():
        if source not in results:
            continue
        result = results[source]
        answer = result.get("answer")
        if isinstance(answer, str) and answer.strip():
            answers.append({"source": source, "answer": answer})
            sections.append(f"【{label}】\n{answer}")
        items = result.get("items")
        if not isinstance(items, list):
            raise ValueError("Invalid knowledge base items")
        for item in items:
            if not isinstance(item, dict):
                raise ValueError("Invalid knowledge base item")
            title = item.get("doc") or item.get("kb") or label
            if not isinstance(title, str):
                raise ValueError("Invalid knowledge base title")
            snippet = item.get("full") or item.get("match") or ""
            if not isinstance(snippet, str):
                raise ValueError("Invalid knowledge base snippet")
            snippet = snippet[:1200]
            index = len(references) + 1
            locator = {
                key: value
                for key in (
                    "kb",
                    "kb_id",
                    "doc_id",
                    "node_id",
                    "parent_id",
                    "ver",
                    "cpath",
                    "chunk",
                )
                if isinstance(value := item.get(key), str) and value
            }
            references.append(
                {
                    "index": index,
                    "source": source,
                    "title": title,
                    "snippet": snippet,
                    **locator,
                }
            )
    if references:
        sections.append(
            "参考来源\n"
            + "\n".join(
                f"[{ref['index']}] {ref['title']}\n{ref['snippet']}"
                for ref in references
            )
        )
    if not sections:
        return _failure("知识库检索没有返回可用结果。")
    return ToolChunk(
        content=[TextBlock(text="\n\n".join(sections))],
        state=ToolResultState.SUCCESS,
        metadata={"knowledge_search": {"answers": answers, "references": references}},
    )


def _failure(message: str) -> ToolChunk:
    return ToolChunk(content=[TextBlock(text=message)], state=ToolResultState.ERROR)


class KnowledgeBaseSearch(ToolBase):
    name = "KnowledgeBaseSearch"
    description = "检索知识库中的文档和 Wiki，返回检索答案和参考来源。"
    is_concurrency_safe = True
    is_read_only = True
    input_schema = {
        "type": "object",
        "properties": {
            "query": {"type": "string", "minLength": 1},
            "topK": {"type": "integer", "minimum": 1, "maximum": 50, "default": 5},
            "threshold": {"type": "number", "minimum": 0, "maximum": 1, "default": 0.5},
            "scope": {
                "type": "object",
                "properties": {
                    key: {"type": "array", "items": {"type": "string", "minLength": 1}}
                    for key in ("kbIds", "tagIds", "docIds", "wikiIds")
                },
                "additionalProperties": False,
            },
        },
        "required": ["query"],
        "additionalProperties": False,
    }

    def __init__(
        self,
        config: KnowledgeBaseToolConfig,
        *,
        access_token: SecretStr | None = None,
        http_client: httpx.AsyncClient | None = None,
    ):
        super().__init__()
        self.config = config
        self._access_token = access_token
        self.http_client = http_client

    async def check_permissions(self, tool_input, context):
        return PermissionDecision(PermissionBehavior.ALLOW)

    async def call(
        self,
        *,
        query: str,
        topK: int = 5,
        threshold: float = 0.5,
        scope: dict | None = None,
    ) -> ToolChunk:
        if not query.strip():
            return _failure("请输入要检索的问题。")
        if self.config.query_url is None:
            return _failure("知识库检索地址尚未配置。")
        if not self._access_token:
            return _failure("当前执行缺少已验证的登录凭据，无法访问知识库。")
        request = {"query": query.strip(), "topK": topK, "threshold": threshold}
        if scope:
            request["scope"] = scope
        headers = {
            "Accept": "text/event-stream",
            "Authorization": f"Bearer {self._access_token.get_secret_value()}",
        }
        results = {}
        error = None
        client_context = (
            nullcontext(self.http_client)
            if self.http_client is not None
            else httpx.AsyncClient(timeout=self.config.timeout_seconds)
        )
        try:
            async with (
                asyncio.timeout(self.config.timeout_seconds),
                client_context as client,
            ):
                async with client.stream(
                    "POST",
                    self.config.query_url,
                    json=request,
                    headers=headers,
                    follow_redirects=False,
                ) as response:
                    response.raise_for_status()
                    if (
                        response.headers.get("content-type", "")
                        .partition(";")[0]
                        .strip()
                        .lower()
                        != "text/event-stream"
                    ):
                        return _failure("知识库接口未返回 SSE 数据。")
                    async for event, value in _search_events(response):
                        if event == "done":
                            if value.get("status") == "ok" and results:
                                return _final_result(results)
                            return _failure(error or "知识库检索没有返回可用结果。")
                        source = value.get("source")
                        if value.get("status") == "failed":
                            error = _ERRORS.get(
                                value.get("error_code"), "知识库检索失败。"
                            )
                        elif value.get("status") == "ok" and source in _SOURCES:
                            results[source] = value
        except (httpx.TimeoutException, TimeoutError):
            return _failure("知识库检索超时。")
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code in (401, 403):
                return _failure("知识库鉴权失败或当前凭据没有检索权限。")
            return _failure(f"知识库检索失败（HTTP {exc.response.status_code}）。")
        except httpx.RequestError:
            return _failure("无法连接知识库服务。")
        except (TypeError, ValueError):
            return _failure("知识库返回了无效的 SSE 数据。")
        return _failure(error or "知识库连接在检索完成前断开。")
