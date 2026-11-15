"""调用现有 Java 知识库 API，不承担 LlamaIndex 检索或知识库持久化。

核心能力
- list_knowledge_bases：读取应用身份可见的库，供业务层缩小用户查询范围。
- query：精确发送 QueryRequest，解析 QueryResult；不增加私有接口字段。
- _request：逐次签名、有界读取、拒绝重定向，安全转换鉴权和协议错误。
- close：只关闭自己拥有的 HTTP 连接，便于复用和测试。
"""

import json
import time
from uuid import uuid4

import httpx
from pydantic import TypeAdapter, ValidationError

from egis_agentkit.errors import AgentKitError, Forbidden

from ._models import KnowledgeBase, QueryResult
from ._signing import sign_request


class JavaKnowledgeClient:
    def __init__(
        self, settings, *, secret, http_client=None, clock=time.time, nonce=None
    ):
        if not settings.base_url or not settings.app_id or not secret:
            raise AgentKitError("知识库应用连接或凭证未配置", 503)
        self.settings = settings
        self._secret = secret
        self._clock = clock
        self._nonce = nonce or (lambda: uuid4().hex)
        self._owns_http = http_client is None
        self.http = http_client or httpx.AsyncClient(
            timeout=httpx.Timeout(
                settings.timeout_seconds, connect=settings.connect_timeout_seconds
            ),
            limits=httpx.Limits(max_connections=settings.max_connections),
            follow_redirects=False,
        )

    async def list_knowledge_bases(self) -> list[KnowledgeBase]:
        """应用可见不代表用户可见，调用方必须继续做用户范围过滤。"""
        data = await self._request("GET", "/openapi/v1/kb")
        try:
            return TypeAdapter(list[KnowledgeBase]).validate_python(data)
        except ValidationError:
            raise AgentKitError("知识库目录响应不符合接口契约", 502) from None

    async def query(self, query, kb_ids, *, top_k, threshold, sort) -> QueryResult:
        """范围只能来自业务层已检查的知识库 ID，不接收用户身份或认证参数。"""
        body = {
            "query": query,
            "topK": top_k,
            "threshold": threshold,
            "sort": sort,
            "scope": {"kbIds": kb_ids},
        }
        data = await self._request("POST", "/openapi/v1/query", body)
        try:
            return QueryResult.model_validate(data)
        except ValidationError:
            raise AgentKitError("知识库检索响应不符合接口契约", 502) from None

    async def _request(self, method, path, payload=None):
        # 1. 固定业务路径，签名原始 UTF-8 字节；不转发浏览器的 Bearer/Cookie。
        content = (
            b""
            if payload is None
            else json.dumps(
                payload, ensure_ascii=False, separators=(",", ":"), allow_nan=False
            ).encode()
        )
        timestamp, nonce = str(int(self._clock())), self._nonce()
        headers = {
            "X-App-Id": self.settings.app_id,
            "X-Timestamp": timestamp,
            "X-Nonce": nonce,
            "X-Request-Id": uuid4().hex,
            "X-Signature": sign_request(
                self._secret, method, path, "", timestamp, nonce, content
            ),
        }
        if payload is not None:
            headers["Content-Type"] = "application/json"
        try:
            # 2. 即便外部注入的 client 开启跳转，这里也不能携带签名跟随跳转。
            async with self.http.stream(
                method,
                self.settings.base_url + path,
                content=content,
                headers=headers,
                follow_redirects=False,
                timeout=httpx.Timeout(
                    self.settings.timeout_seconds,
                    connect=self.settings.connect_timeout_seconds,
                ),
            ) as response:
                if response.status_code != 200:
                    raise AgentKitError("知识库服务请求未完成，请稍后重试", 503)
                chunks, size = [], 0
                async for chunk in response.aiter_bytes():
                    size += len(chunk)
                    if size > self.settings.max_response_bytes:
                        raise AgentKitError("知识库返回内容超过接入限额", 502)
                    chunks.append(chunk)
                envelope = json.loads(b"".join(chunks))
        except httpx.HTTPError:
            raise AgentKitError("知识库连接超时或暂时不可用", 503) from None
        except (ValueError, UnicodeError):
            raise AgentKitError("知识库响应无法解析", 502) from None
        # 3. 非零 code 是失败，不能把错误正文误当答案或泄露上游调试信息。
        if not isinstance(envelope, dict) or type(envelope.get("code")) is not int:
            raise AgentKitError("知识库响应缺少结果状态", 502)
        if envelope["code"] == 1006:
            raise Forbidden("知识库拒绝了当前检索范围")
        if envelope["code"] != 0 or envelope.get("success") is False:
            raise AgentKitError("知识库应用认证或业务请求未通过", 503)
        return envelope.get("data")

    async def close(self):
        """释放 HTTP 连接池，不关闭外部注入的共享连接。"""
        if self._owns_http:
            await self.http.aclose()
