"""管理知识库检索和回答来源访问，组合真实身份、Java 客户端和原生会话。

核心能力
- allowed_knowledge_bases：当前用户/启用组的显式授权，不给管理员隐式权限。
- search：与 Java 应用可见范围求交，查询前后重查授权，返回可追溯引用。
- document_resolver：核对会话、已完成工具结果和当前授权，再提供用户态原件入口。
"""

from urllib.parse import quote

from egis_agentkit.core.knowledge import normalize_result, read_search_results
from egis_agentkit.core.knowledge._access import resolve_knowledge_scope
from egis_agentkit.errors import AgentKitError, Forbidden, InvalidRequest, NotFound
from egis_agentkit.service._user import UserService


class KnowledgeManager:
    def __init__(self, storage, config_provider, client, *, sessions=None):
        self.storage, self.config, self.client, self.sessions = (
            storage,
            config_provider,
            client,
            sessions,
        )
        self.users = UserService(storage)

    def _settings(self):
        """连接在启动时固定，查询限额、原文入口和授权映射使用最新 Nacos 快照。"""
        return self.config.snapshot.knowledge

    @property
    def settings_label(self) -> str:
        """向页面标识当前知识库服务环境，不暴露连接地址或凭证。"""
        return self._settings().label

    async def allowed_knowledge_bases(self, user_id):
        """个人和启用组授权取并集；不存在、停用或无显式授权均不会扩大范围。"""
        settings = self._settings()
        if not settings.enabled:
            return set()
        user = await self.users.get(user_id)
        groups = await self.storage.users.enabled_group_ids(user.group_ids)
        return resolve_knowledge_scope(user.user_id, groups, settings.access.grants)

    async def _require_scope(self, user_id, kb_ids):
        allowed = await self.allowed_knowledge_bases(user_id)
        if not kb_ids or not set(kb_ids).issubset(allowed):
            raise Forbidden("未获得当前知识库范围的访问权限")

    async def list_knowledge_bases(self, user_id):
        """目录查询直接过滤未授权库；不会把应用可见范围当作用户权限。"""
        allowed = await self.allowed_knowledge_bases(user_id)
        if not allowed:
            return []
        bases = [
            base
            for base in await self.client.list_knowledge_bases()
            if base.id in allowed
        ]
        # Java 调用期间发生撤权时，不返回失效目录信息。
        current = await self.allowed_knowledge_bases(user_id)
        return [base for base in bases if base.id in current]

    async def search(self, user_id, query):
        """只执行文档规定的检索接口；返回内容不作为系统指令执行。"""
        settings = self._settings()
        query = query.strip()
        if not query or len(query) > settings.search.max_query_chars:
            raise InvalidRequest(
                f"检索问题须有 1 至 {settings.search.max_query_chars} 个字符"
            )
        # 1. 应用凭证和平台管理员角色都不能代替用户授权。
        bases = await self.list_knowledge_bases(user_id)
        kb_ids = sorted({base.id for base in bases})
        await self._require_scope(user_id, kb_ids)
        # 2. Java 继续负责 LlamaIndex 检索和该应用的 kb_scope/scopes 校验。
        result = await self.client.query(
            query,
            kb_ids,
            top_k=settings.search.top_k,
            threshold=settings.search.threshold,
            sort=settings.search.sort,
        )
        # 3. 长检索期间可能撤权，返回数据前再次校验，不泄露已失效范围。
        await self._require_scope(user_id, kb_ids)
        return normalize_result(
            query, result, bases, settings.search, label=settings.label
        )

    async def document_resolver(
        self, user_id, session_id, reply_id, call_id, source_index
    ):
        """返回 Java 用户 Cookie 接口地址，不伪造文档下载 API 或版本预览能力。"""
        user = await self.users.get(user_id)
        await self.sessions.get_session_record(user, session_id, require_use=False)
        message = await self.storage.get_message(user_id, session_id, reply_id)
        if message is None or not message.finished_at:
            raise NotFound("回复不存在或来源尚未保存")
        # 1. 文档 ID 必须来自当前会话的真实成功工具结果，不能接受客户端自报 ID。
        matching = next(
            (
                result
                for identifier, result in read_search_results(message)
                if identifier == call_id
            ),
            None,
        )
        source = (
            next(
                (value for value in matching.sources if value.index == source_index),
                None,
            )
            if matching
            else None
        )
        if source is None or not source.document_id:
            raise NotFound("此来源没有可定位的原始文档")
        # 2. Java 引用没有 kbId 时保守要求原检索范围仍完整授权，不能用名称猜权限。
        scope = (
            [source.knowledge_base_id]
            if source.knowledge_base_id
            else matching.scope_kb_ids
        )
        await self._require_scope(user_id, scope)
        documents = self._settings().documents
        if not documents.public_base_url:
            raise AgentKitError("知识库原文入口尚未配置，可先查看命中片段", 503)
        # 3. 浏览器在知识库自身域下携带 Cookie；平台 HMAC 和用户 Bearer 不外传。
        document_id = quote(source.document_id, safe="")
        path = (
            "/kb/"
            + quote(source.knowledge_base_id, safe="")
            + "/documents/"
            + document_id
            + "/raw/preview"
            if source.knowledge_base_id
            else "/query/documents/" + document_id + "/raw"
        )
        return {
            "resolver_url": documents.public_base_url + path,
            "allowed_file_origins": list(documents.allowed_file_origins),
        }
