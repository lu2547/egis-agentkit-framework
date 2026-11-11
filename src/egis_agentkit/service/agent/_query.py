"""读取单条 Agent 定义、版本历史和按类型分页的定义列表。

核心能力
- 单条详情和执行入口读取各自获准访问的版本。
- 游标校验确保同一用户沿用相同的类型筛选继续翻页。
- 候选查询由存储按创建时间和 ID 分批读取。
- 权限过滤后按用户实际可见的版本区分本地和远程专家。
"""

import base64
import json
from datetime import datetime

from egis_agentkit.backend.nacos._settings import AgentCatalogConfig
from egis_agentkit.errors import Forbidden, InvalidRequest, NotFound
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.storage.model._definition import EXPERT_TYPES

CATALOG_TYPES = EXPERT_TYPES
AGENT_TYPES = {*CATALOG_TYPES, "personal", "orchestrator"}


class AgentQuery:
    """组合数据库候选页和用户权限，返回管理页所需的定义列表。"""

    def __init__(self, storage, policies=None, *, config_provider=None):
        self.storage, self.config = storage, config_provider
        self.repository = storage.definitions
        self.permissions = PermissionService(storage, policies)

    async def read(self, user, identifier, view="private", *, version=None):
        """读取专家编辑或详情页使用的一条定义。"""
        source = await self._expert(identifier)
        return await self._read_visible(user, source, view, version=version)

    async def read_for_use(
        self, user, identifier, view, *, version=None, permissions=None
    ):
        """校验专家的使用权限，读取该授权指向的配置版本。"""
        permissions = self.permissions if permissions is None else permissions
        await self._expert(identifier)
        access = await permissions.require(
            user, "agent", identifier, view, version=version
        )
        return await self.repository.read_view(
            identifier, view, version=access.resource_version, **access.source_options
        )

    async def versions(self, user, identifier):
        """读取调用者有权查看的专家版本引用。"""
        source = await self._expert(identifier)
        result = await self.permissions.definition_versions(user, source)
        if not result:
            raise Forbidden()
        return result

    async def _expert(self, identifier):
        source = await self.repository.get_definition(identifier)
        if (
            source["definition_type"] != "agent"
            or source["agent_type"] not in EXPERT_TYPES
        ):
            raise NotFound("资源类型不匹配")
        if not source["is_enabled"]:
            raise Forbidden("资源已停用")
        return source

    async def _read_visible(
        self, user, source, view=None, *, version=None, permissions=None
    ):
        permissions = self.permissions if permissions is None else permissions
        access = await permissions.definition_access(
            user, source, view, version=version
        )
        value = await self.repository.read_view(
            source["id"],
            access.view,
            version=version or access.decision.resource_version,
            **access.decision.source_options,
        )
        share = await permissions.share_availability(
            user,
            value["definition_type"],
            value["id"],
            version=value["resource_version"],
            view=access.view,
        )
        editable = access.is_editable and (
            value["agent_type"] != "remote" or user.role_type == "admin"
        )
        return {
            **value,
            **share,
            "resource_view_type": access.view,
            "is_editable": editable,
            "is_deletable": editable,
            "is_usable": access.decision.is_allowed,
        }

    async def list(self, user, *, types=CATALOG_TYPES, cursor=None, limit=None):
        """返回当前可见的定义和下一页游标。"""
        configuration = self.config.snapshot if self.config else None
        pagination = (
            configuration.agent.catalog if configuration else AgentCatalogConfig()
        )
        permissions = (
            PermissionService(self.storage, configuration.access)
            if configuration is not None
            else self.permissions
        )
        limit = pagination.default_page_size if limit is None else limit
        # 校验筛选条件和游标范围，再映射到数据库中的 Agent 类型。
        if not types or set(types) - AGENT_TYPES:
            raise ValueError("无效的 Agent 类型")
        if not 1 <= limit <= pagination.max_page_size:
            raise InvalidRequest(
                f"分页大小必须介于 1 和 {pagination.max_page_size} 之间"
            )
        scope = [user.user_id, sorted(set(types))]
        after = self._decode_cursor(cursor, scope) if cursor is not None else None
        items = []
        batch_size = pagination.candidate_batch_size
        last_visible = None
        # 分批扫描候选定义，按权限和可见版本筛出当前页。
        while True:
            rows, states = await self.repository.page_agents(
                types, after=after, limit=batch_size
            )
            for row in rows:
                after = (row["created_at"], row["id"])
                try:
                    value = await self._read_visible(user, row, permissions=permissions)
                except (Forbidden, NotFound):
                    continue
                # 确认还有一条可见记录后，再从当前页末尾生成下一页游标。
                if len(items) == limit:
                    return {
                        "items": items,
                        "next_cursor": self._encode_cursor(scope, last_visible),
                    }
                share_status = (
                    "approved"
                    if value["resource_view_type"] == "shared"
                    else states.get((row["id"], value["resource_version"]), "private")
                )
                items.append({**value, "share_status_type": share_status})
                last_visible = after
            if len(rows) < batch_size:
                return {"items": items, "next_cursor": None}

    @staticmethod
    def _encode_cursor(scope, position):
        """保存筛选范围和最后一条已返回记录的位置。"""
        created_at, identifier = position
        data = json.dumps([scope, created_at.isoformat(), identifier]).encode()
        return base64.urlsafe_b64encode(data).decode()

    @staticmethod
    def _decode_cursor(cursor, scope):
        """校验游标属于当前查询，并还原数据库排序位置。"""
        try:
            saved_scope, timestamp, identifier = json.loads(
                base64.b64decode(cursor, altchars=b"-_", validate=True)
            )
            position = datetime.fromisoformat(timestamp)
            if saved_scope == scope and position.tzinfo and isinstance(identifier, str):
                return position, identifier
        except (ValueError, TypeError):
            pass
        raise ValueError("无效的 Agent 分页游标")


async def select_definition(permissions, user, kind, identifier, view, *, version=None):
    """新会话或邀请选用获准的当前定义，并检查固定绑定。"""
    access = await permissions.require(user, kind, identifier, view, version=version)
    root = await permissions.storage.definitions.read_view(
        identifier, view, version=access.resource_version, **access.source_options
    )
    await permissions.require_execution(user, root)
    return root
