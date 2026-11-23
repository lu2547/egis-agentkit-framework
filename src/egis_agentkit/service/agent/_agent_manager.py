"""向 App 提供专家管理以及受权限约束的定义查询。

核心能力
- add_definition / update_definition / delete_definition 管理专家及关联授权。
- get_definition / list_versions 读取当前用户可访问的配置。
- page_definitions 将访问范围、版本、搜索和分页交给 Storage 在数据库内完成。
"""

from __future__ import annotations

import base64
import json
from datetime import datetime
from hashlib import sha256
from typing import Any, Literal, Sequence

from egis_agentkit.client.nacos import NacosConfigProvider
from egis_agentkit.client.nacos._config import LivePolicies
from egis_agentkit.client.nacos._settings import (
    AccessConfig,
    CatalogPaginationConfig,
    RemoteConfig,
)
from egis_agentkit.client.remote_agent._base import build_connection_config
from egis_agentkit.core.agent._expert_definition import ExpertDefinition
from egis_agentkit.core.agent._remote_definition import RemoteDefinition
from egis_agentkit.errors import Forbidden, InvalidRequest, NotFound
from egis_agentkit.service._user import require_admin
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.storage import AgentKitStorage
from egis_agentkit.storage._postgres import DefinitionWrite
from egis_agentkit.storage.model import AgentDefinition, AgentType, User, ViewType
from egis_agentkit.storage.model._definition import EXPERT_TYPES
from egis_agentkit.storage.model.agent._remote_config import RemoteAgentConfig
from egis_agentkit.storage.model.agent._update import AgentUpdate

CATALOG_TYPES = EXPERT_TYPES
AGENT_TYPES = {*CATALOG_TYPES, "personal", "orchestrator"}


class AgentManager:
    """组合定义维护与数据库权限查询，提供专家管理入口。"""

    def __init__(
        self,
        storage: AgentKitStorage | DefinitionWrite,
        policies: AccessConfig | LivePolicies | None = None,
        *,
        config_provider: NacosConfigProvider | None = None,
        expert: ExpertDefinition | None = None,
        remote: RemoteDefinition | None = None,
    ) -> None:
        self.storage, self.config = storage, config_provider
        self.repository = storage.agents
        self.permissions = PermissionService(storage, policies)
        self.expert = expert or ExpertDefinition()
        self.remote = remote or RemoteDefinition()

    def _get_definition_builder(
        self, agent_type: AgentType
    ) -> ExpertDefinition | RemoteDefinition:
        """选择允许通过专家管理入口维护的具体定义。"""
        definitions = {"expert": self.expert, "remote": self.remote}
        if agent_type not in definitions:
            raise Forbidden("该 Agent 类型由个人助理或专家团入口维护")
        return definitions[agent_type]

    def _validate_write_access(
        self,
        user: User,
        agent_type: AgentType,
        owner_type: Literal["user", "system"],
        owner_id: str,
    ) -> None:
        """统一管理操作资格，远程专家仅允许管理员维护。"""
        self.permissions.require_definition_write(
            user, owner_type=owner_type, owner_id=owner_id
        )
        if agent_type == "remote":
            require_admin(user)

    async def _build_definition(
        self,
        transaction: DefinitionWrite,
        user: User,
        definition_builder: ExpertDefinition | RemoteDefinition,
        definition: AgentDefinition,
    ) -> AgentDefinition:
        """组合使用权限与具体类型的定义能力，得到可提交配置。"""
        assets = await PermissionService(
            transaction, self.permissions.policies
        ).require_agent_bindings(user, definition)
        if definition.agent_type == "remote":
            settings = (
                self.config.snapshot.agent.remote if self.config else RemoteConfig()
            )
            build_connection_config(
                RemoteAgentConfig.model_validate(definition.payload["remote_config"]),
                settings,
            )
        return definition_builder.build_definition(definition, assets=assets)

    async def add_definition(
        self, user: User, definition: AgentDefinition
    ) -> dict[str, Any]:
        """核对管理资格，构建配置，原子创建首版及所有者授权。"""
        # 1. 管理入口校验角色和归属，不允许维护内部 Agent 类型。
        core = self._get_definition_builder(definition.agent_type)
        self._validate_write_access(
            user, definition.agent_type, definition.owner_type, definition.owner_id
        )
        # 2. 使用存储管理的事务，共同提交定义和授权。
        async with self.storage.definition_write() as write:
            configured = await self._build_definition(write, user, core, definition)
            saved = await write.agents.add_definition(
                configured.model_dump(mode="json"), actor=user.user_id
            )
            await GrantService(write).record_definition_owner(saved, actor=user.user_id)
            return saved

    async def update_definition(
        self, user: User, definition_id: str, changes: AgentUpdate
    ) -> dict[str, Any]:
        """以现有身份检查资格，仅更新允许编辑的配置并发布下一版。"""
        async with self.storage.definition_write() as write:
            # 1. 类型和归属只来自锁定记录，更新输入不提供这些字段。
            previous = await write.agents.get_definition_for_update(definition_id)
            core = self._get_definition_builder(previous["agent_type"])
            self._validate_write_access(
                user,
                previous["agent_type"],
                previous["owner_type"],
                previous["owner_id"],
            )
            definition = AgentDefinition(
                **{
                    key: previous[key]
                    for key in ("id", "owner_type", "owner_id", "agent_type")
                },
                **changes.model_dump(),
            )
            # 2. Core 构建新配置；Storage 检查并发版本并追加明细。
            configured = await self._build_definition(write, user, core, definition)
            saved = await write.agents.update_definition(
                definition.id, configured.model_dump(mode="json"), actor=user.user_id
            )
            await GrantService(write).record_definition_owner(saved, actor=user.user_id)
            return saved

    async def delete_definition(self, user: User, definition_id: str) -> None:
        """检查管理资格，原子删除定义并回收其授权。"""
        async with self.storage.definition_write() as write:
            # 1. 以锁定记录的实际类型和归属检查资格。
            previous = await write.agents.get_definition_for_update(definition_id)
            self._get_definition_builder(previous["agent_type"])
            self._validate_write_access(
                user,
                previous["agent_type"],
                previous["owner_type"],
                previous["owner_id"],
            )
            # 2. 定义删除与授权回收一起提交，失败一起回滚。
            await write.agents.delete_definition(definition_id, actor=user.user_id)
            await GrantService(write).retire_definition_grants(
                definition_id, actor=user.user_id, previous=previous
            )

    async def get_definition(
        self,
        user: User,
        definition_id: str,
        view: ViewType = "private",
        *,
        version: int | str | None = None,
    ) -> dict[str, Any]:
        """读取专家编辑或详情页使用的一条定义。"""
        source = await self._get_expert_definition(definition_id)
        return await self._get_visible_definition(user, source, view, version=version)

    async def get_definition_for_use(
        self,
        user: User,
        definition_id: str,
        view: ViewType,
        *,
        version: int | str | None = None,
        permissions: PermissionService | None = None,
    ) -> dict[str, Any]:
        """校验专家的使用权限，读取该授权指向的配置版本。"""
        permissions = self.permissions if permissions is None else permissions
        source = await self._get_expert_definition(definition_id)
        reference = {
            "resource_type": "agent",
            "resource_id": definition_id,
            "resource_view_type": view,
            "resource_version": version,
            "allow_pinned": False,
        }
        snapshot = await permissions.read_snapshot(
            user, [reference], loaded=[source], include_dependencies=False
        )
        access = await snapshot.require("agent", definition_id, view, version=version)
        return await snapshot.definition_view(
            {
                **reference,
                "resource_version": access.resource_version,
                **access.source_options,
            }
        )

    async def list_versions(
        self, user: User, definition_id: str
    ) -> list[dict[str, Any]]:
        """读取调用者有权查看的专家版本引用。"""
        source = await self._get_expert_definition(definition_id)
        snapshot = await self._read_definition_snapshot(user, source)
        result = await snapshot.definition_versions(source)
        if not result:
            raise Forbidden()
        return result

    async def _get_expert_definition(self, definition_id: str) -> dict[str, Any]:
        source = await self.repository.get_definition(definition_id)
        if (
            source["definition_type"] != "agent"
            or source["agent_type"] not in EXPERT_TYPES
        ):
            raise NotFound("资源类型不匹配")
        if source["deleted_at"] is not None:
            raise Forbidden("资源已停用")
        return source

    async def _get_visible_definition(
        self,
        user: User,
        definition: dict[str, Any],
        view: ViewType | None = None,
        *,
        version: int | str | None = None,
        permissions: PermissionService | None = None,
    ) -> dict[str, Any]:
        permissions = self.permissions if permissions is None else permissions
        snapshot = await self._read_definition_snapshot(
            user, definition, view, version=version, permissions=permissions
        )
        user = snapshot.user
        access = await snapshot.definition_access(definition, view, version=version)
        value = await snapshot.definition_view(
            {
                "resource_type": definition["definition_type"],
                "resource_id": definition["id"],
                "resource_view_type": access.view,
                "resource_version": version or access.decision.resource_version,
                "allow_pinned": False,
                **access.decision.source_options,
            }
        )
        share = await snapshot.share_availability(
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

    async def _read_definition_snapshot(
        self, user, source, view=None, *, version=None, permissions=None
    ):
        permissions = self.permissions if permissions is None else permissions
        selected = view or (
            "system"
            if source["owner_type"] == "system"
            else "private"
            if source["owner_id"] == user.user_id
            else "shared"
        )
        reference = {
            "resource_type": source["definition_type"],
            "resource_id": source["id"],
            "resource_view_type": selected,
            "resource_version": version,
        }
        return await permissions.read_snapshot(
            user, [reference], loaded=[source], include_dependencies=False
        )

    async def page_definitions(
        self,
        user: User,
        *,
        types: Sequence[AgentType] = CATALOG_TYPES,
        cursor: str | None = None,
        limit: int | None = None,
        query: str = "",
        catalog: bool = False,
        category: str = "",
        mine: bool = False,
    ) -> dict[str, Any]:
        """返回当前可见的定义和下一页游标。"""
        configuration = self.config.snapshot if self.config else None
        pagination = (
            configuration.agent.catalog if configuration else CatalogPaginationConfig()
        )
        limit = pagination.default_page_size if limit is None else limit
        # 校验筛选条件和游标范围，再映射到数据库中的 Agent 类型。
        if not types or set(types) - AGENT_TYPES:
            raise ValueError("无效的 Agent 类型")
        if not 1 <= limit <= pagination.max_page_size:
            raise InvalidRequest(
                f"分页大小必须介于 1 和 {pagination.max_page_size} 之间"
            )
        if len(query) > pagination.max_query_length:
            raise InvalidRequest(f"专家查询最多 {pagination.max_query_length} 个字符")
        if len(category) > pagination.max_query_length:
            raise InvalidRequest(f"专家分类最多 {pagination.max_query_length} 个字符")
        if "\0" in query or "\0" in category:
            raise InvalidRequest("无效的目录筛选")
        if cursor is not None and len(cursor) > pagination.max_cursor_length:
            raise InvalidRequest(f"专家游标最多 {pagination.max_cursor_length} 个字符")
        query = query.strip().casefold()
        scope = [user.user_id, sorted(set(types)), query]
        if catalog or category or mine:
            filters = json.dumps([*scope, catalog, category, mine]).encode()
            scope = [sha256(filters).hexdigest()]
        after = self._decode_cursor(cursor, scope) if cursor is not None else None
        from egis_agentkit.service._user import UserService

        user = await UserService(self.storage).get(user.user_id)
        rows = await self.repository.page_visible_definitions(
            user,
            types,
            after=after,
            limit=limit + 1,
            query=query,
            category=category,
            owner_only=mine,
            policies=configuration.access
            if configuration
            else self.permissions.policies,
        )
        items = rows[:limit]
        next_cursor = (
            self._encode_cursor(scope, (items[-1]["created_at"], items[-1]["id"]))
            if len(rows) > limit
            else None
        )
        result: dict[str, Any] = {"items": items, "next_cursor": next_cursor}
        if catalog and cursor is None:
            result["categories"] = await self.repository.list_visible_categories(
                user, types
            )
        return result

    async def page_remote_definitions(self, user: User, page) -> dict[str, Any]:
        """分页查询管理员可维护的第三方专家，只返回管理摘要。"""
        require_admin(user)
        rows = await self.repository.page_visible_definitions(
            user,
            ("remote",),
            after=None,
            limit=page.fetch_limit,
            offset=page.offset,
            editable_only=True,
            policies=self.permissions.policies,
        )
        return page.response(rows)

    @staticmethod
    def _encode_cursor(
        query_scope: list[Any], last_position: tuple[datetime, str]
    ) -> str:
        """保存筛选范围和最后一条已返回记录的位置。"""
        created_at, definition_id = last_position
        encoded_cursor = json.dumps(
            [query_scope, created_at.isoformat(), definition_id]
        ).encode()
        return base64.urlsafe_b64encode(encoded_cursor).decode()

    @staticmethod
    def _decode_cursor(cursor: str, query_scope: list[Any]) -> tuple[datetime, str]:
        """校验游标属于当前查询，并还原数据库排序位置。"""
        try:
            saved_scope, timestamp, definition_id = json.loads(
                base64.b64decode(cursor, altchars=b"-_", validate=True)
            )
            position = datetime.fromisoformat(timestamp)
            if (
                saved_scope == query_scope
                and position.tzinfo
                and isinstance(definition_id, str)
            ):
                return position, definition_id
        except (ValueError, TypeError):
            pass
        raise ValueError("无效的 Agent 分页游标")


async def get_definition_for_execution(
    permissions: PermissionService,
    user: User,
    resource_type: Literal["agent", "team"],
    definition_id: str,
    view: ViewType,
    *,
    version: int | str | None = None,
) -> dict[str, Any]:
    """新会话或邀请选用获准的当前定义，并检查固定绑定。"""
    reference = {
        "resource_type": resource_type,
        "resource_id": definition_id,
        "resource_view_type": view,
        "resource_version": version,
        "allow_pinned": False,
    }
    snapshot = await permissions.read_snapshot(user, [reference])
    access = await snapshot.require(resource_type, definition_id, view, version=version)
    root = await snapshot.definition_view(
        {
            **reference,
            "resource_version": access.resource_version,
            **access.source_options,
        }
    )
    await snapshot.require_execution(root)
    return root
