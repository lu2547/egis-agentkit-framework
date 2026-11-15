"""管理个人助理初始化、个人文档以及广场安装项。

核心能力
- get_or_add_definition 组合个人核心构建与原子存储，返回用户唯一个人助理。
- get_preferences 读取文档；save_preferences 校验并保存个人偏好文档。
- list_resources / add_resource / remove_resource 管理个人资源选择。
- update_resource_enabled 更新已选资源的启停状态，不记录版本。
"""

from __future__ import annotations

import asyncio
from importlib.resources import files as package_files
from pathlib import Path
from typing import Any

from egis_agentkit.client.nacos import NacosConfigProvider
from egis_agentkit.client.nacos._settings import PersonalDocumentsConfig
from egis_agentkit.core.agent._personal_definition import (
    DOCUMENT_FILES,
    PersonalDefinition,
)
from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.storage import AgentKitStorage
from egis_agentkit.storage._files import FileStorage
from egis_agentkit.storage.model import User


class PersonalManager:
    """组合个人助理核心、权限、资源目录和存储完成管理流程。"""

    def __init__(
        self,
        storage: AgentKitStorage,
        *,
        workspace_root: str | Path | None = None,
        config: NacosConfigProvider | None = None,
        definition: PersonalDefinition | None = None,
    ) -> None:
        self.storage = storage
        self.config = config
        self.definition = definition or PersonalDefinition()
        self.files = (
            FileStorage(Path(workspace_root), config_provider=config)
            if workspace_root is not None
            else None
        )

    async def get_or_add_definition(self, user: User) -> dict[str, Any]:
        """验证用户状态，构建个人定义，再原子创建或读取唯一记录。"""
        if not user.is_enabled:
            raise Forbidden("用户已停用")
        configured = self.definition.build_definition(user)
        return await self.storage.agents.get_or_add_personal(
            configured.model_dump(mode="json"), actor=user.user_id
        )

    def _get_document_storage(self) -> FileStorage:
        """确认文档管理所需的文件存储已经装配。"""
        if self.files is None:
            raise RuntimeError("个人助理文档存储未装配")
        return self.files

    async def get_preferences(self, user: User) -> dict[str, str]:
        """安全读取当前用户文件，使用模板补齐缺失文件并保留明确的空内容。"""
        files = self._get_document_storage()
        current = await asyncio.to_thread(
            files.read_user_texts,
            user.user_id,
            "preferences",
            DOCUMENT_FILES.values(),
            max_bytes=self._get_document_settings().max_bytes,
        )
        templates = package_files("egis_agentkit.core.agent").joinpath("templates")
        names = self._get_document_settings().templates
        return {
            key: current[filename]
            if filename in current
            else templates.joinpath(getattr(names, key.removesuffix("_md"))).read_text(
                encoding="utf-8"
            )
            for key, filename in DOCUMENT_FILES.items()
        }

    async def save_preferences(
        self, user: User, documents: dict[str, str]
    ) -> dict[str, str]:
        """校验三份偏好文档，创建或覆盖当前用户的文件并返回保存内容。"""
        files = self._get_document_storage()
        # 1. 完整检查字段、正文类型和配置限额，通过后才写入文件。
        max_bytes = self._get_document_settings().max_bytes
        if set(documents) != set(DOCUMENT_FILES) or any(
            not isinstance(value, str) or len(value.encode()) > max_bytes
            for value in documents.values()
        ):
            raise Conflict(
                f"偏好必须包含 agent_md、soul_md、user_md，单份不超过 {max_bytes} 字节"
            )
        contents = {DOCUMENT_FILES[key]: value for key, value in documents.items()}
        # 2. 统一通过安全文件存储写入，保留用户明确提交的空内容。
        await asyncio.to_thread(
            files.write_user_texts,
            user.user_id,
            "preferences",
            contents,
            max_bytes=max_bytes,
        )
        return documents

    def _get_document_settings(self) -> PersonalDocumentsConfig:
        """读取当前偏好文档的配置限额和默认模板。"""
        return (
            self.config.snapshot.agent.personal.documents
            if self.config
            else PersonalDocumentsConfig()
        )

    async def list_resources(
        self, user: User, resource_type: str
    ) -> dict[str, dict[str, Any]]:
        """按用户查询个人资源列表及开关，不构建定义或解析资源版本。"""
        self._validate_resource_type(resource_type)
        return await self.storage.personal_resources.list_resources(
            user.user_id, resource_type
        )

    @staticmethod
    def _validate_resource_type(resource_type: str) -> None:
        """个人资源选择仅包含技能和连接器。"""
        if resource_type not in ("skill", "mcp"):
            raise Forbidden("个人助理只能选择技能和连接器")

    async def add_resource(
        self, user: User, resource_type: str, resource_id: str
    ) -> dict[str, Any]:
        """选用已由资源入口授权的资源，创建个人关联并默认启用。"""
        self._validate_resource_type(resource_type)
        definition = await self.get_or_add_definition(user)
        return await self.storage.personal_resources.save_resource(
            user.user_id,
            definition["id"],
            resource_type,
            resource_id,
            True,
            actor=user.user_id,
        )

    async def update_resource_enabled(
        self, user: User, resource_type: str, resource_id: str, enabled: bool
    ) -> dict[str, Any]:
        """启停已经选择的资源，不隐式新增个人关联。"""
        self._validate_resource_type(resource_type)
        return await self.storage.personal_resources.update_resource_enabled(
            user.user_id, resource_type, resource_id, enabled, actor=user.user_id
        )

    async def remove_resource(
        self, user: User, resource_type: str, resource_id: str
    ) -> None:
        """取消个人资源选择，资源及授权仍然保留。"""
        self._validate_resource_type(resource_type)
        await self.storage.personal_resources.remove_resource(
            user.user_id, resource_type, resource_id
        )
