"""为每个用户初始化一份个人助理定义。

核心能力
- build 根据用户生成固定 ID，首次创建，重复调用返回已有定义。
- read_documents / save_documents 维护个人助理的三份提示文档。
"""

from hashlib import sha256
from importlib.resources import files as package_files
from pathlib import Path

from egis_agentkit.backend.nacos._settings import PersonalDocumentsConfig
from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.storage._personal_documents import (
    DOCUMENT_FILES,
    PersonalDocumentStorage,
)
from egis_agentkit.storage._workspace import WorkspaceFiles
from egis_agentkit.storage.model import Definition, User

from ._definition_base import AgentDefinitionBase


class PersonalDefinition(AgentDefinitionBase):
    """个人助理由平台初始化，每个用户对应一份定义。"""

    agent_type = "personal"

    def __init__(
        self,
        storage,
        policies=None,
        *,
        workspace_root=None,
        config=None,
        installations=None,
    ):
        super().__init__(storage, policies)
        self.config = config
        self.installations = installations
        self.documents = (
            PersonalDocumentStorage(storage, WorkspaceFiles(Path(workspace_root)))
            if workspace_root is not None
            else None
        )

    async def build(self, user: User, data: Definition | None = None) -> dict:
        """每个用户只有一个个人助理，重复初始化返回同一条定义。"""
        if not user.is_enabled:
            raise Forbidden("用户已停用")
        if data is not None:
            raise Forbidden("个人助理由系统初始化")
        # 生成稳定 ID，让并发初始化定位到同一条记录。
        identifier = sha256(("personal:" + user.user_id).encode()).hexdigest()[:32]
        record = Definition(
            id=identifier,
            owner_id=user.user_id,
            agent_type="personal",
            agent_name="personal",
            agent_display_name="个人助理",
            payload={"description": "你的工作助理", "avatar": "spark"},
        )
        # 由存储原子地创建或读取个人助理。
        return await self.repository.ensure_personal(
            record.model_dump(mode="json"), actor=user.user_id
        )

    def _document_settings(self) -> PersonalDocumentsConfig:
        if self.config is not None:
            return self.config.snapshot.agent.personal.documents
        return PersonalDocumentsConfig()

    async def read_documents(self, user: User) -> dict[str, str]:
        """读取已有文档，用对应模板填补缺失的文件。"""
        if self.documents is None:
            raise RuntimeError("个人助理文档存储未装配")
        current = await self.documents.read(user.user_id)
        templates = package_files("egis_agentkit.service").joinpath("templates")
        filenames = self._document_settings().templates
        return {
            key: current[key]
            if key in current
            else templates.joinpath(
                getattr(filenames, key.removesuffix("_md"))
            ).read_text(encoding="utf-8")
            for key in DOCUMENT_FILES
        }

    async def save_documents(
        self, user: User, documents: dict[str, str]
    ) -> dict[str, str]:
        """校验三份文档及大小，再交给文件存储写入。"""
        if self.documents is None:
            raise RuntimeError("个人助理文档存储未装配")
        max_bytes = self._document_settings().max_bytes
        if set(documents) != set(DOCUMENT_FILES) or any(
            not isinstance(value, str) or len(value.encode()) > max_bytes
            for value in documents.values()
        ):
            raise Conflict(
                f"偏好必须包含 agent_md、soul_md、user_md，单份不超过 {max_bytes} 字节"
            )
        await self.documents.save(user.user_id, documents)
        return documents
