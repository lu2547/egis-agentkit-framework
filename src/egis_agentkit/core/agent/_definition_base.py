"""声明定义构建契约，统一装配原生 Agent 与会话配置。

核心能力
- build_definition 由具体类型生成配置，不访问业务存储。
- build_agent 将定义装配为完整 AgentRecord，创建和运行时共用。
- build_session 装配 SessionConfig 与 AgentState，保存定义和项目引用。
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Generic, Literal, Sequence, TypeVar

from agentscope.app.storage import (
    AgentChatConfig,
    AgentData,
    AgentRecord,
    ChatModelConfig,
    InviteConfig,
    SessionConfig,
)
from agentscope.permission import PermissionMode
from agentscope.state import AgentState

from egis_agentkit.storage.model import AgentDefinition, User
from egis_agentkit.storage.model._definition import build_definition_reference

_DefinitionInput = TypeVar("_DefinitionInput", User, AgentDefinition)


class AgentDefinitionBase(ABC, Generic[_DefinitionInput]):
    """各类 Agent 实现定义能力，管理流程由 Service 组织。"""

    agent_type: Literal["personal", "expert", "remote", "orchestrator"]

    @abstractmethod
    def build_definition(
        self, definition: _DefinitionInput, /, *, assets: Sequence[dict[str, Any]] = ()
    ) -> AgentDefinition:
        """根据类型对应的输入和资源信息构建配置，不保存版本。"""

    def _build_configuration(self, definition: AgentDefinition) -> AgentDefinition:
        """由具体实现固定类型，并独立构建配置，避免修改调用方输入。"""
        return AgentDefinition.model_validate(
            {**definition.model_dump(mode="json"), "agent_type": self.agent_type}
        )

    @staticmethod
    def build_agent(
        user_id: str,
        definition: dict[str, Any],
        *,
        source: Literal["user", "team"] = "user",
        invite_description: str | None = None,
        record_id: str | None = None,
        record: AgentRecord | None = None,
        preferences: dict[str, str] | None = None,
    ) -> AgentRecord:
        """装配固定定义的名称、提示和原生配置；运行重建时保留原生身份。"""
        agent_data = AgentData(
            **({"id": record.data.id} if record is not None else {}),
            name=definition["agent_name"],
            system_prompt=definition["payload"]["system_prompt"],
            chat_config=AgentChatConfig(
                invite_config=InviteConfig(
                    invitable=invite_description is not None,
                    invite_description=invite_description,
                )
            ),
        )
        if record is not None:
            return record.model_copy(update={"data": agent_data}, deep=True)
        return AgentRecord(
            user_id=user_id,
            source=source,
            data=agent_data,
            **({"id": record_id} if record_id is not None else {}),
        )

    @staticmethod
    def build_session(
        definition: dict[str, Any],
        *,
        project_id: str,
        workspace_id: str,
        session_id: str,
        cwd: str,
        root: dict[str, Any] | None = None,
        name: str | None = None,
        auto_name: bool = False,
        model: ChatModelConfig | dict[str, Any] | None = None,
        selected_model_key: str | None = None,
        permission_mode: PermissionMode = PermissionMode.DEFAULT,
        is_initialized: bool = False,
        config: SessionConfig | None = None,
    ) -> tuple[SessionConfig, AgentState]:
        """装配原生会话配置与业务引用，不写数据库或复制完整 Definition。"""
        # 1. 原生邀请可以携带已有配置，仅替换项目对应的工作区和目录。
        configuration = (
            config.model_copy(update={"workspace_id": workspace_id, "cwd": cwd})
            if config is not None
            else SessionConfig(
                workspace_id=workspace_id,
                cwd=cwd,
                name=name,
                naming={"auto": auto_name},
                chat_model_config=model,
            )
        )
        # 2. 绑定根定义及当前成员，团队成员共享根引用并保留自己的定义身份。
        root = root if root is not None else definition
        binding = {
            "project_id": project_id,
            "is_initialized": is_initialized,
            **build_definition_reference(root),
            "agent_definition_id": definition["id"],
            "is_team": root["definition_type"] == "team",
            "selected_model_key": selected_model_key,
        }
        state = AgentState(
            session_id=session_id,
            middle_context={"egis_agentkit": binding},
            permission_context={"mode": permission_mode},
        )
        return configuration, state
