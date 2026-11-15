"""构建个人助理身份，并装配包含个人偏好的原生智能体。

核心能力
- build_definition 生成稳定的个人助理身份与初始配置。
- build_agent 将 Service 提供的个人偏好装配到原生提示中。
- 文档读写及资源开关管理留在 Service。
"""

from __future__ import annotations

from hashlib import sha256
from typing import Any, Literal, Sequence

from agentscope.app.storage import AgentRecord

from egis_agentkit.storage.model import AgentDefinition, User

from ._definition_base import AgentDefinitionBase

DOCUMENT_FILES = {"agent_md": "agent.md", "soul_md": "soul.md", "user_md": "user.md"}


class PersonalDefinition(AgentDefinitionBase[User]):
    """构建个人助理定义，并在原生装配时应用个人偏好。"""

    agent_type = "personal"

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
        """复用原生装配，将本轮读取的三份个人偏好加入提示词。"""
        record = AgentDefinitionBase.build_agent(
            user_id,
            definition,
            source=source,
            invite_description=invite_description,
            record_id=record_id,
            record=record,
        )
        if preferences:
            content = "\n\n".join(preferences.values())
            if content:
                record.data.system_prompt = "\n\n".join(
                    (record.data.system_prompt, content)
                )
        return record

    def build_definition(
        self, user: User, /, *, assets: Sequence[dict[str, Any]] = ()
    ) -> AgentDefinition:
        """生成用户稳定身份与初始配置；唯一性由存储管理。"""
        return AgentDefinition(
            id=sha256(("personal:" + user.user_id).encode()).hexdigest()[:32],
            owner_id=user.user_id,
            agent_type=self.agent_type,
            agent_name="personal",
            agent_display_name="个人助理",
            payload={"description": "你的工作助理", "avatar": "spark"},
        )
