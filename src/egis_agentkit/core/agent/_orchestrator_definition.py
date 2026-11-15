"""构建专家团主理人配置，并保持主理人的固定身份。

核心能力
- from_team 将团队归属及主理人配置组成定义输入。
- build_definition 校验主理人配置；固定成员与数据库版本由 Team Service 管理。
"""

from __future__ import annotations

from typing import Any, Literal, Sequence

from egis_agentkit.errors import Forbidden
from egis_agentkit.storage.model import AgentDefinition, new_id

from ._definition_base import AgentDefinitionBase


class OrchestratorDefinition(AgentDefinitionBase[AgentDefinition]):
    """提供主理人的定义能力，供团队管理流程组合使用。"""

    agent_type = "orchestrator"

    def build_definition(
        self, definition: AgentDefinition, /, *, assets: Sequence[dict[str, Any]] = ()
    ) -> AgentDefinition:
        """主理人使用技能和连接器，专属函数工具由专家成员提供。"""
        configured = self._build_configuration(definition)
        if configured.payload.get("remote_config") is not None:
            raise Forbidden("该 Agent 类型不能配置远程连接")
        if any(
            ref["resource_type"] == "tool" for ref in configured.payload["bindings"]
        ):
            raise Forbidden("主理人不能绑定专家专属工具")
        return configured

    def build_from_team(
        self,
        *,
        owner_type: Literal["user", "system"],
        owner_id: str,
        orchestrator_config: dict[str, Any],
        previous_definition: dict[str, Any] | None = None,
    ) -> AgentDefinition:
        """构造主理人配置，已有主理人沿用身份和当前 owner 版本。"""
        definition = AgentDefinition(
            id=previous_definition["id"] if previous_definition else new_id(),
            owner_type=owner_type,
            owner_id=owner_id,
            agent_type=self.agent_type,
            agent_name=orchestrator_config["agent_name"],
            agent_display_name=orchestrator_config["agent_display_name"],
            owner_version=previous_definition["owner_version"]
            if previous_definition
            else 1,
            payload=orchestrator_config.get("payload", {}),
        )
        return self.build_definition(definition)
