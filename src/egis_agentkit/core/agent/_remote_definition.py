"""构建远程专家的连接配置，不创建通信客户端。

核心能力
- build_definition 校验远程协议模型和绑定规则。
- 管理员资格、授权和版本维护交由 Service 组织。
"""

from __future__ import annotations

from typing import Any, Sequence

from egis_agentkit.errors import Forbidden
from egis_agentkit.storage.model import AgentDefinition
from egis_agentkit.storage.model.agent._remote_config import RemoteAgentConfig

from ._definition_base import AgentDefinitionBase


class RemoteDefinition(AgentDefinitionBase[AgentDefinition]):
    """实现远程配置规则，通信和管理分别由 Client、Service 承担。"""

    agent_type = "remote"

    def build_definition(
        self, definition: AgentDefinition, /, *, assets: Sequence[dict[str, Any]] = ()
    ) -> AgentDefinition:
        """校验远程连接及本地绑定限制，返回独立的定义配置。"""
        configured = self._build_configuration(definition)
        RemoteAgentConfig.model_validate(configured.payload["remote_config"])
        if any(
            ref["resource_type"] == "tool" for ref in configured.payload["bindings"]
        ):
            raise Forbidden("远程专家不能绑定本地专家专属工具")
        return configured
