"""构建本地专家配置，固定提示与资源绑定。

核心能力
- build_definition 校验本地专家类型、固定绑定和专属工具规则。
- 原生 Agent 和 Session 装配复用 Base；保存和授权由 Manager 组织。
"""

from __future__ import annotations

from typing import Any, Sequence

from egis_agentkit.errors import Forbidden
from egis_agentkit.storage.model import AgentDefinition

from ._definition_base import AgentDefinitionBase


class ExpertDefinition(AgentDefinitionBase[AgentDefinition]):
    """实现本地专家的配置能力，不承担管理接口和持久化。"""

    agent_type = "expert"

    def build_definition(
        self, definition: AgentDefinition, /, *, assets: Sequence[dict[str, Any]] = ()
    ) -> AgentDefinition:
        """构建固定配置，仅允许绑定已部署并向模型暴露的专属工具。"""
        configured = self._build_configuration(definition)
        if configured.payload.get("remote_config") is not None:
            raise Forbidden("该 Agent 类型不能配置远程连接")
        for asset in assets:
            if asset["resource_type"] == "tool" and (
                asset["payload"].get("source") != "deployed"
                or asset["payload"].get("exposure") != "model"
            ):
                raise Forbidden("仅本地专家可以绑定已部署的专家工具")
        return configured
