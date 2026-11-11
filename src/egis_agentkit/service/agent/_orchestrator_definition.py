"""维护专家团拥有的主理人定义。

核心能力
- 随 Team 创建或更新主理人配置。
- 随 Team 停用主理人并回收权限。
"""

from egis_agentkit.storage._postgres import DefinitionWrite

from ._definition_base import EditableAgentDefinition


class OrchestratorDefinition(EditableAgentDefinition):
    """Team 注入它自己的事务，主理人与 Team 版本一起提交。"""

    agent_type = "orchestrator"

    def __init__(self, write: DefinitionWrite, policies=None):
        if not isinstance(write, DefinitionWrite):
            raise TypeError("主理人只能在 Team 定义事务内维护")
        super().__init__(write, policies)
