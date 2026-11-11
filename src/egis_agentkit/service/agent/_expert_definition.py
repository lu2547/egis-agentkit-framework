"""维护供独立召唤或团队邀请的本地专家。

核心能力
- 创建专家，保存固定提示和资源绑定。
- 发布下一版配置或停用定义。
"""

from ._definition_base import EditableAgentDefinition


class ExpertDefinition(EditableAgentDefinition):
    """本地专家使用通用写入流程，按定义版本固定资源。"""

    agent_type = "expert"
