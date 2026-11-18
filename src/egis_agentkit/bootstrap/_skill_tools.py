"""登记技能助手可绑定的专属工具及其稳定版本。"""

from functools import partial

from egis_agentkit.core.skill._tools import CreateSkill, InstallSkillPackage

SKILL_TOOL_VERSION = "1.0.0"
SKILL_TOOLS = (InstallSkillPackage, CreateSkill)


def deploy_skill_tools(registry, skill_imports):
    for tool in SKILL_TOOLS:
        registry.deploy(
            tool.name,
            SKILL_TOOL_VERSION,
            partial(tool, skill_imports=skill_imports),
            description=tool.description,
            input_schema=tool.input_schema,
            dependency_only=True,
        )
