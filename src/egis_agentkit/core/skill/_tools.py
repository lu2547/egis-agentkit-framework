"""把聊天中的技能导入和创建请求交给受管的 Skill 服务。"""

from agentscope.message import TextBlock, ToolResultState
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import ToolBase, ToolChunk


class InstallSkillPackage(ToolBase):
    name = "InstallSkillPackage"
    description = "将当前项目中用户上传的 ZIP 校验、发布为个人技能并启用。"
    input_schema = {
        "type": "object",
        "properties": {
            "path": {
                "type": "string",
                "minLength": 1,
                "description": "当前项目上传区中的 ZIP 相对路径",
            },
        },
        "required": ["path"],
        "additionalProperties": False,
    }
    is_concurrency_safe = False
    is_read_only = False

    def __init__(self, scope, skill_imports):
        super().__init__()
        self.scope = scope
        self.skill_imports = skill_imports

    async def check_permissions(self, tool_input, context):
        return PermissionDecision(
            behavior=PermissionBehavior.ASK,
            message="安装并启用此个人技能",
        )

    async def call(self, *, path: str) -> ToolChunk:
        skill = await self.skill_imports.install_from_project(self.scope, path)
        return ToolChunk(
            content=[
                TextBlock(
                    text=(
                        f"已安装并启用个人技能「{skill['display_name']}」"
                        f"（{skill['resource_id']}，{skill['resource_version']}）。"
                    )
                )
            ],
            state=ToolResultState.SUCCESS,
        )


class CreateSkill(ToolBase):
    name = "CreateSkill"
    description = "将编写完成的 SKILL.md 发布到当前用户的个人技能目录。"
    input_schema = {
        "type": "object",
        "properties": {
            "skill_id": {"type": "string", "minLength": 1, "maxLength": 128},
            "markdown": {
                "type": "string",
                "minLength": 1,
                "description": "包含 name、description 和 version 的 SKILL.md",
            },
        },
        "required": ["skill_id", "markdown"],
        "additionalProperties": False,
    }
    is_concurrency_safe = False
    is_read_only = False

    def __init__(self, scope, skill_imports):
        super().__init__()
        self.scope = scope
        self.skill_imports = skill_imports

    async def check_permissions(self, tool_input, context):
        return PermissionDecision(
            behavior=PermissionBehavior.ASK,
            message="创建此个人技能",
        )

    async def call(self, *, skill_id: str, markdown: str) -> ToolChunk:
        result = await self.skill_imports.create_for_user(
            self.scope, skill_id, markdown
        )
        return ToolChunk(
            content=[
                TextBlock(
                    text=(
                        f"已创建个人技能「{result['display_name']}」"
                        f"（{result['resource_id']}，{result['resource_version']}）。"
                    )
                )
            ],
            state=ToolResultState.SUCCESS,
        )
