"""个人助理使用的受管专家邀请工具。"""

import json

from agentscope.message import TextBlock, ToolResultState
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import ToolBase, ToolChunk

from egis_agentkit.errors import Forbidden, InvalidRequest
from egis_agentkit.storage.model._definition import build_definition_reference


class ExpertInvite(ToolBase):
    name = "AgentInvite"
    description = (
        "根据候选专家的能力描述选择适合当前任务的专家，并把需要它完成的任务交给它。"
    )
    is_concurrency_safe = False
    is_read_only = False

    def __init__(self, candidates, invite):
        super().__init__()
        options = []
        self._candidates = {}
        for expert in candidates:
            reference = build_definition_reference(expert)
            target = f"{reference['definition_id']}:{reference['resource_view_type']}"
            self._candidates[target] = reference
            options.append(
                {
                    "target": target,
                    "name": expert["agent_display_name"],
                    "description": expert["payload"].get("description", ""),
                }
            )
        self.description = (
            f"{self.description}\n{json.dumps(options, ensure_ascii=False)}"
        )
        self.input_schema = {
            "type": "object",
            "properties": {
                "target": {"type": "string", "enum": list(self._candidates)},
                "prompt": {"type": "string", "minLength": 1},
            },
            "required": ["target", "prompt"],
            "additionalProperties": False,
        }
        self._invite = invite

    async def check_permissions(self, tool_input, context):
        return PermissionDecision(PermissionBehavior.ALLOW)

    async def call(self, *, target: str, prompt: str) -> ToolChunk:
        if target not in self._candidates:
            raise Forbidden("该专家不在本次自动邀请候选中")
        if not prompt.strip():
            raise InvalidRequest("请填写交给专家的任务")
        message = await self._invite(self._candidates[target], prompt)
        return ToolChunk(
            content=[TextBlock(text=message)],
            state=ToolResultState.SUCCESS,
        )
