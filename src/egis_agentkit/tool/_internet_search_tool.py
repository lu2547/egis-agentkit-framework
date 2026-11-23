"""互联网搜索工具占位，后续在此接入搜索服务。"""

from agentscope.message import TextBlock, ToolResultState
from agentscope.permission import PermissionBehavior, PermissionDecision
from agentscope.tool import ToolBase, ToolChunk


class InternetSearch(ToolBase):
    name = "InternetSearch"
    description = "互联网搜索，当前待实现。"
    is_concurrency_safe = True
    is_read_only = True
    input_schema = {
        "type": "object",
        "properties": {"query": {"type": "string", "minLength": 1}},
        "required": ["query"],
        "additionalProperties": False,
    }

    async def check_permissions(self, tool_input, context):
        return PermissionDecision(PermissionBehavior.ALLOW)

    async def call(self, *, query: str) -> ToolChunk:
        return ToolChunk(
            content=[TextBlock(text="待实现")],
            state=ToolResultState.SUCCESS,
        )
