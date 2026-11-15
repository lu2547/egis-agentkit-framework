"""管理本地连接器配置与受权的 MCP 客户端。"""

from __future__ import annotations

from agentscope.mcp import MCPClient

from egis_agentkit.errors import NotFound

from ._registry import _LocalRegistry


class LocalMCPRegistry(_LocalRegistry):
    def __init__(self, assets):
        super().__init__(assets, "mcp")

    def config(self, asset):
        config = asset["payload"].get("config")
        if config is None:
            raise NotFound("MCP 版本配置不存在")
        return config




class AuthorizedMCPClient(MCPClient):
    async def get_tool(self, name):
        tool = await super().get_tool(name)
        tool._egis_mcp_client = self
        return tool
