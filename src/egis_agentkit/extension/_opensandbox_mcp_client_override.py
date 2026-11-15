"""覆盖网关 MCP 工具取得方式，保留企业连接来源供权限校验。"""

from agentscope.workspace._gateway_client import GatewayMCPClient


class AuthorizedGatewayMCPClient(GatewayMCPClient):
    """调用仍走沙箱网关，工具携带当前获授权的连接配置。"""

    async def get_tool(self, name):
        tool = await super().get_tool(name)
        tool._egis_mcp_client = self
        return tool
