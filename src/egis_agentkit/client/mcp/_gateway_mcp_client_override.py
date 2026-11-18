"""通过原生 MCP 网关登记连接，分开传递配置和运行凭据。"""

import hashlib
import json

from agentscope.workspace._gateway_client import GatewayMCPClient


def credential_fingerprint(client):
    """检测凭据变化，供工作区调和以及工具调用校验使用。"""
    return hashlib.sha256(
        json.dumps(client._runtime_headers, sort_keys=True).encode()
    ).hexdigest()


class AuthorizedGatewayMCPClient(GatewayMCPClient):
    """将当前受管连接与网关工具关联，供框架校验调用权限。"""

    async def get_tool(self, name):
        tool = await super().get_tool(name)
        tool._egis_mcp_client = self
        return tool


class AuthorizedGatewayClient:
    """在原生网关注册请求中附带运行凭据，配置模型中只保留公开字段。"""

    def __init__(self, gateway, *, declarations, client_factory):
        self._gateway = gateway
        self._declarations = declarations
        self._client_factory = client_factory

    async def health(self):
        return await self._gateway.health()

    async def aclose(self):
        await self._gateway.aclose()

    def make_client(self, spec, *, agent_id="", session_id="", connected=False):
        client = self._client_factory.model_validate(spec)
        client.attach(
            self._gateway, agent_id=agent_id, session_id=session_id, connected=connected
        )
        for declaration in self._declarations(agent_id, session_id):
            if declaration.name == client.name:
                # 原生 connect 将这些头放在单独的 runtime_headers 字段，
                # 网关的列表和连接配置序列化均不会返回该字段。
                client._runtime_headers = dict(declaration._runtime_headers)
                break
        return client
