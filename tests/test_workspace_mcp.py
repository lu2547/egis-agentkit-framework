"""沙箱 MCP 的会话隔离、声明替换和撤销沿用原生网关生命周期。"""

from pathlib import Path
from unittest.mock import AsyncMock

import pytest
from agentscope.mcp import HttpMCPConfig, MCPClient
from agentscope.workspace._gateway_client import GatewayClient
from runtime_helpers import sandbox_manager

from egis_agentkit.client.mcp._gateway_mcp_client_override import (
    AuthorizedGatewayMCPClient,
)
from egis_agentkit.errors import Forbidden


@pytest.mark.asyncio
async def test_sandbox_gateway_reuses_per_session_and_reconciles_revoked_resources(
    tmp_path, monkeypatch
):
    manager = sandbox_manager(tmp_path, monkeypatch)
    spec = MCPClient(
        name="demo",
        mcp_config=HttpMCPConfig(url="https://mcp.example.test/mcp"),
        is_stateful=True,
    )
    async with manager:
        first = None
        workspaces = []
        for sid in ("s1", "s1", "s2"):
            workspace = await manager.get_workspace(
                "alice", "agent", sid, project_id="project"
            )
            if workspace._gateway is None:
                gateway = GatewayClient(workspace.get_backend(), 5600)
                gateway.exec_request = AsyncMock(return_value=(200, b"{}"))
                workspace._gateway = gateway
            workspace.configure_workspace_resources(tools=[], mcps=[spec])
            clients = await workspace.list_mcps(agent_id="agent", session_id=sid)
            assert len(clients) == 1
            assert isinstance(clients[0], AuthorizedGatewayMCPClient)
            assert clients[0].is_connected
            assert not (Path(workspace.workdir) / ".mcp").exists()
            if first is None:
                first = clients[0]
                workspaces.append(workspace)
            elif sid == "s1":
                assert clients[0] is first
            else:
                assert clients[0] is not first
                workspaces.append(workspace)

        # 声明移除会注销当前会话连接，其他会话的连接继续可用。
        one, two = workspaces
        one.configure_workspace_resources(tools=[], mcps=[])
        assert await one.list_mcps() == []
        assert not first.is_connected
        assert not (Path(one.workdir) / ".mcp").exists()
        assert (await two.list_mcps())[0].is_connected
        with pytest.raises(Forbidden, match="绑定"):
            await one.list_mcps(agent_id="agent", session_id="s2")

        # 同名连接器配置变化必须创建新句柄，不能沿用旧凭据。
        previous = (await two.list_mcps())[0]
        changed = MCPClient(
            name="demo",
            mcp_config=HttpMCPConfig(url="https://changed.example.test/mcp"),
            is_stateful=True,
        )
        two.configure_workspace_resources(tools=[], mcps=[changed])
        current = (await two.list_mcps())[0]
        assert current is not previous and not previous.is_connected
        assert current.mcp_config.url == "https://changed.example.test/mcp"
