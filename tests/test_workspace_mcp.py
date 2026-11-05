import os
from types import SimpleNamespace

import pytest
from agentscope.mcp import HttpMCPConfig, MCPClient
from test_domain import identities

from egis_agentkit.services.assembly import execution_scope
from egis_agentkit.workspace.manager import EgisLocalWorkspaceManager


@pytest.mark.asyncio
@pytest.mark.skipif(
    os.getenv("AGENTKIT_RESOURCE_LIVE_NACOS") != "1",
    reason="requires the explicitly started local MCP demo",
)
async def test_native_workspace_connects_and_reuses_stateful_mcp_per_session(
    storage, tmp_path
):
    await identities(storage)

    class Resources:
        def __init__(self):
            self.storage = storage

        async def mcps_for_refs(self, user, refs, authorization_scope=None):
            return [
                MCPClient(
                    name="demo",
                    mcp_config=HttpMCPConfig(url="http://127.0.0.1:8766/mcp"),
                    is_stateful=True,
                )
            ]

    manager = EgisLocalWorkspaceManager(tmp_path, Resources())
    async with manager:
        first = None
        for sid in ("s1", "s1", "s2"):
            scope = SimpleNamespace(
                user_id="alice",
                agent_id="a",
                session_id=sid,
                definition={"agent_type": "expert", "payload": {"bindings": []}},
            )
            token = execution_scope.set(scope)
            try:
                workspace = await manager.get_workspace(
                    "alice", "a", sid, "alice-workspace"
                )
                clients = await workspace.list_mcps(agent_id="a", session_id=sid)
                assert clients[0].is_connected
                if first is None:
                    first = clients[0]
                elif sid == "s1":
                    assert clients[0] is first
                else:
                    assert clients[0] is not first
            finally:
                execution_scope.reset(token)
