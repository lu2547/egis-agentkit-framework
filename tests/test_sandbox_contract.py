"""通用继承和可替换 Client 必须走真实 Manager/Core/Backend，不绑定厂商。"""

import asyncio
from functools import partial

import pytest
from agentscope.app.workspace_manager import WorkspaceManagerBase
from agentscope.workspace._sandboxed_base import SandboxedWorkspaceBase
from sandbox_helpers import TestSandboxClient

from egis_agentkit.middleware.workspace._workspace_read_guard_middleware_impl import (
    ReadToolGuardMiddleware,
)


def test_workspace_and_manager_directly_implement_generic_native_bases():
    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.service.workspace._egis_sandbox_manager import EgisSandboxManager

    assert EgisSandboxWorkspace.__bases__ == (SandboxedWorkspaceBase,)
    assert EgisSandboxManager.__bases__ == (WorkspaceManagerBase,)


@pytest.fixture
def native_gateway(monkeypatch):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    async def gateway(workspace):
        workspace._gateway = SimpleNamespace(
            aclose=AsyncMock(), health=AsyncMock(return_value=True)
        )

    monkeypatch.setattr(SandboxedWorkspaceBase, "_setup_mcp_gateway", gateway)


def build_manager(tmp_path, *, client_factory):
    from egis_agentkit.client.nacos._settings import SandboxWorkspaceConfig
    from egis_agentkit.service.workspace._egis_sandbox_manager import EgisSandboxManager

    settings = SandboxWorkspaceConfig(
        gateway_home=str(tmp_path / "gateway"),
    )
    from runtime_helpers import workspace_dependencies

    return EgisSandboxManager(
        tmp_path,
        settings,
        client_factory=client_factory,
        read_guard_factory=ReadToolGuardMiddleware,
        **workspace_dependencies(),
    )


@pytest.mark.asyncio
async def test_independent_client_executes_native_tools_and_reuses_one_binding(
    tmp_path, native_gateway
):
    from agentscope.tool import BackendBase

    from egis_agentkit.core.workspace._egis_sandbox_backend import EgisSandboxBackend

    events = []
    manager = build_manager(
        tmp_path, client_factory=partial(TestSandboxClient, events=events)
    )
    manager.files.initialize_project("alice", "project")
    async with manager:
        one, two = await asyncio.gather(
            *(
                manager.get_workspace("alice", "agent", "session", project_id="project")
                for _ in range(2)
            )
        )
        assert one is two
        assert isinstance(one.get_backend(), EgisSandboxBackend)
        assert isinstance(one.get_backend(), BackendBase)
        assert sum(event[0] == "create" for event in events) == 1
        path = str(tmp_path / "runtime/report.txt")
        await one.get_backend().write_file(path, b"company-client-runtime")
        assert await one.get_backend().read_file(path) == b"company-client-runtime"
        result = await one.get_backend().exec_shell(
            ["printf", "%s", "safe; literal"], cwd=str(tmp_path)
        )
        assert result.stdout == b"safe; literal" and result.exit_code == 0
        assert {tool.name for tool in await one.list_tools()} >= {
            "Read",
            "Write",
            "Bash",
        }
        # 实际经 Toolkit 执行，不只检查工具列表或直接调用 Backend。
        import json

        from agentscope.message import ToolCallBlock
        from agentscope.state import AgentState
        from agentscope.tool import Toolkit

        path = str(tmp_path / "runtime/toolkit.txt")
        toolkit = Toolkit()
        for tool in await one.list_tools():
            await toolkit.add_tool(tool)
        for name, values in [
            ("Write", {"file_path": path, "content": "neutral-toolkit"}),
            ("Read", {"file_path": path}),
        ]:
            chunks = [
                chunk
                async for chunk in toolkit.call_tool(
                    ToolCallBlock(id=name, name=name, input=json.dumps(values)),
                    state=AgentState(),
                )
            ]
            assert chunks[-1].state == "success", (name, chunks)
        assert await one.get_backend().read_file(path) == b"neutral-toolkit"


@pytest.mark.asyncio
async def test_different_bindings_do_not_hold_a_global_lock_across_provision(
    tmp_path, native_gateway
):
    gate = asyncio.Event()
    events = []
    manager = build_manager(
        tmp_path, client_factory=partial(TestSandboxClient, events=events, gate=gate)
    )
    async with manager:
        tasks = [
            asyncio.create_task(
                manager.get_workspace("alice", "agent", sid, project_id="project")
            )
            for sid in ["one", "two"]
        ]
        try:
            async with asyncio.timeout(1):
                while sum(e[0] == "create" for e in events) < 2:
                    await asyncio.sleep(0)
        finally:
            gate.set()
            await asyncio.gather(*tasks)


@pytest.mark.asyncio
async def test_app_pks_injected_client_needs_no_opensandbox(
    storage, tmp_path, native_gateway, monkeypatch
):
    import sys

    from egis_agentkit_app.bootstrap import build_app
    from test_app_http import Config, app_snapshot

    from egis_agentkit.client.nacos import NacosBootstrap, NacosClient
    from egis_agentkit.client.nacos._settings import WorkspaceProbeConfig

    monkeypatch.setitem(sys.modules, "egis_agentkit.client.sandbox._opensandbox", None)
    monkeypatch.setenv("TEST_DOWNLOAD", "download-secret-with-more-than-thirty-two-bytes")
    config = Config(
        app_snapshot(
            tmp_path,
            workspace={
                "probe": WorkspaceProbeConfig(
                    storage_python=sys.executable,
                    runtime_commands=[[sys.executable, "-c", "import sys"]],
                ).model_dump(),
                "runtime": {
                    "root": str(tmp_path),
                    "mode": "pks",
                    "sandbox": {
                        "gatewayHome": str(tmp_path / "gateway"),
                    },
                },
            },
        )
    )
    async with NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos:
        app = await build_app(
            config,
            nacos,
            engine=storage.engine,
            owns_client=False,
            sandbox_client_factory=TestSandboxClient,
        )
        manager = app.state.egis_kit.workspace
        async with manager:
            workspace = await manager.get_workspace(
                "alice", "agent", "session", project_id="project"
            )
            assert workspace.sandbox_id.startswith("instance-")


@pytest.mark.asyncio
async def test_close_waits_for_build_and_rejects_unregistered_workspace(
    tmp_path, native_gateway
):
    from egis_agentkit.errors import Conflict

    gate, events, clients = asyncio.Event(), [], []

    def client_factory():
        client = TestSandboxClient(events=events, gate=gate)
        clients.append(client)
        return client

    manager = build_manager(tmp_path, client_factory=client_factory)
    async with manager:
        building = asyncio.create_task(
            manager.get_workspace("alice", "agent", "session", project_id="project")
        )
        async with asyncio.timeout(1):
            while not events:
                await asyncio.sleep(0)
        closing = asyncio.create_task(manager.close_all())
        async with asyncio.timeout(1):
            while not manager._closing_all:
                await asyncio.sleep(0)
        assert not closing.done()
        gate.set()
        with pytest.raises(Conflict, match="关闭"):
            await building
        await closing
        assert clients[0].closed and any(e[0] == "pause" for e in events)
        assert (
            not manager._cache
            and not manager._building
            and not manager._workspace_locks
        )


@pytest.mark.asyncio
async def test_cache_refresh_cannot_return_workspace_after_close_begins(
    tmp_path, native_gateway
):
    from egis_agentkit.errors import Conflict

    manager = build_manager(tmp_path, client_factory=TestSandboxClient)
    async with manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        entered, proceed = asyncio.Event(), asyncio.Event()

        async def renew(timeout):
            entered.set()
            await proceed.wait()

        workspace.client.renew_instance = renew
        retrieving = asyncio.create_task(
            manager.get_workspace("alice", "agent", "session", project_id="project")
        )
        await asyncio.wait_for(entered.wait(), timeout=1)
        closing = asyncio.create_task(manager.close_all())
        async with asyncio.timeout(1):
            while not manager._closing_all:
                await asyncio.sleep(0)
        proceed.set()
        with pytest.raises(Conflict, match="关闭"):
            await retrieving
        await closing


@pytest.mark.asyncio
@pytest.mark.parametrize("close_user", [False, True])
async def test_overlapping_closes_wait_for_each_other_and_keep_gets_blocked(
    tmp_path, native_gateway, close_user
):
    from egis_agentkit.errors import Conflict

    manager = build_manager(tmp_path, client_factory=TestSandboxClient)
    async with manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        entered, proceed = asyncio.Event(), asyncio.Event()
        original = workspace.client.close_instance

        async def blocked_close():
            entered.set()
            await proceed.wait()
            await original()

        workspace.client.close_instance = blocked_close

        async def close():
            if close_user:
                await manager.close_workspaces(workspace.workspace_id)
            else:
                await manager.close_all()

        first = asyncio.create_task(close())
        await asyncio.wait_for(entered.wait(), timeout=1)
        second = asyncio.create_task(close())
        try:
            for _ in range(4):
                await asyncio.sleep(0)
            assert not second.done(), "相同范围的关闭必须等待在途关闭完成"
            with pytest.raises(Conflict, match="关闭"):
                await manager.get_workspace(
                    "alice", "agent", "other-session", project_id="project"
                )
        finally:
            proceed.set()
            await asyncio.gather(first, second)
        assert not manager._cache
