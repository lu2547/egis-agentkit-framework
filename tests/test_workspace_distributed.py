"""两个 Manager 使用真实 PG 租约，验证跨节点创建、Session 使用保护和回收。"""

import asyncio
import sys
import time
from dataclasses import replace
from functools import partial
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from agentscope.app.message_bus import MessageBusKeys
from agentscope.workspace._sandboxed_base import SandboxedWorkspaceBase
from sandbox_helpers import TestSandboxClient
from sqlalchemy import text


async def managers_at(storage, tmp_path, monkeypatch):
    from egis_agentkit.client.nacos._settings import (
        SandboxWorkspaceConfig,
        WorkspaceLockConfig,
        WorkspaceProbeConfig,
    )
    from egis_agentkit.core.message_bus import EgisMessageBus
    from egis_agentkit.middleware.workspace._workspace_read_guard_middleware_impl import (
        ReadToolGuardMiddleware,
    )
    from egis_agentkit.service.workspace._egis_sandbox_manager import EgisSandboxManager

    async def gateway(workspace):
        workspace._gateway = SimpleNamespace(
            health=AsyncMock(return_value=True), aclose=AsyncMock()
        )

    monkeypatch.setattr(SandboxedWorkspaceBase, "_setup_mcp_gateway", gateway)
    one = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    two = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await one._pg.init_schema()
    instances, events = {}, []

    def create(bus):
        manager = EgisSandboxManager(
            tmp_path,
            SandboxWorkspaceConfig(
                gateway_home=str(tmp_path / "gateway"),
            ),
            client_factory=partial(
                TestSandboxClient, instances=instances, events=events
            ),
            read_guard_factory=ReadToolGuardMiddleware,
            bus=bus,
            probe=WorkspaceProbeConfig(
                storage_python=sys.executable,
                runtime_commands=[[sys.executable, "-c", "import sys"]],
            ),
            lock=WorkspaceLockConfig(lease_seconds=0.3, wait_timeout_seconds=1),
        )
        manager.files.initialize_project("alice", "project")
        return manager

    return create(one), create(two), events


@pytest.mark.asyncio
async def test_same_binding_is_created_once_across_independent_managers(
    storage, tmp_path, monkeypatch
):
    one, two, events = await managers_at(storage, tmp_path, monkeypatch)
    async with one, two:
        a, b = await asyncio.gather(
            *(
                m.get_workspace("alice", "agent", "session", project_id="project")
                for m in (one, two)
            )
        )
        assert a.sandbox_id == b.sandbox_id
        assert sum(event[0] == "create" for event in events) == 1
        assert a is not b


@pytest.mark.asyncio
async def test_remote_session_lease_prevents_idle_node_from_pausing_workspace(
    storage, tmp_path, monkeypatch
):
    one, two, events = await managers_at(storage, tmp_path, monkeypatch)
    async with one, two:
        a = await one.get_workspace("alice", "agent", "session", project_id="project")
        one._cache[a.runtime_binding_id] = (a, time.monotonic() - one._ttl - 1)
        async with two.bus.acquire_lock(
            MessageBusKeys.session_lock("session"), ttl_secs=0.3
        ):
            await one._recycle_idle_workspaces()
            assert not [event for event in events if event[0] == "pause"]
            assert (await a.get_backend().exec_shell(["true"], cwd=str(tmp_path))).ok()
        await one._recycle_idle_workspaces()
        assert any(event[0] == "pause" for event in events)


@pytest.mark.asyncio
async def test_shutdown_only_disconnects_when_another_node_owns_session(
    storage, tmp_path, monkeypatch
):
    one, two, events = await managers_at(storage, tmp_path, monkeypatch)
    async with one, two:
        a = await one.get_workspace("alice", "agent", "session", project_id="project")
        async with two.bus.acquire_lock(
            MessageBusKeys.session_lock("session"), ttl_secs=0.3
        ):
            await one.close_all()
            assert a.client.closed
            assert not [event for event in events if event[0] == "pause"]
            assert not one._cache


@pytest.mark.asyncio
async def test_lost_pg_workspace_lease_cancels_provision_without_pausing_new_owner(
    storage, tmp_path, monkeypatch
):
    from egis_agentkit.core.message_bus._message_bus import LeaseLostError
    from egis_agentkit.core.workspace._identity import sandbox_binding_for

    one, two, events = await managers_at(storage, tmp_path, monkeypatch)
    gate = asyncio.Event()
    one._client_factory = partial(TestSandboxClient, events=events, gate=gate)
    task = asyncio.create_task(
        one.get_workspace("alice", "agent", "session", project_id="project")
    )
    async with asyncio.timeout(2):
        while not events:
            await asyncio.sleep(0)
    key = "ak:workspace:" + sandbox_binding_for("alice", "project", "agent", "session")
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("DELETE FROM ak_bus_leases WHERE key=:key"), {"key": key}
        )
    with pytest.raises(LeaseLostError):
        await asyncio.wait_for(task, timeout=2)
    assert not one._cache and not [event for event in events if event[0] == "pause"]
    await one.close_all()


@pytest.mark.asyncio
async def test_unknown_instance_state_is_not_a_reason_to_create_another(
    storage, tmp_path, monkeypatch
):
    one, two, events = await managers_at(storage, tmp_path, monkeypatch)
    async with one, two:
        a = await one.get_workspace("alice", "agent", "session", project_id="project")
        a.client.instances[a.sandbox_id] = replace(
            a.client.instances[a.sandbox_id], state="error"
        )
        with pytest.raises(Exception) as error:
            await one.get_workspace("alice", "agent", "session", project_id="project")
        assert getattr(error.value, "stage", None) == "instance"
        assert sum(event[0] == "create" for event in events) == 1


@pytest.mark.asyncio
async def test_uncached_destroy_waits_for_remote_workspace_usage_lease(
    storage, tmp_path, monkeypatch
):
    one, two, events = await managers_at(storage, tmp_path, monkeypatch)
    async with one, two:
        a = await one.get_workspace("alice", "agent", "session", project_id="project")
        entered = asyncio.Event()

        async def using():
            async with one.renew_workspace(a):
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(using())
        await asyncio.wait_for(entered.wait(), 1)
        destroying = asyncio.create_task(two.destroy_workspaces("alice", "project"))
        try:
            await asyncio.sleep(0.1)
            assert not destroying.done(), "销毁必须等待远端实例的使用租约"
            assert not [event for event in events if event[0] == "destroy"]
        finally:
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
            await destroying
        assert any(event[0] == "destroy" for event in events)


@pytest.mark.asyncio
async def test_close_cancelled_by_lost_lease_does_not_pause_new_holder(
    storage, tmp_path, monkeypatch
):
    from egis_agentkit.core.message_bus._message_bus import LeaseLostError

    one, two, events = await managers_at(storage, tmp_path, monkeypatch)
    a = await one.get_workspace("alice", "agent", "session", project_id="project")
    entered = asyncio.Event()

    async def waiting():
        entered.set()
        await asyncio.Event().wait()

    monkeypatch.setattr(a, "_close_all_mcp_instances", waiting)
    task = asyncio.create_task(one.close_all())
    await asyncio.wait_for(entered.wait(), 1)
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("DELETE FROM ak_bus_leases WHERE key=:key"),
            {"key": "ak:workspace:" + a.runtime_binding_id},
        )
    with pytest.raises(LeaseLostError):
        await asyncio.wait_for(task, 1)
    assert not [event for event in events if event[0] == "pause"]
    assert a.client.closed


@pytest.mark.asyncio
async def test_failed_gateway_initialization_is_safe_and_short_circuited(
    storage, tmp_path, monkeypatch
):
    one, two, events = await managers_at(storage, tmp_path, monkeypatch)
    attempts = 0

    async def broken(workspace):
        nonlocal attempts
        attempts += 1
        raise RuntimeError("private bootstrap command output")

    monkeypatch.setattr(SandboxedWorkspaceBase, "_setup_mcp_gateway", broken)
    async with one, two:
        for _ in range(2):
            with pytest.raises(Exception) as error:
                await one.get_workspace(
                    "alice", "agent", "session", project_id="project"
                )
            assert getattr(error.value, "stage", None) == "runtime"
            assert "private bootstrap" not in str(error.value)
        assert attempts == 1
