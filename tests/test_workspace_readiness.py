"""分层检查必须发现真实文件映射、依赖和跨节点租约故障，失败后不交付环境。"""

import asyncio
import errno
import sys
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from agentscope.workspace._sandboxed_base import SandboxedWorkspaceBase
from sandbox_helpers import TestSandboxClient


async def workspace_at(tmp_path, monkeypatch):
    from egis_agentkit.client.nacos._settings import (
        SandboxWorkspaceConfig,
        WorkspaceProbeConfig,
    )
    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.storage.project._files import ProjectFileStorage

    async def gateway(workspace):
        workspace._gateway = SimpleNamespace(
            health=AsyncMock(return_value=True), aclose=AsyncMock()
        )

    monkeypatch.setattr(SandboxedWorkspaceBase, "_setup_mcp_gateway", gateway)
    files = ProjectFileStorage(tmp_path)
    files.initialize_project("alice", "project")
    client = TestSandboxClient()
    workspace = EgisSandboxWorkspace(
        user_id="alice",
        project_id="project",
        agent_id="agent",
        session_id="session",
        files=files,
        client=client,
        probe=WorkspaceProbeConfig(
            storage_python=sys.executable,
            runtime_commands=[[sys.executable, "-c", "import sys"]],
        ),
        settings=SandboxWorkspaceConfig(
            workdir=str(tmp_path / "runtime"),
            gateway_home=str(tmp_path / "gateway"),
        ),
    )
    await workspace.initialize()

    # Only provider communication is substituted; Core checks and file IO are real.
    async def check_connection(*, reconnect=False):
        return client.instances[client.instance_id]

    client.check_connection = check_connection
    return workspace


@pytest.mark.asyncio
async def test_ready_checks_read_actual_nas_both_ways_and_leave_no_probe_files(
    tmp_path, monkeypatch
):
    workspace = await workspace_at(tmp_path, monkeypatch)
    # Runtime command policy comes from typed config in production.
    workspace.probe_settings = SimpleNamespace(
        timeout_seconds=2,
        retry_attempts=2,
        retry_delay_seconds=0.001,
        interval_seconds=30,
        full_interval_seconds=300,
        runtime_commands=[[sys.executable, "-c", "import sys; print(sys.version)"]],
        command=["sh", "-c", ":"],
        storage_python=sys.executable,
    )
    try:
        await workspace.check_ready(full=True)
        assert not [p for p in tmp_path.rglob("*") if p.is_file()]
    finally:
        await workspace.close_workspace()


@pytest.mark.asyncio
async def test_wrong_but_writable_mount_is_rejected(tmp_path, monkeypatch):
    workspace = await workspace_at(tmp_path, monkeypatch)
    original = workspace.client.read_file

    async def foreign_read(path):
        if ".agentkit-probes" in path:
            return b"wrong mount contents"
        return await original(path)

    workspace.client.read_file = foreign_read
    with pytest.raises(Exception) as error:
        await workspace.check_ready(full=True)
    assert getattr(error.value, "stage", None) == "storage"
    assert getattr(error.value, "reason", None) == "mount_mismatch"
    assert not [p for p in tmp_path.rglob("*") if p.is_file()]
    await workspace.close_workspace()


@pytest.mark.asyncio
async def test_runtime_failure_is_classified_without_recreating_sandbox(
    tmp_path, monkeypatch
):
    from agentscope.tool import ExecResult

    workspace = await workspace_at(tmp_path, monkeypatch)
    original = workspace.client.exec_command

    async def broken(command, **kwargs):
        if command and command[0].endswith("python"):
            return ExecResult(127, b"", b"private runtime details")
        return await original(command, **kwargs)

    workspace.client.exec_command = broken
    with pytest.raises(Exception) as error:
        await workspace.check_ready(full=True)
    assert getattr(error.value, "stage", None) == "runtime"
    assert getattr(error.value, "retryable", True) is False
    assert "private runtime details" not in str(error.value)
    assert len(workspace.client.instances) == 1
    await workspace.close_workspace()


@pytest.mark.asyncio
async def test_bad_metadata_cannot_be_accepted_as_healthy(tmp_path, monkeypatch):
    workspace = await workspace_at(tmp_path, monkeypatch)
    instance = workspace.client.instances[workspace.sandbox_id]
    workspace.client.instances[instance.id] = replace(
        instance, metadata={**instance.metadata, "agentkit.project.id": "foreign"}
    )
    with pytest.raises(Exception) as error:
        await workspace.check_ready(full=True)
    assert getattr(error.value, "stage", None) == "instance"
    await workspace.close_workspace()


@pytest.mark.asyncio
async def test_network_retry_reconnects_same_instance_without_replaying_tool(
    tmp_path, monkeypatch
):
    from egis_agentkit.client.sandbox import SandboxConnectionError

    workspace = await workspace_at(tmp_path, monkeypatch)
    observed = []

    async def flaky(*, reconnect=False):
        observed.append(reconnect)
        if not reconnect:
            raise SandboxConnectionError()
        return workspace.client.instances[workspace.sandbox_id]

    workspace.client.check_connection = flaky
    await workspace.check_ready(full=True)
    assert observed == [False, True]
    assert len(workspace.client.instances) == 1
    await workspace.close_workspace()


@pytest.mark.asyncio
async def test_storage_failure_cleans_random_file_and_keeps_business_file(
    tmp_path, monkeypatch
):
    workspace = await workspace_at(tmp_path, monkeypatch)
    target = (
        workspace.egis_files.project_root("alice", "project")
        / "artifacts"
        / "report.md"
    )
    target.write_text("business data")

    from agentscope.tool import ExecResult

    original = workspace.client.exec_command

    async def read_only(command, **kwargs):
        if len(command) > 2 and "os.O_RDWR" in command[2]:
            return ExecResult(1, str(errno.EROFS).encode(), b"private upstream text")
        return await original(command, **kwargs)

    workspace.client.exec_command = read_only
    with pytest.raises(Exception) as error:
        await workspace.check_ready(full=True)
    assert getattr(error.value, "stage", None) == "storage"
    assert getattr(error.value, "reason", None) == "permission"
    assert target.read_text() == "business data"
    assert [p for p in tmp_path.rglob("*") if p.is_file()] == [target]
    await workspace.close_workspace()


@pytest.mark.asyncio
async def test_cancelled_storage_check_cleans_its_files(tmp_path, monkeypatch):
    workspace = await workspace_at(tmp_path, monkeypatch)
    entered = asyncio.Event()

    async def waiting(path):
        entered.set()
        await asyncio.Event().wait()

    workspace.client.read_file = waiting
    task = asyncio.create_task(workspace.check_ready(full=True))
    await asyncio.wait_for(entered.wait(), timeout=2)
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert not [p for p in tmp_path.rglob("*") if p.is_file()]
    await workspace.close_workspace()


@pytest.mark.asyncio
async def test_reserved_probe_files_are_invisible_to_lists_and_change_scan(
    tmp_path, monkeypatch
):
    from egis_agentkit.service.project._file_manager import ProjectFileManager

    workspace = await workspace_at(tmp_path, monkeypatch)
    files = workspace.egis_files
    files.write_probe_file("alice", "project", "artifacts", "canary", b"internal")
    assert files.list("alice", "project", "artifacts") == []
    assert ProjectFileManager(None, tmp_path).scan("alice", "project") == {}
    files.remove_probe_file("alice", "project", "artifacts", "canary")
    await workspace.close_workspace()


@pytest.mark.asyncio
async def test_probe_name_collision_never_overwrites_or_deletes_another_file(
    tmp_path, monkeypatch
):
    import egis_agentkit.core.workspace._checks._egis_sandbox_checks as module

    workspace = await workspace_at(tmp_path, monkeypatch)
    files = workspace.egis_files
    path = files.write_probe_file(
        "alice", "project", "work", "collision", b"other owner"
    )
    monkeypatch.setattr(module, "uuid4", lambda: SimpleNamespace(hex="collision"))
    try:
        with pytest.raises(Exception) as error:
            await workspace.check_ready(full=True)
        assert getattr(error.value, "stage", None) == "storage"
        assert Path(path).read_bytes() == b"other owner"
    finally:
        files.remove_probe_file("alice", "project", "work", "collision")
        await workspace.close_workspace()


@pytest.mark.asyncio
async def test_cancelled_host_write_cleans_when_blocked_nas_write_eventually_finishes(
    tmp_path, monkeypatch
):
    import threading

    workspace = await workspace_at(tmp_path, monkeypatch)
    entered, finished = asyncio.Event(), asyncio.Event()
    release = threading.Event()
    loop = asyncio.get_running_loop()
    original = workspace.egis_files.write_probe_file

    def blocked(*args):
        loop.call_soon_threadsafe(entered.set)
        release.wait(2)
        try:
            return original(*args)
        finally:
            loop.call_soon_threadsafe(finished.set)

    monkeypatch.setattr(workspace.egis_files, "write_probe_file", blocked)
    task = asyncio.create_task(workspace.check_ready(full=True))
    await asyncio.wait_for(entered.wait(), 1)
    task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await asyncio.wait_for(finished.wait(), 1)
    async with asyncio.timeout(0.5):
        while [p for p in tmp_path.rglob("*") if p.is_file()]:
            await asyncio.sleep(0.01)
    await workspace.close_workspace()


@pytest.mark.asyncio
async def test_late_remote_probe_write_cannot_recreate_cleaned_file(
    tmp_path, monkeypatch
):
    workspace = await workspace_at(tmp_path, monkeypatch)
    entered, release = asyncio.Event(), asyncio.Event()
    original_write = workspace.client.write_file
    original_exec = workspace.client.exec_command
    server_tasks = []

    async def defer(operation):
        async def serve():
            entered.set()
            await release.wait()
            return await operation()

        task = asyncio.create_task(serve())
        server_tasks.append(task)
        return await asyncio.shield(task)

    async def delayed_write(path, content):
        return await defer(lambda: original_write(path, content))

    async def delayed_exec(command, **kwargs):
        if len(command) > 2 and "os.O_RDWR" in command[2]:
            return await defer(lambda: original_exec(command, **kwargs))
        return await original_exec(command, **kwargs)

    workspace.client.write_file = delayed_write
    workspace.client.exec_command = delayed_exec
    task = asyncio.create_task(workspace.check_ready(full=True))
    await asyncio.wait_for(entered.wait(), 2)
    task.cancel()
    try:
        with pytest.raises(asyncio.CancelledError):
            await task
    finally:
        release.set()
        await asyncio.gather(*server_tasks, return_exceptions=True)
    assert not [path for path in tmp_path.rglob("*") if path.is_file()]
    await workspace.close_workspace()
