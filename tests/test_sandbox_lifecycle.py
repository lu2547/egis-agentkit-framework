"""沙箱到期后恢复项目目录，执行中的实例续期且不被空闲回收。"""

import asyncio
import time
from datetime import timedelta

import pytest
from agentscope.tool import LocalBackend
from agentscope.workspace import OpenSandboxWorkspace
from opensandbox.exceptions import SandboxApiException
from test_opensandbox_workspace import configuration, manager_type


class ProviderHandle:
    """仅替代远程 SDK；缓存、续期、回收和项目文件均走真实实现。"""

    def __init__(self):
        self.failure = None
        self.expires_at = 0
        self.closed = False
        self.renewed = asyncio.Event()

    async def renew(self, timeout):
        if self.failure:
            raise SandboxApiException("provider failure", status_code=self.failure)
        self.expires_at = time.monotonic() + timeout.total_seconds()
        self.renewed.set()

    async def pause(self):
        pass

    async def close(self):
        self.closed = True


@pytest.fixture
def sandbox_manager(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")

    async def initialize(workspace):
        workspace._sandbox = ProviderHandle()
        workspace._backend = LocalBackend()

    monkeypatch.setattr(OpenSandboxWorkspace, "initialize", initialize)
    return manager_type()(tmp_path / "app", configuration(tmp_path))


@pytest.mark.asyncio
async def test_missing_cached_sandbox_is_rebuilt_with_original_project_files(
    sandbox_manager,
):
    async with sandbox_manager as manager:
        manager.files.initialize_project("alice", "project")
        manager.files.write(
            "alice", "project", "input.txt", b"original", area="uploads"
        )
        old = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        handle = old._sandbox
        handle.failure = 404
        recovered = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        assert recovered is not old
        assert handle.closed
        assert recovered.project_mounts() == old.project_mounts()
        assert (
            await recovered.get_backend().read_file(
                str(manager.files.project_root("alice", "project") / "input.txt")
            )
            == b"original"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [403, 500, 503])
async def test_provider_failure_does_not_create_duplicate_sandbox(
    sandbox_manager, status
):
    async with sandbox_manager as manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        workspace._sandbox.failure = status
        with pytest.raises(SandboxApiException):
            await manager.get_workspace(
                "alice", "agent", "session", project_id="project"
            )
        workspace._sandbox.failure = None
        assert (
            await manager.get_workspace(
                "alice", "agent", "session", project_id="project"
            )
            is workspace
        )


@pytest.mark.asyncio
async def test_active_reply_is_not_evicted_but_idle_workspace_can_be(sandbox_manager):
    async with sandbox_manager as manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        handle = workspace._sandbox
        async with manager.keep_alive(workspace):
            manager._cache[workspace.runtime_binding_id] = (
                workspace,
                time.monotonic() - manager._ttl - 1,
            )
            await manager._sweep_once()
            assert workspace.get_backend() is not None
            assert not handle.closed
        assert (
            manager._cache[workspace.runtime_binding_id][1]
            > time.monotonic() - manager._ttl
        )
        manager._cache[workspace.runtime_binding_id] = (
            workspace,
            time.monotonic() - manager._ttl - 1,
        )
        await manager._sweep_once()
        assert handle.closed
        assert workspace.runtime_binding_id not in manager._cache


@pytest.mark.asyncio
async def test_active_reply_renews_provider_deadline_and_stops_after_completion(
    sandbox_manager,
):
    manager = sandbox_manager
    manager.settings = manager.settings.model_copy(
        update={"keepalive_interval_seconds": 0.01}
    )
    async with manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        handle = workspace._sandbox
        async with manager.keep_alive(workspace):
            handle.expires_at = 0
            handle.renewed.clear()
            await asyncio.wait_for(handle.renewed.wait(), timeout=1)
            assert (
                handle.expires_at
                > time.monotonic() + timedelta(minutes=59).total_seconds()
            )
        # 退出后让事件循环继续运行，不能留下续期任务。
        handle.renewed.clear()
        with pytest.raises(TimeoutError):
            await asyncio.wait_for(handle.renewed.wait(), timeout=0.04)


@pytest.mark.asyncio
async def test_missing_sandbox_during_reply_stops_execution_instead_of_replaying_tools(
    sandbox_manager,
):
    manager = sandbox_manager
    manager.settings = manager.settings.model_copy(
        update={"keepalive_interval_seconds": 0.01}
    )
    async with manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        completed = False
        with pytest.raises(SandboxApiException):
            async with manager.keep_alive(workspace):
                workspace._sandbox.failure = 404
                await asyncio.wait_for(asyncio.Event().wait(), timeout=1)
                completed = True
        assert not completed
        recovered = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        assert recovered is not workspace


@pytest.mark.asyncio
async def test_session_reply_wiring_protects_workspace_until_stream_finishes(
    sandbox_manager, tmp_path
):
    from types import SimpleNamespace

    from agentscope.state import AgentState
    from runtime_helpers import session_middlewares
    from test_runtime_middleware_components import Changes, Users, scope_at

    from egis_agentkit.extension._session_lock_override import execution_scope

    async with sandbox_manager as manager:
        workspace = await manager.get_workspace(
            "alice", "agent-one", "session-two", project_id="project-one"
        )
        handle = workspace._sandbox
        token = execution_scope.set(scope_at(tmp_path))
        try:
            factory = session_middlewares(
                None, users=Users(), file_changes=Changes(), workspace_manager=manager
            )
            middlewares = await factory("alice", "agent-one", "session-two", workspace)

            async def terminal(**kwargs):
                manager._cache[workspace.runtime_binding_id] = (
                    workspace,
                    time.monotonic() - manager._ttl - 1,
                )
                await manager._sweep_once()
                assert not handle.closed
                yield "still executing"

            agent = SimpleNamespace(state=AgentState())
            events = [
                event async for event in middlewares[0].on_reply(agent, {}, terminal)
            ]
            assert events == ["still executing"]
        finally:
            execution_scope.reset(token)


def test_keepalive_interval_must_be_shorter_than_sandbox_lifetime(tmp_path):
    from pydantic import ValidationError

    config = configuration(tmp_path)
    with pytest.raises(ValidationError):
        type(config)(
            **{
                **config.model_dump(),
                "keepalive_interval_seconds": config.timeout_seconds,
            }
        )


@pytest.mark.asyncio
async def test_reply_exception_is_preserved_and_workspace_becomes_idle(sandbox_manager):
    async with sandbox_manager as manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        with pytest.raises(ValueError, match="reply failed"):
            async with manager.keep_alive(workspace):
                raise ValueError("reply failed")
        manager._cache[workspace.runtime_binding_id] = (
            workspace,
            time.monotonic() - manager._ttl - 1,
        )
        await manager._sweep_once()
        assert workspace.runtime_binding_id not in manager._cache


@pytest.mark.asyncio
async def test_cancelled_reply_releases_workspace_for_idle_collection(sandbox_manager):
    async with sandbox_manager as manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        entered = asyncio.Event()

        async def reply():
            async with manager.keep_alive(workspace):
                entered.set()
                await asyncio.Event().wait()

        task = asyncio.create_task(reply())
        await asyncio.wait_for(entered.wait(), timeout=1)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        manager._cache[workspace.runtime_binding_id] = (
            workspace,
            time.monotonic() - manager._ttl - 1,
        )
        await manager._sweep_once()
        assert workspace.runtime_binding_id not in manager._cache
