"""Service 组织项目释放，Core 执行沙箱销毁；普通关闭保持暂停语义。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from opensandbox import SandboxManager as ProviderManager
from opensandbox.models.sandboxes import SandboxState
from test_opensandbox_workspace import configuration, initialized_provider, manager_type

from egis_agentkit.client.sandbox._opensandbox import OpenSandboxClient
from egis_agentkit.core.workspace._egis_sandbox_workspace import EgisSandboxWorkspace
from egis_agentkit.errors import Forbidden


@pytest.mark.asyncio
async def test_service_releases_cached_handles_then_delegates_destruction_to_core(
    tmp_path, monkeypatch
):
    manager = manager_type()(tmp_path, configuration(tmp_path))
    events = []

    async def close_owned():
        events.append("close")

    owned = SimpleNamespace(
        egis_user_id="alice",
        egis_project_id="project",
        close_workspace=AsyncMock(side_effect=close_owned),
    )
    other = SimpleNamespace(
        egis_user_id="alice", egis_project_id="other", close_workspace=AsyncMock()
    )
    manager._cache = {"owned": (owned, 0), "other": (other, 0)}

    async def destroy(user_id, project_id, *, client):
        assert (user_id, project_id) == ("alice", "project")
        assert isinstance(client, OpenSandboxClient)
        assert client.settings is manager.settings
        events.append("destroy")

    destroy_core = AsyncMock(side_effect=destroy)
    monkeypatch.setattr(
        EgisSandboxWorkspace, "destroy_sandbox_instances", destroy_core, raising=False
    )
    monkeypatch.setattr(
        ProviderManager,
        "create",
        AsyncMock(side_effect=AssertionError("Service 不应直接调用 OpenSandbox SDK")),
    )
    await manager.destroy_workspaces("alice", "project")
    assert events == ["close", "destroy"]
    destroy_core.assert_awaited_once()
    other.close_workspace.assert_not_awaited()
    assert set(manager._cache) == {"other"}


@pytest.mark.asyncio
@pytest.mark.parametrize("foreign", [False, True])
async def test_core_validates_all_pages_before_destroying_owned_running_and_paused_instances(
    tmp_path, monkeypatch, foreign
):
    settings = configuration(tmp_path)
    metadata = {
        "agentkit.user.hash": EgisSandboxWorkspace._hash_metadata(b"alice"),
        "agentkit.project.id": "project",
    }
    events = []

    async def listing(query):
        assert query.states == [SandboxState.RUNNING, SandboxState.PAUSED]
        assert query.metadata == metadata
        page = query.page or 1
        events.append(f"page-{page}")
        actual = (
            {**metadata, "agentkit.project.id": "foreign"}
            if foreign and page == 2
            else metadata
        )
        return SimpleNamespace(
            sandbox_infos=[
                SimpleNamespace(
                    id=f"instance-{page}",
                    metadata=actual,
                    status=SimpleNamespace(state="running"),
                )
            ],
            pagination=SimpleNamespace(page=page, has_next_page=page == 1),
        )

    async def kill(identifier):
        events.append(identifier)

    provider = SimpleNamespace(
        list_sandbox_infos=AsyncMock(side_effect=listing),
        kill_sandbox=AsyncMock(side_effect=kill),
        close=AsyncMock(),
    )
    create = AsyncMock(return_value=provider)
    monkeypatch.setattr(ProviderManager, "create", create)
    method = getattr(EgisSandboxWorkspace, "destroy_sandbox_instances", None)
    assert method is not None, "Core 缺少实例销毁能力"
    if foreign:
        with pytest.raises(Forbidden, match="归属"):
            await method(
                "alice",
                "project",
                client=OpenSandboxClient(settings, api_key="test-only"),
            )
        provider.kill_sandbox.assert_not_awaited()
    else:
        await method(
            "alice", "project", client=OpenSandboxClient(settings, api_key="test-only")
        )
        assert events[:2] == ["page-1", "page-2"]
        assert set(events[2:]) == {"instance-1", "instance-2"}
    assert provider.close.await_count == (1 if foreign else 3)
    connection = create.call_args.kwargs["connection_config"]
    assert connection.domain == settings.domain
    assert connection.use_server_proxy == settings.use_server_proxy


@pytest.mark.asyncio
@pytest.mark.parametrize("failure_stage", ["list_sandbox_infos", "kill_sandbox"])
async def test_core_propagates_provider_failure_and_closes_client(
    tmp_path, monkeypatch, failure_stage
):
    metadata = {
        "agentkit.user.hash": EgisSandboxWorkspace._hash_metadata(b"alice"),
        "agentkit.project.id": "project",
    }
    provider = SimpleNamespace(
        list_sandbox_infos=AsyncMock(
            return_value=SimpleNamespace(
                sandbox_infos=[
                    SimpleNamespace(
                        id="owned",
                        metadata=metadata,
                        status=SimpleNamespace(state="running"),
                    )
                ],
                pagination=SimpleNamespace(has_next_page=False),
            )
        ),
        kill_sandbox=AsyncMock(),
        close=AsyncMock(),
    )
    getattr(provider, failure_stage).side_effect = RuntimeError("provider failed")
    monkeypatch.setattr(ProviderManager, "create", AsyncMock(return_value=provider))
    method = getattr(EgisSandboxWorkspace, "destroy_sandbox_instances", None)
    assert method is not None, "Core 缺少实例销毁能力"
    with pytest.raises(RuntimeError, match="provider failed"):
        await method(
            "alice",
            "project",
            client=OpenSandboxClient(configuration(tmp_path), api_key="test-only"),
        )
    assert provider.close.await_count == (
        1 if failure_stage == "list_sandbox_infos" else 2
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["close", "close_workspaces"])
async def test_normal_close_pauses_without_destroying_sandbox(
    tmp_path, monkeypatch, entrypoint
):
    monkeypatch.setattr(EgisSandboxWorkspace, "initialize", initialized_provider)
    provider_create = AsyncMock(side_effect=AssertionError("普通关闭不得销毁实例"))
    monkeypatch.setattr(ProviderManager, "create", provider_create)
    async with manager_type()(tmp_path, configuration(tmp_path)) as manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        handle = workspace.client._sandbox
        await getattr(manager, entrypoint)(workspace.workspace_id)
        handle.pause.assert_awaited_once()
        handle.close.assert_awaited_once()
        provider_create.assert_not_awaited()


@pytest.mark.asyncio
@pytest.mark.parametrize("entrypoint", ["close", "close_workspace"])
async def test_core_close_entrypoints_pause_and_clear_connections_once(
    tmp_path, monkeypatch, entrypoint
):
    monkeypatch.setattr(EgisSandboxWorkspace, "initialize", initialized_provider)
    async with manager_type()(tmp_path, configuration(tmp_path)) as manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        handle = workspace.client._sandbox
        await getattr(workspace, entrypoint)()
        handle.pause.assert_awaited_once()
        handle.close.assert_awaited_once()
        assert workspace.client._sandbox is None
        assert not workspace.is_alive
