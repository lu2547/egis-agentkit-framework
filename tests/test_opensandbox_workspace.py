"""企业 OpenSandbox 接入复用原生生命周期，并隔离项目挂载。"""

import asyncio
import importlib
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from agentscope.app.workspace_manager import OpenSandboxWorkspaceManager
from agentscope.workspace import OpenSandboxWorkspace
from pydantic import ValidationError

from egis_agentkit.errors import Forbidden


def configuration(tmp_path):
    settings = importlib.import_module("egis_agentkit.client.nacos._settings")
    config_type = getattr(settings, "OpenSandboxConfig", None)
    assert config_type is not None, "缺少类型化 OpenSandbox 配置"
    return config_type(
        domain="127.0.0.1:8090",
        api_key_env="TEST_SANDBOX_KEY",
        host_root=str(tmp_path / "docker-host"),
    )


def manager_type():
    module = importlib.import_module("egis_agentkit.service.workspace._egis_sandbox_manager")
    return module.EgisSandboxManager


async def initialized_provider(workspace):
    """替代远程创建，保留 Manager 所需的真实 SDK 生命周期接口。"""
    workspace.is_alive = True
    workspace._sandbox = SimpleNamespace(
        renew=AsyncMock(), pause=AsyncMock(), close=AsyncMock()
    )


def test_opensandbox_configuration_rejects_invalid_connection_and_mount_root(tmp_path):
    config = configuration(tmp_path)
    assert config.use_server_proxy
    with pytest.raises(ValidationError):
        type(config)(domain="http://user:secret@host/path", host_root="relative")


@pytest.mark.asyncio
async def test_native_manager_reuses_workspace_and_rejects_cross_user_binding(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")

    monkeypatch.setattr(OpenSandboxWorkspace, "initialize", initialized_provider)
    manager = manager_type()(tmp_path / "app", configuration(tmp_path))
    assert isinstance(manager, OpenSandboxWorkspaceManager)
    async with manager:
        first, same = await asyncio.gather(
            *[
                manager.get_workspace("alice", "agent", "session", project_id="project")
                for _ in range(2)
            ]
        )
        assert first is same
        another = await manager.get_workspace(
            "alice", "agent", "session-2", project_id="another"
        )
        assert another is not first
        isolated = await manager.get_workspace(
            "bob", "agent", "session", project_id="project"
        )
        assert isolated is not first
        assert isolated.egis_user_id == "bob"


@pytest.mark.asyncio
async def test_project_mounts_expose_only_current_project_and_protect_inputs(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")

    monkeypatch.setattr(OpenSandboxWorkspace, "initialize", initialized_provider)
    manager = manager_type()(tmp_path / "app", configuration(tmp_path))
    async with manager:
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        mounts = workspace.build_project_mounts()
        sources = {
            mount.name: (mount.host.path, mount.mount_path, mount.read_only)
            for mount in mounts
        }
        host = tmp_path / "docker-host/user/alice/projects/project"
        target = tmp_path / "app/user/alice/projects/project"
        assert sources == {
            "project-inputs": (str(host), str(target), True),
            "project-work": (str(host / ".work"), str(target / ".work"), False),
            "project-artifacts": (
                str(host / "artifacts"),
                str(target / "artifacts"),
                False,
            ),
        }
        assert all("common" not in str(Path(mount.host.path).parts) for mount in mounts)


@pytest.mark.asyncio
async def test_failed_provision_is_not_cached_and_can_retry(tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")
    attempts = 0

    async def initialize(workspace):
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("provider unavailable")
        await initialized_provider(workspace)

    monkeypatch.setattr(OpenSandboxWorkspace, "initialize", initialize)
    manager = manager_type()(tmp_path / "app", configuration(tmp_path))
    async with manager:
        with pytest.raises(RuntimeError, match="provider unavailable"):
            await manager.get_workspace(
                "alice", "agent", "session", project_id="project"
            )
        workspace = await manager.get_workspace(
            "alice", "agent", "session", project_id="project"
        )
        assert workspace.is_alive and attempts == 2


@pytest.mark.asyncio
async def test_activated_skill_is_published_inside_backend_without_exposing_other_caches(
    tmp_path, monkeypatch
):
    from agentscope.skill import Skill
    from agentscope.tool import LocalBackend

    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.storage.project._files import ProjectFileStorage

    source = tmp_path / "app/common/selected/skills/report"
    source.mkdir(parents=True)
    (source / "SKILL.md").write_text("# 报告\n使用 scripts/check.py")
    (source / "scripts").mkdir()
    (source / "scripts/check.py").write_text("print('report')")
    other = tmp_path / "app/common/private/secret.txt"
    other.parent.mkdir(parents=True)
    other.write_text("other-resource")
    workspace = EgisSandboxWorkspace(
        user_id="alice",
        agent_id="agent",
        project_id="project",
        files=ProjectFileStorage(tmp_path / "app"),
        settings=configuration(tmp_path),
    )
    # Exercise the real file Backend without provisioning a remote provider here.
    workspace.workdir = str(tmp_path / "runtime")
    workspace._backend = LocalBackend()
    skill = Skill("report", "报告", str(source), "# 报告", 0)
    published = await workspace.publish_skill(
        skill, agent_id="agent", checksum="a" * 64
    )
    target = Path(published.dir)
    assert target.is_relative_to(tmp_path / "runtime")
    assert (target / "SKILL.md").read_text().startswith("# 报告")
    assert (target / "scripts/check.py").read_text() == "print('report')"
    assert not any(p.name == "secret.txt" for p in (tmp_path / "runtime").rglob("*"))


@pytest.mark.asyncio
async def test_gateway_tools_keep_managed_connection_source():
    import json

    from agentscope.mcp import HttpMCPConfig

    module = importlib.import_module(
        "egis_agentkit.extension.workspace._opensandbox_mcp_client_override"
    )

    class ToolEndpoint:
        async def exec_request(self, method, path, **kwargs):
            return 200, json.dumps(
                [
                    {
                        "name": "echo",
                        "description": "echo",
                        "inputSchema": {"type": "object"},
                    }
                ]
            ).encode()

    client = module.AuthorizedGatewayMCPClient(
        name="managed",
        is_stateful=True,
        mcp_config=HttpMCPConfig(url="https://connector.example/mcp"),
    )
    client.attach(
        ToolEndpoint(), agent_id="agent", session_id="session", connected=True
    )
    tool = await client.get_tool("echo")
    assert tool.mcp_name == "managed"
    assert tool._egis_mcp_client is client


@pytest.mark.asyncio
async def test_skill_loader_publishes_runtime_path_but_persists_host_cache(tmp_path):
    from agentscope.state import AgentState
    from agentscope.tool import LocalBackend
    from test_runtime_middleware_components import Resources, scope_at

    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.extension.resource._skill_loader_impl import LiveSkillLoader
    from egis_agentkit.storage.project._files import ProjectFileStorage

    root = tmp_path / "app"
    resources = Resources(root, [])
    source = Path(resources.materialized.dir)
    source.mkdir(parents=True)
    (source / "SKILL.md").write_text("Full report instructions")
    workspace = EgisSandboxWorkspace(
        user_id="alice",
        agent_id="agent-one",
        project_id="project-one",
        files=ProjectFileStorage(root),
        settings=configuration(tmp_path),
    )
    workspace.workdir = str(tmp_path / "runtime")
    workspace._backend = LocalBackend()
    loader = LiveSkillLoader(object(), scope_at(root), resources, workspace=workspace)
    state = AgentState()
    await loader.bind_state(state)
    await loader.activate("report")
    assert loader.loaded["report"].dir != str(source)
    assert (
        Path(loader.loaded["report"].dir) / "SKILL.md"
    ).read_text() == "Full report instructions"
    pin = state.middle_context["egis_agentkit"]["activated_skills"]["report"]
    assert pin["cache_dir"] == "skills/report"
    assert (
        resources.saved[-1][-1].middle_context["egis_agentkit"]["activated_skills"][
            "report"
        ]
        == pin
    )

    runtime_dir = Path(loader.loaded["report"].dir)

    async def revoked(*args, **kwargs):
        raise Forbidden("revoked")

    resources.skill_descriptors = revoked
    await loader.refresh_access()
    assert not runtime_dir.exists()
    assert (source / "SKILL.md").exists()


@pytest.mark.asyncio
async def test_bootstrap_selects_native_sandbox_manager(storage, tmp_path, monkeypatch):
    from test_app_http import Config, app_snapshot

    from egis_agentkit.bootstrap import init_agentkit

    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")
    settings = configuration(tmp_path)
    config = Config(
        app_snapshot(
            tmp_path,
            workspace={
                "runtime": {
                    "root": str(tmp_path),
                    "mode": "opensandbox",
                    "opensandbox": settings.model_dump(),
                },
            },
        )
    )
    kit = init_agentkit(
        config,
        None,
        "postgresql+asyncpg://unused",
        engine=storage.engine,
        owns_client=False,
    )
    assert isinstance(kit.workspace, OpenSandboxWorkspaceManager)
    assert not hasattr(kit.workspace, "resources")


def test_binding_metadata_obeys_opensandbox_label_contract(tmp_path):
    import re

    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.storage.project._files import ProjectFileStorage

    workspace = EgisSandboxWorkspace(
        user_id="alice",
        agent_id="agent",
        project_id="project",
        files=ProjectFileStorage(tmp_path / "app"),
        settings=configuration(tmp_path),
    )
    for value in workspace._binding_metadata.values():
        assert re.fullmatch(r"[a-zA-Z0-9]([a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?", value)


@pytest.mark.asyncio
async def test_external_user_subject_remains_valid_provider_metadata(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")

    monkeypatch.setattr(OpenSandboxWorkspace, "initialize", initialized_provider)
    async with manager_type()(tmp_path / "app", configuration(tmp_path)) as manager:
        workspace = await manager.get_workspace(
            "张三@example.com", "agent", "session", project_id="project"
        )
        import re

        for value in workspace.sandbox_metadata.values():
            assert re.fullmatch(r"[a-zA-Z0-9]([a-zA-Z0-9._-]{0,61}[a-zA-Z0-9])?", value)


@pytest.mark.asyncio
@pytest.mark.parametrize("agent_id", [".", ".."])
async def test_skill_partition_rejects_parent_segments(tmp_path, agent_id):
    from agentscope.skill import Skill

    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.storage.project._files import ProjectFileStorage

    workspace = EgisSandboxWorkspace(
        user_id="alice",
        agent_id="agent",
        project_id="project",
        files=ProjectFileStorage(tmp_path / "app"),
        settings=configuration(tmp_path),
    )
    with pytest.raises(Forbidden, match="分区"):
        await workspace.publish_skill(
            Skill("report", "", str(tmp_path), "", 0),
            agent_id=agent_id,
            checksum="a" * 64,
        )


@pytest.mark.asyncio
async def test_gateway_tool_uses_same_resource_authorization_and_revocation(
    storage, tmp_path
):
    import json

    from test_domain import expert, identities

    from egis_agentkit.extension.runtime._session_lock_override import (
        ExecutionCoordinator,
    )
    from egis_agentkit.extension.workspace._opensandbox_mcp_client_override import (
        AuthorizedGatewayMCPClient,
    )
    from egis_agentkit.service._session import SessionService
    from egis_agentkit.service.agent._agent_manager import AgentManager
    from egis_agentkit.service.resource._catalog import ResourceService

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_mcp(
        alice,
        {
            "resource_id": "gateway-check",
            "config": {
                "transport": "streamable_http",
                "url": "https://mcp.example.test/mcp",
                "auth_type": "none",
            },
        },
    )
    definition = expert()
    refs = [
        {
            "resource_type": "mcp",
            "resource_id": "gateway-check",
            "resource_version": "1.0.0",
            "resource_view_type": "private",
        }
    ]
    definition.payload["bindings"] = refs
    saved = await AgentManager(storage).add_definition(alice, definition)
    session = await SessionService(storage, tmp_path).add_session(
        alice, saved["id"], "private"
    )

    class Endpoint:
        async def exec_request(self, *args, **kwargs):
            return 200, json.dumps(
                [{"name": "echo", "inputSchema": {"type": "object"}}]
            ).encode()

    async with ExecutionCoordinator(storage, tmp_path).bind(
        session["session_id"]
    ) as scope:
        declaration = (
            await resources.mcps_for_refs(alice, refs, authorization_scope=scope)
        )[0]
        client = AuthorizedGatewayMCPClient.model_validate(declaration.model_dump())
        client.attach(
            Endpoint(),
            agent_id=scope.agent_id,
            session_id=scope.session_id,
            connected=True,
        )
        tool = await client.get_tool("echo")
        assert await resources.authorize_mcp_tool(alice, scope, tool) is True
        await resources.set_asset_enabled(alice, "mcp", "gateway-check", "1.0.0", False)
        with pytest.raises(Forbidden):
            await resources.authorize_mcp_tool(alice, scope, tool)


@pytest.mark.asyncio
async def test_agents_and_sessions_do_not_share_running_resources(
    tmp_path, monkeypatch
):
    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")

    monkeypatch.setattr(OpenSandboxWorkspace, "initialize", initialized_provider)
    async with manager_type()(tmp_path / "app", configuration(tmp_path)) as manager:
        a = await manager.get_workspace("alice", "writer", "one", project_id="project")
        b = await manager.get_workspace(
            "alice", "reviewer", "two", project_id="project"
        )
        c = await manager.get_workspace(
            "alice", "writer", "three", project_id="project"
        )
        assert len({id(a), id(b), id(c)}) == 3
        assert len({a.workspace_id, b.workspace_id, c.workspace_id}) == 1
        assert (
            len({a.runtime_binding_id, b.runtime_binding_id, c.runtime_binding_id}) == 3
        )
        assert a.build_project_mounts() == b.build_project_mounts() == c.build_project_mounts()


@pytest.mark.asyncio
async def test_skill_publication_reuses_complete_files_and_failure_preserves_them(
    tmp_path,
):
    from agentscope.skill import Skill
    from agentscope.tool import LocalBackend

    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.storage.project._files import ProjectFileStorage

    source = tmp_path / "app/common/report"
    source.mkdir(parents=True)
    (source / "SKILL.md").write_text("# Original")
    workspace = EgisSandboxWorkspace(
        user_id="alice",
        agent_id="agent",
        project_id="project",
        files=ProjectFileStorage(tmp_path / "app"),
        settings=configuration(tmp_path),
    )
    workspace.workdir = str(tmp_path / "runtime")
    backend = LocalBackend()
    workspace._backend = backend
    skill = Skill("report", "", str(source), "# Original", 0)
    first = await workspace.publish_skill(skill, agent_id="agent", checksum="a" * 64)

    async def failed_write(*args):
        raise OSError("transfer interrupted")

    backend.write_file = failed_write
    again = await workspace.publish_skill(skill, agent_id="agent", checksum="a" * 64)
    assert again.dir == first.dir
    with pytest.raises(OSError, match="interrupted"):
        await workspace.publish_skill(skill, agent_id="agent", checksum="b" * 64)
    assert (Path(first.dir) / "SKILL.md").read_text() == "# Original"
    assert not list(Path(first.dir).parent.glob("*.staging-*"))


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_gateway_credentials_use_memory_and_cleanup_on_failed_request(
    tmp_path, monkeypatch, failure
):
    from agentscope.tool import ExecResult
    from agentscope.workspace._gateway_client import GatewayClient

    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.storage.project._files import ProjectFileStorage

    class MemoryBoundary:
        def __init__(self):
            self.files, self.writes = {}, []

        async def exec_shell(self, command, **kwargs):
            if command[0] == "stat":
                return ExecResult(0, b"tmpfs\n", b"")
            raise failure("transport stopped")

        async def write_file(self, path, data):
            self.writes.append(path)
            self.files[path] = data

        async def delete_path(self, path):
            self.files.pop(path, None)

    backend = MemoryBoundary()

    async def native_setup(workspace):
        workspace._gateway = GatewayClient(backend, workspace.gateway_port)

    monkeypatch.setattr(OpenSandboxWorkspace, "_setup_mcp_gateway", native_setup)
    workspace = EgisSandboxWorkspace(
        user_id="alice",
        agent_id="agent",
        project_id="project",
        files=ProjectFileStorage(tmp_path / "app"),
        settings=configuration(tmp_path),
    )
    workspace._backend = backend
    await workspace._setup_mcp_gateway()
    with pytest.raises(failure):
        await workspace._gateway.exec_request(
            "POST", "/mcps", body={"headers": {"Authorization": "synthetic-only"}}
        )
    assert backend.writes and all(
        path.startswith("/dev/shm/") for path in backend.writes
    )
    assert backend.files == {}


@pytest.mark.asyncio
async def test_project_release_destroys_only_owned_provider_bindings(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace

    from opensandbox import SandboxManager as ProviderManager

    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )

    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")
    manager = manager_type()(tmp_path, configuration(tmp_path))
    owner = EgisSandboxWorkspace._hash_metadata(b"alice")
    closed, killed, pages = [], [], []

    class Provider:
        async def list_sandbox_infos(self, query):
            pages.append(query)
            assert query.metadata == {
                "agentkit.user.hash": owner,
                "agentkit.project.id": "project",
            }
            return SimpleNamespace(
                sandbox_infos=[SimpleNamespace(id="owned", metadata=query.metadata)],
                pagination=SimpleNamespace(has_next_page=False),
            )

        async def kill_sandbox(self, identifier):
            killed.append(identifier)

        async def close(self):
            closed.append("provider")

    async def connect(**kwargs):
        return Provider()

    monkeypatch.setattr(ProviderManager, "create", connect)
    await manager.destroy_workspaces("alice", "project")
    assert killed == ["owned"] and closed == ["provider"]
    assert len(pages) == 1
