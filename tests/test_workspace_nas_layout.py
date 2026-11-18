"""验证 NAS 挂载源和沙箱目录的初始化顺序。"""

from pathlib import Path

import pytest

from egis_agentkit.errors import Forbidden
from egis_agentkit.storage._files import FileStorage


def test_user_layout_initialization_preserves_existing_files(tmp_path):
    files = FileStorage(tmp_path)
    files.initialize_user_workspace("alice")
    root = tmp_path / "user/alice"
    assert {path.name for path in root.iterdir()} == {
        "skills", "session", "projects"
    }
    (root / "data").mkdir()
    content = root / "data/keep.txt"
    content.write_bytes(b"shared user data")
    files.initialize_user_workspace("alice")
    assert content.read_bytes() == b"shared user data"
    assert not (tmp_path / "user/bob").exists()


def test_skill_runtime_mount_source_is_distinct_from_the_same_named_skill(tmp_path):
    files = FileStorage(tmp_path)
    skills = tmp_path / "user/alice/skills"
    canonical = skills / "report/1.0.0"
    canonical.mkdir(parents=True)
    (canonical / "SKILL.md").write_text("Published instructions")
    files.initialize_user_workspace("alice", runtime_binding_id="report")
    assert (skills / ".runtime/report").is_dir()
    assert (canonical / "SKILL.md").read_text() == "Published instructions"


def test_user_layout_rejects_symlinked_standard_directory(tmp_path):
    root = tmp_path / "user/alice"
    root.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / "skills").symlink_to(outside, target_is_directory=True)
    with pytest.raises(Forbidden):
        FileStorage(tmp_path).initialize_user_workspace("alice")
    assert not list(outside.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize("sandbox_state", ["fresh", "matching", "mismatched"])
async def test_workspace_prepares_mount_sources_before_binding(
    tmp_path, monkeypatch, sandbox_state
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from agentscope.workspace._sandboxed_base import SandboxedWorkspaceBase
    from sandbox_helpers import TestSandboxClient

    from egis_agentkit.client.nacos._settings import SandboxWorkspaceConfig
    from egis_agentkit.client.sandbox import SandboxInstance, SandboxMount
    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.storage.project._files import ProjectFileStorage

    async def gateway(workspace):
        workspace._gateway = SimpleNamespace(aclose=AsyncMock())

    monkeypatch.setattr(SandboxedWorkspaceBase, "_setup_mcp_gateway", gateway)
    client = TestSandboxClient()
    workspace = EgisSandboxWorkspace(
        user_id="alice", agent_id="agent", project_id="project", session_id="session",
        files=ProjectFileStorage(tmp_path), client=client,
        settings=SandboxWorkspaceConfig(gateway_home=str(tmp_path / "gateway")),
    )
    root = Path(workspace.workdir)
    project = root / "projects/project"
    project.mkdir(parents=True)
    (project / "input.txt").write_bytes(b"keep input")

    def check_mount_sources(mounts):
        for mount in mounts:
            source = Path(mount.source)
            assert source.is_dir(), f"Missing NAS mount source: {source}"
            assert source.is_relative_to(root)

    create_instance = client.create_instance
    connect_instance = client.connect_instance

    async def create_after_preparation(metadata, mounts):
        check_mount_sources(mounts)
        await create_instance(metadata, mounts)

    async def connect_after_preparation(instance):
        check_mount_sources(workspace.build_mounts())
        await connect_instance(instance)

    async def restore_after_provision():
        assert (root / "sessions").is_dir()
        assert (tmp_path / "gateway").is_dir()
        return {}

    monkeypatch.setattr(client, "create_instance", create_after_preparation)
    monkeypatch.setattr(client, "connect_instance", connect_after_preparation)
    monkeypatch.setattr(workspace, "_restore_mcp_specs", restore_after_provision)

    if sandbox_state != "fresh":
        metadata = dict(workspace.sandbox_metadata)
        if sandbox_state == "mismatched":
            mounts = [
                SandboxMount("user-data", str(root / "data"), str(root / "data")),
                *workspace.build_mounts(),
            ]
            metadata["agentkit.mount.hash"] = client.mount_fingerprint(mounts)
        client.instances["existing"] = SandboxInstance("existing", "paused", metadata)
    try:
        if sandbox_state == "mismatched":
            with pytest.raises(Forbidden, match="挂载"):
                await workspace.initialize()
            assert client.instance_id is None
            return
        await workspace.initialize()
        if sandbox_state == "matching":
            assert workspace.sandbox_id == "existing"
        assert workspace.workdir == str(tmp_path / "user/alice")
        assert str(tmp_path / "user/alice") in await workspace.get_instructions()
        assert (project / "input.txt").read_bytes() == b"keep input"
        assert not (root / "data").exists()
        assert not (root / "mcp").exists()
        assert not (root / ".mcp").exists()
    finally:
        await workspace.close_workspace()


@pytest.mark.asyncio
async def test_session_recreates_only_missing_project_directories(storage, tmp_path):
    from test_domain import expert, identities

    from egis_agentkit.service.agent._agent_manager import AgentManager
    from egis_agentkit.service.project._project_manager import ProjectManager
    from egis_agentkit.service.session._session import SessionService

    _, _, alice, _ = await identities(storage)
    project = await ProjectManager(storage, tmp_path).create(alice, "Project")
    files = SessionService(storage, tmp_path)
    root = tmp_path / "user" / alice.user_id / "projects" / project["id"]
    (root / "input.txt").write_bytes(b"keep input")
    (root / ".work").rmdir()
    (root / "artifacts").rmdir()
    definition = await AgentManager(storage).add_definition(alice, expert())
    await files.add_session(alice, definition["id"], "private", project_id=project["id"])
    assert (root / ".work").is_dir() and (root / "artifacts").is_dir()
    assert (root / "input.txt").read_bytes() == b"keep input"
    assert not (root / "uploads").exists()


@pytest.mark.asyncio
async def test_session_redundant_initialization_rejects_project_symlink(storage, tmp_path):
    from test_domain import expert, identities

    from egis_agentkit.service.agent._agent_manager import AgentManager
    from egis_agentkit.service.project._project_manager import ProjectManager
    from egis_agentkit.service.session._session import SessionService

    _, _, alice, _ = await identities(storage)
    project = await ProjectManager(storage, tmp_path).create(alice, "Project")
    root = tmp_path / "user" / alice.user_id / "projects" / project["id"]
    (root / ".work").rmdir()
    outside = tmp_path / "outside"
    outside.mkdir()
    (root / ".work").symlink_to(outside, target_is_directory=True)
    definition = await AgentManager(storage).add_definition(alice, expert())
    with pytest.raises(Forbidden):
        await SessionService(storage, tmp_path).add_session(
            alice, definition["id"], "private", project_id=project["id"]
        )
    assert not list(outside.iterdir())


@pytest.mark.asyncio
async def test_branch_session_also_repairs_missing_project_directories(storage, tmp_path):
    from agentscope.app.message_bus import InMemoryMessageBus
    from test_message_actions import history

    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    root = tmp_path / "user" / alice.user_id / "projects" / task["project_id"]
    (root / ".work").rmdir()
    (root / "artifacts").rmdir()
    async with InMemoryMessageBus() as bus:
        await service.add_session_branch(alice, task["session_id"], messages[1].id, bus)
    assert (root / ".work").is_dir() and (root / "artifacts").is_dir()
