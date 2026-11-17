"""用户标准目录落在 NAS，项目目录由项目创建与会话兜底共同保证。"""

from pathlib import Path

import pytest

from egis_agentkit.errors import Forbidden
from egis_agentkit.storage._files import FileStorage


def test_user_layout_initialization_preserves_existing_files(tmp_path):
    files = FileStorage(tmp_path)
    files.initialize_user_workspace("alice")
    root = tmp_path / "user/alice"
    assert {path.name for path in root.iterdir()} == {
        "data", "skills", "session", "mcp", "projects"
    }
    content = root / "data/keep.txt"
    content.write_bytes(b"shared user data")
    files.initialize_user_workspace("alice")
    assert content.read_bytes() == b"shared user data"
    assert not (tmp_path / "user/bob").exists()


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
async def test_workspace_initializes_user_mount_sources_without_creating_project(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from agentscope.workspace._sandboxed_base import SandboxedWorkspaceBase
    from sandbox_helpers import TestSandboxClient

    from egis_agentkit.client.nacos._settings import SandboxWorkspaceConfig
    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.storage.project._files import ProjectFileStorage

    async def gateway(workspace):
        workspace._gateway = SimpleNamespace(aclose=AsyncMock())

    monkeypatch.setattr(SandboxedWorkspaceBase, "_setup_mcp_gateway", gateway)
    workspace = EgisSandboxWorkspace(
        user_id="alice", agent_id="agent", project_id="project", session_id="session",
        files=ProjectFileStorage(tmp_path), client=TestSandboxClient(),
        settings=SandboxWorkspaceConfig(workdir=str(tmp_path / "runtime")),
    )
    try:
        await workspace.initialize()
        for mount in workspace.build_mounts():
            if mount.name.startswith("user-"):
                assert Path(mount.source).is_dir()
                assert Path(mount.source).is_relative_to(tmp_path / "user/alice")
        assert not (tmp_path / "user/alice/projects/project").exists()
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
