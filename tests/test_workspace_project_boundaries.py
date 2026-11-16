"""验证 Workspace/Project 的公开能力入口及运行身份边界。"""

import importlib

import pytest


@pytest.mark.parametrize(
    ("module", "symbol"),
    [
        ("egis_agentkit.service.project._project_manager", "ProjectManager"),
        ("egis_agentkit.service.project._file_manager", "ProjectFileManager"),
        ("egis_agentkit.service.project._document_preview", "DocumentPreviewService"),
        ("egis_agentkit.storage.project._file_records", "ProjectFileRecordStorage"),
        ("egis_agentkit.storage.project._files", "ProjectFileStorage"),
    ],
)
def test_project_capabilities_have_explicit_public_owners(module, symbol):
    assert callable(getattr(importlib.import_module(module), symbol))


@pytest.mark.asyncio
async def test_project_management_uses_core_project_rules(
    storage, tmp_path, monkeypatch
):
    from test_domain import identities

    from egis_agentkit.core.project._project import Project
    from egis_agentkit.service.project._project_manager import ProjectManager

    _, _, alice, _ = await identities(storage)
    monkeypatch.setattr(
        Project, "normalize_name", lambda self, name: "Core: " + name.strip()
    )
    created = await ProjectManager(storage, tmp_path).create(alice, "  Project  ")
    assert created["project_name"] == "Core: Project"


@pytest.mark.asyncio
async def test_same_user_two_projects_keep_one_workspace_identity(storage, tmp_path):
    from test_domain import expert, identities

    from egis_agentkit.core.workspace._egis_sandbox_workspace import workspace_id_for
    from egis_agentkit.service.agent._agent_manager import AgentManager
    from egis_agentkit.service.project._project_manager import ProjectManager
    from egis_agentkit.service.session._session import SessionService

    _, _, alice, bob = await identities(storage)
    sessions = SessionService(storage, tmp_path)
    projects = ProjectManager(storage, tmp_path)
    first = await projects.create(alice, "First")
    second = await projects.create(alice, "Second")
    agent = await AgentManager(storage).add_definition(alice, expert())
    one = await sessions.add_session(
        alice, agent["id"], "private", project_id=first["id"]
    )
    two = await sessions.add_session(
        alice, agent["id"], "private", project_id=second["id"]
    )
    a = await sessions.get_session_record(alice, one["session_id"])
    b = await sessions.get_session_record(alice, two["session_id"])
    assert (
        a.config.workspace_id
        == b.config.workspace_id
        == workspace_id_for(alice.user_id)
    )
    assert a.config.workspace_id != workspace_id_for(bob.user_id)
    assert a.config.workspace_id not in (first["id"], second["id"])
    assert a.config.cwd != b.config.cwd
    assert a.state.middle_context["egis_agentkit"]["project_id"] == first["id"]
    assert b.state.middle_context["egis_agentkit"]["project_id"] == second["id"]


def test_sandbox_management_and_execution_have_distinct_owners():
    from agentscope.app.workspace_manager import WorkspaceManagerBase
    from agentscope.workspace._sandboxed_base import SandboxedWorkspaceBase

    from egis_agentkit.core.workspace._egis_sandbox_workspace import (
        EgisSandboxWorkspace,
    )
    from egis_agentkit.service.workspace._egis_sandbox_manager import EgisSandboxManager

    assert issubclass(EgisSandboxManager, WorkspaceManagerBase)
    assert issubclass(EgisSandboxWorkspace, SandboxedWorkspaceBase)


def test_generic_runtime_callback_has_no_project_or_storage_dependencies():
    import ast
    from pathlib import Path

    from egis_agentkit.middleware.session import _session_callback_middleware_impl

    source = Path(_session_callback_middleware_impl.__file__).read_text()
    imports = [
        node.module
        for node in ast.walk(ast.parse(source))
        if isinstance(node, ast.ImportFrom)
    ]
    assert imports == ["agentscope.middleware"]
    extension = Path(_session_callback_middleware_impl.__file__).parent.parent
    assert not (extension / "_workspace_prompt_middleware_impl.py").exists()
    assert not (extension / "_workspace_file_changes_middleware_impl.py").exists()
    assert not (extension / "_session_middlewares_callback.py").exists()


@pytest.mark.asyncio
async def test_project_context_is_built_by_core_from_managed_inputs(
    tmp_path, monkeypatch
):
    from types import SimpleNamespace

    from egis_agentkit.core.project._project import Project
    from egis_agentkit.service.project._file_manager import ProjectFileManager
    from egis_agentkit.service.project._project_manager import ProjectManager

    scope = SimpleNamespace(
        workdir=tmp_path / ".work", artifacts_dir=tmp_path / "artifacts"
    )
    files = ProjectFileManager(None, tmp_path)
    monkeypatch.setattr(
        files, "context_inputs", lambda workdir: [str(tmp_path / "input.pdf")]
    )
    seen = []

    def context(workdir, artifacts, inputs, current_prompt):
        seen.append((workdir, artifacts, inputs))
        return current_prompt + "\nCore project context"

    monkeypatch.setattr(Project, "build_context", staticmethod(context))
    result = await ProjectManager(None, tmp_path).on_system_prompt(
        scope, files, None, "system"
    )
    assert result == "system\nCore project context"
    assert seen == [(scope.workdir, scope.artifacts_dir, [str(tmp_path / "input.pdf")])]


@pytest.mark.asyncio
async def test_project_deletion_releases_runtime_mounts_before_removing_files(
    storage, tmp_path
):
    from agentscope.app.message_bus import InMemoryMessageBus
    from test_domain import identities

    from egis_agentkit.service.project._project_manager import ProjectManager
    from egis_agentkit.storage.project._files import ProjectFileStorage

    _, _, alice, _ = await identities(storage)
    released = []

    class Workspaces:
        async def destroy_workspaces(self, user_id, project_id):
            released.append((user_id, project_id))

    files = ProjectFileStorage(tmp_path)
    manager = ProjectManager(storage, tmp_path, files=files, workspaces=Workspaces())
    project = await manager.create(alice, "mounted project")
    original = files.remove_project

    def remove(user_id, project_id):
        assert released == [(user_id, project_id)]
        original(user_id, project_id)

    files.remove_project = remove
    await manager.delete(alice, project["id"], InMemoryMessageBus())
    assert not files.project_root(alice.user_id, project["id"]).exists()


def test_project_file_path_limits_follow_current_configuration(tmp_path):
    from types import SimpleNamespace

    from egis_agentkit.client.nacos._settings import (
        ProjectScanConfig,
        ProjectUploadConfig,
    )
    from egis_agentkit.errors import Forbidden
    from egis_agentkit.storage.project._files import ProjectFileStorage

    project = SimpleNamespace(
        scan=ProjectScanConfig(max_depth=1, max_path_bytes=8),
        upload=ProjectUploadConfig(max_bytes=1024),
    )
    config = SimpleNamespace(snapshot=SimpleNamespace(project=project))
    files = ProjectFileStorage(tmp_path, config_provider=config)
    for path in ("long-path.md", "p/a", "é" * 5):
        with pytest.raises(Forbidden):
            files.write("alice", "project", path, b"content")
    project.scan = ProjectScanConfig(max_depth=2, max_path_bytes=20)
    assert files.write("alice", "project", "p/a", b"content").read_bytes() == b"content"
