import pytest

from egis_agentkit.errors import Forbidden
from egis_agentkit.service.project._file_manager import ProjectFileManager
from egis_agentkit.storage.project._files import ProjectFileStorage


def test_project_layout_keeps_uploads_flat_and_reserved_areas_separate(tmp_path):
    files = ProjectFileStorage(tmp_path)
    root = tmp_path / "user/alice/projects/project"
    assert files.project_root("alice", "project") == root
    upload = files.write("alice", "project", "input.pdf", b"input", area="uploads")
    work = files.write("alice", "project", "scratch.py", b"script")
    artifact = files.write(
        "alice", "project", "report.pdf", b"report", area="artifacts"
    )
    assert upload == root / "input.pdf"
    assert work == root / ".work/scratch.py"
    assert artifact == root / "artifacts/report.pdf"
    assert [item["path"] for item in files.list("alice", "project", "uploads")] == [
        "input.pdf"
    ]
    assert [
        item["path"]
        for item in files.list("alice", "project", "uploads", recursive=True)
    ] == ["input.pdf"]
    assert set(ProjectFileManager(None, tmp_path).scan("alice", "project")) == {
        ("uploads", "input.pdf"),
        ("work", "scratch.py"),
        ("artifacts", "report.pdf"),
    }


@pytest.mark.parametrize(
    "path", [".work", ".work/secret.txt", "artifacts", "artifacts/report.txt", ".WORK/secret.txt", "Artifacts/report.txt"]
)
def test_upload_area_rejects_reserved_paths_for_all_access(tmp_path, path):
    files = ProjectFileStorage(tmp_path)
    files.write("alice", "project", "secret.txt", b"work")
    files.write("alice", "project", "report.txt", b"artifact", area="artifacts")
    operations = [
        lambda: files.write("alice", "project", path, b"overwrite", area="uploads"),
        lambda: files.read("alice", "project", "uploads", path),
        lambda: files.resolve("alice", "project", "uploads", path),
        lambda: files.list("alice", "project", "uploads", path),
        lambda: files.list("alice", "project", "uploads", path, recursive=True),
    ]
    for operation in operations:
        with pytest.raises(Forbidden):
            operation()
    assert files.read("alice", "project", "work", "secret.txt")[0] == b"work"
    assert files.read("alice", "project", "artifacts", "report.txt")[0] == b"artifact"


def test_project_removal_retains_other_projects_and_does_not_follow_links(tmp_path):
    files = ProjectFileStorage(tmp_path)
    files.write("alice", "first", "input.txt", b"input", area="uploads")
    files.write("alice", "second", "keep.txt", b"keep")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"secret")
    (files.project_root("alice", "first") / "link").symlink_to(outside)
    files.remove_project("alice", "first")
    assert not files.project_root("alice", "first").exists()
    assert files.read("alice", "second", "work", "keep.txt")[0] == b"keep"
    assert (outside / "secret").read_bytes() == b"secret"
    files.discover_deliveries("alice", "first")
    assert files.list("alice", "first", "uploads") == []
    assert not files.project_root("alice", "first").exists()


def test_delivery_discovery_excludes_project_inputs_but_keeps_work_outputs(tmp_path):
    files = ProjectFileStorage(tmp_path)
    files.write("alice", "project", "input.md", b"input", area="uploads")
    files.write("alice", "project", "copy.md", b"input")
    files.write("alice", "project", "report.md", b"report")
    files.write("alice", "project", "script.py", b"print('hello')")
    files.discover_deliveries("alice", "project")
    assert [item["path"] for item in files.list("alice", "project", "artifacts")] == [
        "report.md"
    ]


@pytest.mark.asyncio
async def test_native_workspace_uses_project_scope_for_shared_sessions(tmp_path):
    from types import SimpleNamespace

    from egis_agentkit.extension._session_lock_override import execution_scope
    from egis_agentkit.service.workspace._local_manager import (
        LocalWorkspaceManager,
    )

    manager = LocalWorkspaceManager(tmp_path)
    manager.files.initialize_project("alice", "project")
    scope = execution_scope.set(
        SimpleNamespace(user_id="alice", project_id="project", session_id="first")
    )
    try:
        first = await manager.get_workspace(
            "alice", "agent", "first", project_id="project"
        )
        execution_scope.set(
            SimpleNamespace(user_id="alice", project_id="project", session_id="second")
        )
        second = await manager.get_workspace(
            "alice", "agent", "second", project_id="project"
        )
        assert first is not second
        assert first.workspace_id == second.workspace_id
        assert first.current_project_id() == "project"
        bash = next(tool for tool in await second.list_tools() if tool.name == "Bash")
        files_root = manager.files.project_root("alice", "project") / ".work"
        assert files_root.is_dir()
        chunks = [chunk async for chunk in bash.call("pwd")]
        assert str(files_root) in str(chunks)
        instructions = await second.get_instructions()
        assert str(manager.files.user_root("alice")) in instructions
    finally:
        execution_scope.reset(scope)
        await manager.close_all()


@pytest.mark.asyncio
async def test_project_history_records_each_physical_file_once(storage, tmp_path):
    from sqlalchemy import insert

    from egis_agentkit.storage import tables as t

    async with storage.engine.begin() as conn:
        await conn.execute(
            insert(t.projects).values(
                id="project",
                owner_id="alice",
                project_name="Shared",
                created_by="alice",
                updated_by="alice",
            )
        )
    first = ProjectFileManager(storage, tmp_path)
    second = ProjectFileManager(storage, tmp_path)
    await first.observe("alice", "project")
    for area in ("uploads", "work", "artifacts"):
        first.files.write("alice", "project", "file.txt", area.encode(), area=area)
    await second.observe("alice", "project")
    changes = (await first.list_changes("project"))["items"]
    assert {(item["area"], item["path"], item["change_type"]) for item in changes} == {
        (area, "file.txt", "created") for area in ("uploads", "work", "artifacts")
    }
    assert len(changes) == 3
    await first.observe("alice", "project")
    assert (await second.list_changes("project"))["items"] == changes


@pytest.mark.asyncio
async def test_late_observation_skips_deleted_and_deleting_projects(
    storage, tmp_path, monkeypatch
):
    from sqlalchemy import insert, select

    from egis_agentkit.errors import Conflict
    from egis_agentkit.storage import tables as t

    service = ProjectFileManager(storage, tmp_path)
    async with storage.engine.begin() as conn:
        await conn.execute(
            insert(t.projects).values(
                id="deleting",
                owner_id="alice",
                project_name="Deleting",
                status_type="deleting",
                created_by="alice",
                updated_by="alice",
            )
        )
    service.files.write("alice", "deleting", "keep.txt", b"existing")
    await service.observe_safe("alice", "deleted", discover=True)
    await service.observe_safe("alice", "deleting", discover=True)

    def fail(*args, **kwargs):
        raise Conflict("interrupted scan")

    monkeypatch.setattr(service, "observe", fail)
    await service.observe_safe("alice", "deleted", discover=True)
    await service.observe_safe("alice", "deleting", discover=True)
    assert not service.files.project_root("alice", "deleted").exists()
    assert service.files.read("alice", "deleting", "work", "keep.txt")[0] == b"existing"
    async with storage.engine.connect() as conn:
        for table in (
            t.file_fingerprints,
            t.file_changes,
        ):
            assert not (await conn.execute(select(table))).all()
