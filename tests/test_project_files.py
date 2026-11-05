import os

import pytest

from egis_agentkit.errors import Forbidden
from egis_agentkit.services.file_changes import FileChangeService
from egis_agentkit.workspace.files import WorkspaceFiles


def test_project_layout_keeps_uploads_flat_and_reserved_areas_separate(tmp_path):
    files = WorkspaceFiles(tmp_path)
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
    assert set(FileChangeService(None, tmp_path).scan("alice", "project")) == {
        ("uploads", "input.pdf"),
        ("work", "scratch.py"),
        ("artifacts", "report.pdf"),
    }


@pytest.mark.parametrize(
    "path", [".work", ".work/secret.txt", "artifacts", "artifacts/report.txt", ".WORK/secret.txt", "Artifacts/report.txt"]
)
def test_upload_area_rejects_reserved_paths_for_all_access(tmp_path, path):
    files = WorkspaceFiles(tmp_path)
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
    files = WorkspaceFiles(tmp_path)
    files.write("alice", "first", "input.txt", b"input", area="uploads")
    files.write("alice", "second", "keep.txt", b"keep")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"secret")
    (files.project_root("alice", "first") / "link").symlink_to(outside)
    files.remove_session("alice", "first")
    assert files.project_root("alice", "first").exists()
    files.remove_project("alice", "first")
    assert not files.project_root("alice", "first").exists()
    assert files.read("alice", "second", "work", "keep.txt")[0] == b"keep"
    assert (outside / "secret").read_bytes() == b"secret"
    files.discover_deliveries("alice", "first")
    assert files.list("alice", "first", "uploads") == []
    assert not files.project_root("alice", "first").exists()


def test_legacy_migration_merges_collisions_and_never_follows_symlinks(tmp_path):
    files = WorkspaceFiles(tmp_path)
    legacy = files.user_root("alice")
    for area in ("uploads", "work", "artifacts"):
        old = legacy / area / "session"
        old.mkdir(parents=True)
        (old / "same.txt").write_bytes(area.encode())
        files.write("alice", "project", "same.txt", b"existing", area=area)
    (legacy / "uploads/archive.txt").write_bytes(b"archive")
    reserved = legacy / "uploads/session/.work"
    reserved.mkdir()
    (reserved / "input.txt").write_bytes(b"reserved input")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"secret")
    (legacy / "work/session/link").symlink_to(outside)
    # Migration is independent of preview/download limits.
    files.max_bytes = 2
    mapping = files.migrate_legacy_session("alice", "session", "project")
    for area in ("uploads", "work", "artifacts"):
        assert mapping[(area, "same.txt")] == (area, "same-legacy-session-1.txt")
        assert (
            files.area_root("alice", "project", area) / "same.txt"
        ).read_bytes() == b"existing"
        assert (
            files.area_root("alice", "project", area) / "same-legacy-session-1.txt"
        ).read_bytes() == area.encode()
        assert not (legacy / area / "session").exists()
    assert (
        mapping[("uploads", ".work/input.txt")][1] == ".work-legacy-session-1/input.txt"
    )
    assert not (files.project_root("alice", "project") / ".work/link").exists()
    assert (outside / "secret").read_bytes() == b"secret"
    assert (legacy / "uploads/archive.txt").read_bytes() == b"archive"
    assert files.migrate_legacy_session("alice", "session", "project") == mapping


def test_delivery_discovery_excludes_project_inputs_but_keeps_work_outputs(tmp_path):
    files = WorkspaceFiles(tmp_path)
    files.write("alice", "project", "input.md", b"input", area="uploads")
    files.write("alice", "project", "copy.md", b"input")
    files.write("alice", "project", "report.md", b"report")
    files.write("alice", "project", "script.py", b"print('hello')")
    files.discover_deliveries("alice", "project")
    assert [item["path"] for item in files.list("alice", "project", "artifacts")] == [
        "report.md"
    ]


@pytest.mark.parametrize("interrupted_step", ["journal", "unlink"])
def test_legacy_migration_retries_without_duplicates_after_interruption(
    tmp_path, monkeypatch, interrupted_step
):
    files = WorkspaceFiles(tmp_path)
    legacy = files.user_root("alice") / "uploads/session"
    legacy.mkdir(parents=True)
    (legacy / "input.txt").write_bytes(b"input")
    files.write("alice", "project", "input.txt", b"existing", area="uploads")
    operation = "fsync" if interrupted_step == "journal" else "unlink"
    original = getattr(os, operation)

    def interrupt(*args, **kwargs):
        # The target directory is durable before its journal entry is opened.
        if (
            operation == "fsync"
            and files.area_root("alice", "project", "work")
            .joinpath(".legacy-migrations/session.jsonl")
            .exists()
        ) or (operation == "unlink" and args[0] == "input.txt"):
            raise RuntimeError("interrupted")
        return original(*args, **kwargs)

    with monkeypatch.context() as patch:
        patch.setattr(os, operation, interrupt)
        with pytest.raises(RuntimeError, match="interrupted"):
            files.migrate_legacy_session("alice", "session", "project")
    assert (legacy / "input.txt").read_bytes() == b"input"
    mapping = files.migrate_legacy_session("alice", "session", "project")
    assert mapping == {
        ("uploads", "input.txt"): ("uploads", "input-legacy-session-1.txt")
    }
    assert [item["path"] for item in files.list("alice", "project", "uploads")] == [
        "input-legacy-session-1.txt",
        "input.txt",
    ]
    assert files.migrate_legacy_session("alice", "session", "project") == mapping


def test_legacy_migration_does_not_follow_source_or_destination_links(tmp_path):
    files = WorkspaceFiles(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret").write_bytes(b"secret")
    legacy = files.user_root("alice") / "uploads"
    legacy.mkdir(parents=True)
    (legacy / "linked-session").symlink_to(outside)
    assert files.migrate_legacy_session("alice", "linked-session", "project") == {}
    assert not files.project_root("alice", "project").exists()
    source = legacy / "session"
    source.mkdir()
    (source / "folder").mkdir()
    (source / "folder/input.txt").write_bytes(b"input")
    project = files.project_root("alice", "project")
    project.mkdir(parents=True)
    (project / "folder").symlink_to(outside)
    mapping = files.migrate_legacy_session("alice", "session", "project")
    assert mapping[("uploads", "folder/input.txt")] == (
        "uploads",
        "folder-legacy-session-1/input.txt",
    )
    assert (outside / "secret").read_bytes() == b"secret"
    assert list(outside.iterdir()) == [outside / "secret"]


def test_legacy_migration_recovers_partial_journal_and_long_name_collisions(tmp_path):
    files = WorkspaceFiles(tmp_path)
    legacy = files.user_root("alice") / "uploads/session"
    legacy.mkdir(parents=True)
    filename = "x" * 247 + ".txt"
    (legacy / filename).write_bytes(b"input")
    files.write("alice", "project", filename, b"existing", area="uploads")
    journal = files.area_root("alice", "project", "work") / ".legacy-migrations/session.jsonl"
    journal.parent.mkdir(parents=True)
    journal.write_bytes(b'["uploads", "unfinished')
    mapping = files.migrate_legacy_session("alice", "session", "project")
    renamed = mapping[("uploads", filename)][1]
    assert len(renamed.encode()) <= 255
    assert renamed.endswith("-legacy-session-1.txt")
    assert files.read("alice", "project", "uploads", renamed)[0] == b"input"
    assert files.read("alice", "project", "uploads", filename)[0] == b"existing"
    assert files.migrate_legacy_session("alice", "session", "project") == mapping


@pytest.mark.asyncio
async def test_native_workspace_uses_project_scope_for_shared_sessions(tmp_path):
    from types import SimpleNamespace

    from egis_agentkit.services.assembly import execution_scope
    from egis_agentkit.workspace.manager import EgisLocalWorkspaceManager

    manager = EgisLocalWorkspaceManager(tmp_path)
    scope = execution_scope.set(
        SimpleNamespace(user_id="alice", project_id="project", session_id="first")
    )
    try:
        first = await manager.get_workspace(
            "alice", "agent", "first", workspace_id="project"
        )
        execution_scope.set(
            SimpleNamespace(user_id="alice", project_id="project", session_id="second")
        )
        second = await manager.get_workspace(
            "alice", "agent", "second", workspace_id="project"
        )
        assert first is second
        assert first.current_project_id() == "project"
        bash = next(tool for tool in await second.list_tools() if tool.name == "Bash")
        files_root = manager.files.project_root("alice", "project") / ".work"
        assert files_root.is_dir()
        chunks = [chunk async for chunk in bash.call("pwd")]
        assert str(files_root) in str(chunks)
        instructions = await second.get_instructions()
        assert str(manager.files.project_root("alice", "project")) in instructions
        assert str(files_root) in instructions
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
    first = FileChangeService(storage, tmp_path)
    second = FileChangeService(storage, tmp_path)
    await first.observe("alice", "project")
    for area in ("uploads", "work", "artifacts"):
        first.files.write("alice", "project", "file.txt", area.encode(), area=area)
    await second.observe("alice", "project")
    changes = (await first.list("project"))["items"]
    assert {(item["area"], item["path"], item["change_type"]) for item in changes} == {
        (area, "file.txt", "created") for area in ("uploads", "work", "artifacts")
    }
    assert len(changes) == 3
    await first.observe("alice", "project")
    assert (await second.list("project"))["items"] == changes


@pytest.mark.asyncio
async def test_late_observation_skips_deleted_and_deleting_projects(
    storage, tmp_path, monkeypatch
):
    from sqlalchemy import insert, select

    from egis_agentkit.errors import Conflict
    from egis_agentkit.storage import tables as t

    service = FileChangeService(storage, tmp_path)
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
            t.file_baselines,
            t.file_fingerprints,
            t.file_changes,
            t.file_scan_errors,
        ):
            assert not (await conn.execute(select(table))).all()
