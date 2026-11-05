from pathlib import Path

import pytest

from egis_agentkit.errors import Forbidden
from egis_agentkit.workspace.files import WorkspaceFiles


def test_preview_rejects_cross_user_and_symlink_escape(tmp_path):
    files = WorkspaceFiles(tmp_path)
    root = files.area_root("alice", "s1", "work")
    root.mkdir(parents=True)
    (root / "notes.md").write_text("hello")
    (root / "outside").symlink_to(tmp_path.parent, target_is_directory=True)
    assert files.resolve("alice", "s1", "work", "notes.md").read_text() == "hello"
    for value in ("../s2/a", "/etc/passwd", "outside/x", "../../../../user/bob"):
        with pytest.raises(Forbidden):
            files.resolve("alice", "s1", "work", value)


def test_artifact_commit_preserves_source_and_does_not_overwrite(tmp_path):
    files = WorkspaceFiles(tmp_path)
    root = files.area_root("alice", "s1", "work")
    root.mkdir(parents=True)
    (root / "report.md").write_text("first")
    committed = files.deliver("alice", "s1", "report.md")
    assert Path(committed).read_text() == "first"
    (root / "report.md").write_text("second")
    later = files.deliver("alice", "s1", "report.md")
    assert later != committed
    assert Path(committed).read_text() == "first"


def test_project_uploads_are_visible_and_do_not_become_deliveries(tmp_path):
    files = WorkspaceFiles(tmp_path)
    files.write("alice", "s1", "input.pdf", b"input", area="uploads")
    files.write("alice", "s2", "private.pdf", b"private", area="uploads")
    files.write("bob", "s1", "other.pdf", b"other", area="uploads")
    files.write("alice", "s1", "report.md", b"finished")
    files.write("alice", "s1", ".cache/draft.md", b"draft")
    files.write("alice", "s1", "scratch.tmp", b"work")
    assert [r["path"] for r in files.list("alice", "s1", "uploads")] == ["input.pdf"]
    files.discover_deliveries("alice", "s1")
    assert [r["path"] for r in files.list("alice", "s1", "artifacts")] == ["report.md"]
    assert files.read("alice", "s1", "artifacts", "report.md")[0] == b"finished"
    files.discover_deliveries("alice", "s1")
    assert len(files.list("alice", "s1", "artifacts")) == 1
    files.write("alice", "s2", "elsewhere.md", b"other session")
    files.discover_deliveries("alice", "s1")
    assert len(files.list("alice", "s1", "artifacts")) == 1


def test_legacy_upload_copy_is_not_discovered_and_versions_are_preserved(tmp_path):
    files = WorkspaceFiles(tmp_path)
    files.write_user("alice", "uploads/input.txt", b"original upload")
    files.write("alice", "s1", "input.txt", b"original upload")
    files.write("alice", "s1", "report.md", b"first")
    files.discover_deliveries("alice", "s1")
    files.area_root("alice", "s1", "work").joinpath("report.md").write_bytes(b"updated")
    files.discover_deliveries("alice", "s1")
    artifacts = files.list("alice", "s1", "artifacts")
    assert len(artifacts) == 2
    assert {
        files.read("alice", "s1", "artifacts", r["path"])[0] for r in artifacts
    } == {b"first", b"updated"}
    files.discover_deliveries("alice", "s1")
    assert len(files.list("alice", "s1", "artifacts")) == 2


def test_remove_project_unlinks_symlinks_without_touching_other_data(tmp_path):
    files = WorkspaceFiles(tmp_path)
    for area in ("work", "artifacts", "uploads"):
        files.write("alice", "s1", "remove.txt", b"remove", area=area)
        files.write("alice", "s2", "keep.txt", b"keep", area=area)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "secret.txt").write_bytes(b"secret")
    (files.area_root("alice", "s1", "work") / "link").symlink_to(outside)
    files.remove_project("alice", "s1")
    assert all(
        not files.area_root("alice", "s1", a).exists()
        for a in ("work", "artifacts", "uploads")
    )
    assert all(
        files.read("alice", "s2", a, "keep.txt")[0] == b"keep"
        for a in ("work", "artifacts", "uploads")
    )
    assert (outside / "secret.txt").read_bytes() == b"secret"


def test_discovery_preserves_newer_artifact_and_requires_explicit_code_delivery(
    tmp_path,
):
    import os

    files = WorkspaceFiles(tmp_path)
    work = files.write("alice", "s1", "report.md", b"old work")
    artifact = files.write(
        "alice", "s1", "report.md", b"new explicit delivery", area="artifacts"
    )
    os.utime(work, ns=(1_000_000_000, 1_000_000_000))
    os.utime(artifact, ns=(2_000_000_000, 2_000_000_000))
    files.write("alice", "s1", "solution.py", b"print('answer')")
    files.discover_deliveries("alice", "s1")
    assert [r["path"] for r in files.list("alice", "s1", "artifacts")] == ["report.md"]
    assert artifact.read_bytes() == b"new explicit delivery"
    files.deliver("alice", "s1", "solution.py")
    assert (
        files.read("alice", "s1", "artifacts", "solution.py")[0] == b"print('answer')"
    )


@pytest.mark.asyncio
async def test_shared_native_workspace_initializes_each_project_directory(tmp_path):
    from egis_agentkit.workspace.manager import EgisLocalWorkspaceManager

    manager = EgisLocalWorkspaceManager(tmp_path)
    try:
        first = await manager.get_workspace("alice", "agent", "s1", workspace_id="p1")
        second = await manager.get_workspace("alice", "agent", "s2", workspace_id="p2")
        assert first is second
        assert all(
            manager.files.area_root("alice", sid, area).is_dir()
            for sid in ("p1", "p2")
            for area in ("work", "artifacts", "uploads")
        )
    finally:
        await manager.close_all()


def test_other_project_upload_does_not_hide_current_delivery(tmp_path):
    files = WorkspaceFiles(tmp_path)
    files.write("alice", "s2", "input.md", b"same bytes", area="uploads")
    files.write("alice", "s1", "report.md", b"same bytes")
    files.discover_deliveries("alice", "s1")
    assert files.read("alice", "s1", "artifacts", "report.md")[0] == b"same bytes"


def test_discovery_prunes_dependency_directories_before_recursing(tmp_path):
    files = WorkspaceFiles(tmp_path)
    files.write("alice", "s1", "report.md", b"finished")
    deep = files.area_root("alice", "s1", "work") / "node_modules"
    for _ in range(35):
        deep /= "nested"
    deep.mkdir(parents=True)
    (deep / "dependency.md").write_bytes(b"dependency")
    files.discover_deliveries("alice", "s1")
    assert [row["path"] for row in files.list("alice", "s1", "artifacts")] == [
        "report.md"
    ]


def test_read_only_listing_does_not_recreate_deleted_project(tmp_path):
    files = WorkspaceFiles(tmp_path)
    for area in ("work", "artifacts", "uploads"):
        files.write("alice", "s1", "old.txt", b"old", area=area)
    files.remove_project("alice", "s1")
    files.discover_deliveries("alice", "s1")
    for area in ("work", "artifacts", "uploads"):
        assert files.list("alice", "s1", area) == []
        assert not files.area_root("alice", "s1", area).exists()
