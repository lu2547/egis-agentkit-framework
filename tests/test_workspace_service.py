"""Workspace use cases run against real files, sessions and disposable PG locks."""

import asyncio
from importlib import import_module, util
from types import SimpleNamespace

import pytest
from test_domain import expert, identities

from egis_agentkit.errors import AgentKitError, Conflict, Forbidden, NotFound
from egis_agentkit.service.agent.definition import AgentService
from egis_agentkit.service.project.service import ProjectService
from egis_agentkit.service.session.service import TaskService
from egis_agentkit.service.workspace.changes import FileChangeService
from egis_agentkit.service.workspace.files import WorkspaceFiles
from egis_agentkit.service.workspace.preview import DocumentPreviewService


def make_workspace(storage, root, *, max_bytes=1024):
    module = "egis_agentkit.service.workspace.service"
    assert util.find_spec(module) is not None, "Workspace use cases belong in Framework"
    files = WorkspaceFiles(root, max_bytes=max_bytes)
    projects = ProjectService(storage, root)
    tasks = TaskService(storage, root)
    changes = FileChangeService(storage, root)
    preview = DocumentPreviewService()
    workspace = import_module(module).WorkspaceService(files, projects, tasks, changes, preview)
    return SimpleNamespace(
        workspace=workspace, files=files, projects=projects, tasks=tasks,
        changes=changes, preview=preview,
    )


@pytest.mark.asyncio
async def test_user_upload_normalizes_path_and_preserves_file_dto(tmp_path):
    svc = make_workspace(None, tmp_path, max_bytes=8)
    user = SimpleNamespace(user_id="alice")
    row = await svc.workspace.upload_user_file(user, "../报告.txt", b"report", "uploads/reports")
    assert row["path"] == "uploads/reports/报告.txt"
    assert row["name"] == "报告.txt"
    assert row["size"] == 6 and row["is_directory"] is False
    assert isinstance(row["modified_at"], float)
    assert svc.workspace.max_upload_bytes == 8
    assert await svc.workspace.read_user_file(user, row["path"]) == (b"report", "text/plain")
    listing = await svc.workspace.list_user_files(user, "uploads/reports")
    assert listing["path"] == "uploads/reports"
    assert listing["items"][0]["path"] == row["path"]
    duplicate = await svc.workspace.upload_user_file(user, "报告.txt", b"new", "reports")
    assert duplicate["path"] != row["path"]
    with pytest.raises(Conflict):
        await svc.workspace.upload_user_file(user, "large.txt", b"123456789")


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ["work", "artifacts/sub", "../escape", "/absolute", "uploads/../work", "bad\\path"])
async def test_user_upload_rejects_unsafe_destinations(tmp_path, path):
    svc = make_workspace(None, tmp_path)
    with pytest.raises(Forbidden):
        await svc.workspace.upload_user_file(SimpleNamespace(user_id="alice"), "input.txt", b"data", path)
    assert not svc.files.user_root("alice").exists()


@pytest.mark.asyncio
async def test_project_and_session_share_files_and_durable_changes(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentService(storage).save(alice, expert())
    task = await svc.tasks.create(alice, definition["id"], "private")
    sid, pid = task["session_id"], task["project_id"]
    uploaded = await svc.workspace.upload_session_file(alice, sid, "input.txt", b"input", area="work", path="folder")
    assert uploaded["path"] == "folder/input.txt"
    assert uploaded["area"] == "uploads" and uploaded["size"] == 5
    assert isinstance(uploaded["modified_at"], float)
    assert await svc.workspace.read_project_file(alice, pid, "uploads", uploaded["path"]) == (b"input", "text/plain")
    assert await svc.workspace.read_session_file(alice, sid, "uploads", uploaded["path"]) == (b"input", "text/plain")
    listing = await svc.workspace.list_project_files(alice, pid, recursive=True)
    assert listing == await svc.workspace.list_session_files(alice, sid, recursive=True)
    assert listing["items"][0]["area"] == "uploads"
    assert listing["items"][0]["path"] == "folder/input.txt"
    changes = await svc.workspace.project_file_changes(alice, pid)
    assert changes == await svc.workspace.session_file_changes(alice, sid)
    assert [(r["area"], r["path"], r["change_type"]) for r in changes["items"]] == [("uploads", "folder/input.txt", "created")]
    svc.files.write(alice.user_id, pid, "result.txt", b"result")
    delivered = await svc.workspace.deliver_session_file(alice, sid, "result.txt")
    assert delivered == {"path": "result.txt", "area": "artifacts"}
    assert (await svc.workspace.read_project_file(alice, pid, "artifacts", "result.txt"))[0] == b"result"
    changes = await svc.workspace.project_file_changes(alice, pid)
    assert ("artifacts", "result.txt", "created") in [(r["area"], r["path"], r["change_type"]) for r in changes["items"]]
    svc.files.write(alice.user_id, pid, "automatic.md", b"discover me")
    artifacts = await svc.workspace.list_session_files(alice, sid, "artifacts")
    assert {r["path"] for r in artifacts["items"]} == {"result.txt", "automatic.md"}


@pytest.mark.asyncio
async def test_workspace_keeps_existing_ownership_checks_without_requiring_use(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentService(storage).save(alice, expert())
    task = await svc.tasks.create(alice, definition["id"], "private")
    pid, sid = task["project_id"], task["session_id"]
    svc.tasks.config = SimpleNamespace(snapshot=SimpleNamespace(policies=SimpleNamespace(allow_private_self_use=False)))
    assert (await svc.workspace.list_session_files(alice, sid))["items"] == []
    for operation in (
        svc.workspace.list_project_files(bob, pid),
        svc.workspace.read_project_file(bob, pid, "uploads", "input.txt"),
        svc.workspace.upload_project_file(bob, pid, "input.txt", b"data"),
        svc.workspace.deliver_project_file(bob, pid, "input.txt"),
        svc.workspace.project_file_changes(bob, pid),
        svc.workspace.preview_project_document(bob, pid, "uploads", "input.docx"),
    ):
        with pytest.raises(NotFound):
            await operation
    for operation in (
        svc.workspace.list_session_files(bob, sid),
        svc.workspace.read_session_file(bob, sid, "uploads", "input.txt"),
        svc.workspace.upload_session_file(bob, sid, "input.txt", b"data"),
        svc.workspace.deliver_session_file(bob, sid, "input.txt"),
        svc.workspace.session_file_changes(bob, sid),
        svc.workspace.preview_session_document(bob, sid, "uploads", "input.docx"),
    ):
        with pytest.raises(Forbidden):
            await operation
    with pytest.raises(Forbidden):
        await svc.workspace.upload_project_file(alice, pid, "input.txt", b"data", area="artifacts")
    with pytest.raises(Forbidden):
        await svc.workspace.upload_project_file(alice, pid, "input.txt", b"data", path=".work")
    with pytest.raises(Conflict):
        await svc.workspace.upload_project_file(alice, pid, "input.txt", b"x" * 1025)


@pytest.mark.asyncio
async def test_upload_holds_project_guard_while_waiting_for_file_lock(storage, tmp_path):
    from agentscope.app.message_bus import InMemoryMessageBus

    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    pid = (await svc.projects.create(alice, "files"))["id"]
    async with InMemoryMessageBus() as bus:
        async with svc.changes.locked([pid]):
            pending = asyncio.create_task(svc.workspace.upload_project_file(alice, pid, "input.txt", b"input"))
            try:
                # Wait until the real upload holds a shared project lock and waits
                # for the file lock, using PG's lock state rather than timing sleeps.
                from sqlalchemy import text
                async with asyncio.timeout(3):
                    while True:
                        async with storage.engine.connect() as conn:
                            waiting = await conn.scalar(text("SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted)"))
                        if waiting:
                            break
                assert not pending.done()
                with pytest.raises(Conflict):
                    await svc.projects.delete(alice, pid, bus)
            except BaseException:
                pending.cancel()
                await asyncio.gather(pending, return_exceptions=True)
                raise
        assert (await pending)["path"] == "input.txt"
        await svc.projects.delete(alice, pid, bus)
        with pytest.raises(NotFound):
            await svc.workspace.upload_project_file(alice, pid, "late.txt", b"late")
        assert not svc.files.project_root(alice.user_id, pid).exists()


@pytest.mark.asyncio
async def test_preview_reads_authorized_bytes_and_propagates_domain_errors(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentService(storage).save(alice, expert())
    task = await svc.tasks.create(alice, definition["id"], "private")
    pid, sid = task["project_id"], task["session_id"]
    await svc.workspace.upload_user_file(alice, "input.txt", b"input")
    await svc.workspace.upload_project_file(alice, pid, "input.txt", b"input")
    for operation in (
        svc.workspace.preview_user_document(alice, "uploads/input.txt"),
        svc.workspace.preview_project_document(alice, pid, "uploads", "input.txt"),
        svc.workspace.preview_session_document(alice, sid, "uploads", "input.txt"),
    ):
        with pytest.raises(AgentKitError) as error:
            await operation
        assert error.value.status_code == 415
