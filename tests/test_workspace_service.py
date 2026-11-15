"""Workspace use cases run against real files, sessions and disposable PG locks."""

import asyncio
import hashlib
from importlib import import_module, util
from types import SimpleNamespace

import pytest
from test_domain import expert, identities

from egis_agentkit.client.nacos._settings import AccessConfig
from egis_agentkit.errors import AgentKitError, Conflict, Forbidden, NotFound
from egis_agentkit.service._session import SessionService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.project._document_preview import DocumentPreviewService
from egis_agentkit.service.project._project_manager import ProjectManager
from egis_agentkit.storage.project._files import ProjectFileStorage


def make_workspace(storage, root, *, max_bytes=1024):
    module = "egis_agentkit.service.project._file_manager"
    assert util.find_spec(module) is not None, "Workspace use cases belong in Framework"
    files = ProjectFileStorage(root, max_bytes=max_bytes)
    projects = ProjectManager(storage, root)
    tasks = SessionService(storage, root)
    preview = DocumentPreviewService()
    workspace = import_module(module).ProjectFileManager(
        storage, root, files=files, projects=projects, sessions=tasks, preview=preview
    )
    return SimpleNamespace(
        workspace=workspace,
        files=files,
        projects=projects,
        tasks=tasks,
        changes=workspace,
        preview=preview,
    )


@pytest.mark.asyncio
async def test_project_editor_save_rejects_stale_content_and_other_users(
    storage, tmp_path
):
    """编辑保存只能替换所属项目的当前文件，冲突不能丢弃新内容。"""
    _, _, alice, bob = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    project = await svc.projects.create(alice, "编辑测试")
    pid = project["id"]
    await svc.workspace.upload_project_file(alice, pid, "book.xlsx", b"original")
    checksum = hashlib.sha256(b"original").hexdigest()
    with pytest.raises(NotFound):
        await svc.workspace.update_project_file(
            bob, pid, "uploads", "book.xlsx", b"evil", checksum
        )
    saved = await svc.workspace.update_project_file(
        alice, pid, "uploads", "book.xlsx", b"edited", checksum
    )
    assert saved["checksum"] == hashlib.sha256(b"edited").hexdigest()
    assert saved["path"] == "book.xlsx"
    with pytest.raises(Conflict):
        await svc.workspace.update_project_file(
            alice, pid, "uploads", "book.xlsx", b"stale", checksum
        )
    assert (await svc.workspace.read_project_file(alice, pid, "uploads", "book.xlsx"))[
        0
    ] == b"edited"
    with pytest.raises(NotFound):
        await svc.workspace.update_project_file(
            alice, pid, "uploads", "missing.xlsx", b"new", checksum
        )
    changes = await svc.workspace.project_file_changes(alice, pid)
    assert any(row["change_type"] == "modified" for row in changes["items"])


@pytest.mark.asyncio
async def test_project_files_and_durable_changes(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentManager(storage).add_definition(alice, expert())
    task = await svc.tasks.add_session(alice, definition["id"], "private")
    pid = task["project_id"]
    uploaded = await svc.workspace.upload_project_file(
        alice, pid, "input.txt", b"input", path="folder"
    )
    assert uploaded["path"] == "folder/input.txt"
    assert uploaded["area"] == "uploads" and uploaded["size"] == 5
    assert isinstance(uploaded["modified_at"], float)
    assert await svc.workspace.read_project_file(
        alice, pid, "uploads", uploaded["path"]
    ) == (b"input", "text/plain")
    listing = await svc.workspace.list_project_files(alice, pid, recursive=True)
    assert listing["items"][0]["area"] == "uploads"
    assert listing["items"][0]["path"] == "folder/input.txt"
    changes = await svc.workspace.project_file_changes(alice, pid)
    assert [(r["area"], r["path"], r["change_type"]) for r in changes["items"]] == [
        ("uploads", "folder/input.txt", "created")
    ]
    svc.files.write(alice.user_id, pid, "result.txt", b"result")
    delivered = await svc.workspace.deliver_project_file(alice, pid, "result.txt")
    assert delivered == {"path": "result.txt", "area": "artifacts"}
    assert (
        await svc.workspace.read_project_file(alice, pid, "artifacts", "result.txt")
    )[0] == b"result"
    changes = await svc.workspace.project_file_changes(alice, pid)
    assert ("artifacts", "result.txt", "created") in [
        (r["area"], r["path"], r["change_type"]) for r in changes["items"]
    ]
    svc.files.write(alice.user_id, pid, "automatic.md", b"discover me")
    artifacts = await svc.workspace.list_project_files(alice, pid, "artifacts")
    assert {r["path"] for r in artifacts["items"]} == {"result.txt", "automatic.md"}


@pytest.mark.asyncio
async def test_workspace_keeps_existing_ownership_checks_without_requiring_use(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentManager(storage).add_definition(alice, expert())
    task = await svc.tasks.add_session(alice, definition["id"], "private")
    pid, sid = task["project_id"], task["session_id"]
    svc.tasks.config = SimpleNamespace(
        snapshot=SimpleNamespace(access=AccessConfig(allow_private_self_use=False))
    )
    assert (await svc.workspace.session_artifacts(alice, sid))["items"] == []
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
    with pytest.raises(Forbidden):
        await svc.workspace.session_artifacts(bob, sid)
    for area in ("work", "artifacts"):
        with pytest.raises(Forbidden):
            await svc.workspace.upload_project_file(
                alice, pid, "input.txt", b"data", area=area
            )
    with pytest.raises(Forbidden):
        await svc.workspace.upload_project_file(
            alice, pid, "input.txt", b"data", path=".work"
        )
    with pytest.raises(Conflict):
        await svc.workspace.upload_project_file(alice, pid, "input.txt", b"x" * 1025)


@pytest.mark.asyncio
async def test_upload_holds_project_guard_while_waiting_for_file_lock(
    storage, tmp_path
):
    from agentscope.app.message_bus import InMemoryMessageBus

    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    pid = (await svc.projects.create(alice, "files"))["id"]
    async with InMemoryMessageBus() as bus:
        async with svc.changes.locked([pid]):
            pending = asyncio.create_task(
                svc.workspace.upload_project_file(alice, pid, "input.txt", b"input")
            )
            try:
                # Wait until the real upload holds a shared project lock and waits
                # for the file lock, using PG's lock state rather than timing sleeps.
                from sqlalchemy import text

                async with asyncio.timeout(3):
                    while True:
                        async with storage.engine.connect() as conn:
                            waiting = await conn.scalar(
                                text(
                                    "SELECT EXISTS (SELECT 1 FROM pg_locks WHERE locktype='advisory' AND NOT granted)"
                                )
                            )
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
async def test_preview_reads_authorized_bytes_and_propagates_domain_errors(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentManager(storage).add_definition(alice, expert())
    task = await svc.tasks.add_session(alice, definition["id"], "private")
    pid = task["project_id"]
    await svc.workspace.upload_project_file(alice, pid, "input.txt", b"input")
    with pytest.raises(AgentKitError) as error:
        await svc.workspace.preview_project_document(alice, pid, "uploads", "input.txt")
    assert error.value.status_code == 415


@pytest.mark.asyncio
async def test_preview_options_require_project_access_and_follow_live_nacos(
    storage, tmp_path
):
    """引用配置只返回非敏感字段，并且复用项目访问校验与实时配置。"""
    from egis_agentkit.client.nacos._settings import (
        ProjectPreviewConfig,
        ProjectReferenceConfig,
    )

    _, _, alice, bob = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    project = await svc.projects.create(alice, "引用预览配置")
    live = SimpleNamespace(project=SimpleNamespace(preview=ProjectPreviewConfig()))
    svc.preview.config = SimpleNamespace(snapshot=live)
    result = await svc.workspace.get_preview_options(alice, project["id"])
    assert result["maxCount"] == 4
    assert "maxTotalBytes" in result and "secret" not in str(result).lower()
    live.project.preview = ProjectPreviewConfig(
        reference=ProjectReferenceConfig(max_count=2)
    )
    assert (await svc.workspace.get_preview_options(alice, project["id"]))[
        "maxCount"
    ] == 2
    with pytest.raises((Forbidden, NotFound)):
        await svc.workspace.get_preview_options(bob, project["id"])
