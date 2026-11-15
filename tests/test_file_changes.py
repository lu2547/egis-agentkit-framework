import pytest
from test_domain import expert, identities

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service._session import SessionService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.project._file_manager import ProjectFileManager
from egis_agentkit.service.project._project_manager import ProjectManager


class Bus:
    def __init__(self, available=True):
        self.available = available

    async def try_lock(self, key):
        return self.available

    async def unlock(self, key):
        pass


@pytest.mark.asyncio
async def test_rename_owner_lock_and_persistence(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    service = SessionService(storage, tmp_path)
    task = await service.add_session(alice, source["id"], "private")
    sid = task["session_id"]
    with pytest.raises(Forbidden):
        await service.update_session_name(bob, sid, Bus(), "other")
    with pytest.raises(Conflict):
        await service.update_session_name(alice, sid, Bus(False), "busy")
    renamed = await service.update_session_name(alice, sid, Bus(), "  新名称  ")
    assert renamed["name"] == "新名称"
    session = await storage.read_session_unscoped("alice", task["agent_id"], sid)
    assert session.config.naming.auto is False
    assert (await SessionService(storage, tmp_path).get_session(alice, sid))["name"] == "新名称"
    with pytest.raises(Conflict):
        await service.update_session_name(alice, sid, Bus(), "   ")


@pytest.mark.asyncio
async def test_baseline_same_size_delete_and_symlink_isolation(storage, tmp_path):
    service = ProjectFileManager(storage, tmp_path)
    _, _, alice, _ = await identities(storage)
    project_id = (await ProjectManager(storage, tmp_path).create(alice, "Project"))[
        "id"
    ]
    root = service.files.area_root("alice", project_id, "work")
    (root / "existing.txt").write_text("first")
    outside = tmp_path / "outside"
    outside.write_text("secret")
    (root / "link").symlink_to(outside)
    await service.observe("alice", project_id)
    assert (await service.list_changes(project_id))["items"] == []
    (root / "existing.txt").write_text("other")
    (root / "new.txt").write_text("new")
    await service.observe("alice", project_id)
    (root / "existing.txt").unlink()
    await ProjectFileManager(storage, tmp_path).observe("alice", project_id)
    result = await service.list_changes(project_id, limit=2)
    assert [item["change_type"] for item in result["items"]] == ["deleted", "created"]
    assert result["next_cursor"] is not None
    remainder = await service.list_changes(project_id, result["next_cursor"])
    assert [
        (i["path"], i["change_type"], i["size"], i["previous_size"])
        for i in remainder["items"]
    ] == [("existing.txt", "modified", 5, 5)]
    assert all(i["path"] != "link" for i in result["items"] + remainder["items"])


@pytest.mark.asyncio
async def test_failed_scan_preserves_baseline_and_reports_error(
    storage, tmp_path, monkeypatch
):
    service = ProjectFileManager(storage, tmp_path)
    _, _, alice, _ = await identities(storage)
    project_id = (await ProjectManager(storage, tmp_path).create(alice, "Project"))[
        "id"
    ]
    root = service.files.area_root("alice", project_id, "work")
    (root / "file").write_text("one")
    await service.observe("alice", project_id)
    (root / "file").write_text("two")
    original = service.scan

    def fail(*args):
        raise Conflict("scan limit")

    monkeypatch.setattr(service, "scan", fail)
    await service.observe_safe("alice", project_id)
    assert (await service.list_changes(project_id))["scan_error"]["message"] == "scan limit"
    assert (await service.list_changes(project_id))["items"] == []
    monkeypatch.setattr(service, "scan", original)
    await service.observe_safe("alice", project_id)
    result = await service.list_changes(project_id)
    assert result["scan_error"] is None
    assert result["items"][0]["change_type"] == "modified"


async def test_file_history_reads_current_page_limits(storage, tmp_path):
    from types import SimpleNamespace

    from egis_agentkit.client.nacos._settings import ProjectChangesConfig
    from egis_agentkit.errors import AgentKitError

    _, _, alice, _ = await identities(storage)
    project_id = (await ProjectManager(storage, tmp_path).create(alice, "Paged"))["id"]
    service = ProjectFileManager(storage, tmp_path)
    await service.observe(alice.user_id, project_id)
    for path in ("first.txt", "second.txt"):
        service.files.write(alice.user_id, project_id, path, b"file")
    await service.observe(alice.user_id, project_id)
    service.config = SimpleNamespace(
        snapshot=SimpleNamespace(
            project=SimpleNamespace(
                changes=ProjectChangesConfig(default_page_size=1, max_page_size=2),
            )
        )
    )

    page = await service.list_changes(project_id)
    assert len(page["items"]) == 1
    assert page["next_cursor"] is not None
    assert len((await service.list_changes(project_id, page["next_cursor"]))["items"]) == 1
    with pytest.raises(AgentKitError) as error:
        await service.list_changes(project_id, limit=3)
    assert error.value.status_code == 422


@pytest.mark.asyncio
async def test_task_delete_keeps_project_files_and_observation_records(
    storage, tmp_path
):
    from sqlalchemy import func, select, update

    from egis_agentkit.storage import tables as t

    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    tasks = SessionService(storage, tmp_path)
    first = await tasks.add_session(alice, source["id"], "private")
    second = await tasks.add_session(alice, source["id"], "private")
    other_project = await ProjectManager(storage, tmp_path).create(alice, "Other")
    other = await tasks.add_session(
        alice, source["id"], "private", project_id=other_project["id"]
    )
    project_id = first["project_id"]
    assert second["project_id"] == project_id
    changes = ProjectFileManager(storage, tmp_path)
    for pid in (project_id, other["project_id"]):
        await changes.observe(alice.user_id, pid)
        for area in ("work", "artifacts", "uploads"):
            tasks.files.write(
                alice.user_id, pid, "file.txt", b"project file", area=area
            )
        await changes.observe(alice.user_id, pid)
        async with storage.engine.begin() as conn:
            await conn.execute(
                update(t.projects)
                .where(t.projects.c.id == pid)
                .values(scan_error="old scan error", scan_error_at=func.now())
            )
    history = await changes.list_changes(project_id)
    from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys

    bus = InMemoryMessageBus()
    for sid in (first["session_id"], second["session_id"]):
        await bus.log_append(MessageBusKeys.session_events(sid), {"event": "old"})
        await bus.queue_push(MessageBusKeys.inbox(sid), {"input": "old"})
        for key in (
            MessageBusKeys.inbox_consumer(sid),
            MessageBusKeys.bg_tasks(sid),
            MessageBusKeys.session_event_checkpoint(sid),
            MessageBusKeys.projection_namespace(sid),
        ):
            await bus.registry_set(key, "old", "value")
    await tasks.delete_session(alice, first["session_id"], bus)
    assert await changes.list_changes(project_id) == history
    assert await bus.log_read(MessageBusKeys.session_events(first["session_id"])) == []
    assert await bus.queue_drain(MessageBusKeys.inbox(first["session_id"])) == []
    for key in (
        MessageBusKeys.inbox_consumer(first["session_id"]),
        MessageBusKeys.bg_tasks(first["session_id"]),
        MessageBusKeys.session_event_checkpoint(first["session_id"]),
        MessageBusKeys.projection_namespace(first["session_id"]),
    ):
        assert await bus.registry_getall(key) == {}
    assert (
        len(await bus.log_read(MessageBusKeys.session_events(second["session_id"])))
        == 1
    )
    assert len(await bus.queue_drain(MessageBusKeys.inbox(second["session_id"]))) == 1
    for area in ("work", "artifacts", "uploads"):
        for pid in (project_id, other["project_id"]):
            assert (
                tasks.files.read(alice.user_id, pid, area, "file.txt")[0]
                == b"project file"
            )
    async with storage.engine.connect() as conn:
        for table in (
            t.file_fingerprints,
            t.file_changes,
        ):
            for pid in (project_id, other["project_id"]):
                assert (
                    await conn.execute(select(table).where(table.c.project_id == pid))
                ).all()
    members = await storage.sessions.in_project(alice.user_id, project_id)
    assert {row.id for row in members} == {second["session_id"]}
    members = await storage.sessions.in_project(alice.user_id, other["project_id"])
    assert {row.id for row in members} == {other["session_id"]}
    assert (await tasks.get_session(alice, second["session_id"]))["project_id"] == project_id
