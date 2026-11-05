import pytest
from test_domain import expert, identities

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.services import AgentService
from egis_agentkit.services.file_changes import FileChangeService
from egis_agentkit.services.tasks import TaskService


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
    source = await AgentService(storage).save(alice, expert())
    service = TaskService(storage, tmp_path)
    task = await service.create(alice, source["id"], "private")
    sid = task["session_id"]
    with pytest.raises(Forbidden):
        await service.rename(bob, sid, Bus(), "other")
    with pytest.raises(Conflict):
        await service.rename(alice, sid, Bus(False), "busy")
    renamed = await service.rename(alice, sid, Bus(), "  新名称  ")
    assert renamed["name"] == "新名称"
    session = await storage.read_session_unscoped("alice", task["agent_id"], sid)
    assert session.config.naming.auto is False
    assert (await TaskService(storage, tmp_path).get(alice, sid))["name"] == "新名称"
    with pytest.raises(Conflict):
        await service.rename(alice, sid, Bus(), "   ")


@pytest.mark.asyncio
async def test_baseline_same_size_delete_and_symlink_isolation(storage, tmp_path):
    service = FileChangeService(storage, tmp_path)
    root = service.files.area_root("alice", "session", "work")
    root.mkdir(parents=True)
    (root / "existing.txt").write_text("first")
    outside = tmp_path / "outside"
    outside.write_text("secret")
    (root / "link").symlink_to(outside)
    await service.observe("alice", "session")
    assert (await service.list("session"))["items"] == []
    (root / "existing.txt").write_text("other")
    (root / "new.txt").write_text("new")
    await service.observe("alice", "session")
    (root / "existing.txt").unlink()
    await FileChangeService(storage, tmp_path).observe("alice", "session")
    result = await service.list("session", limit=2)
    assert [item["change_type"] for item in result["items"]] == ["deleted", "created"]
    assert result["next_cursor"] is not None
    remainder = await service.list("session", result["next_cursor"])
    assert [
        (i["path"], i["change_type"], i["size"], i["previous_size"])
        for i in remainder["items"]
    ] == [("existing.txt", "modified", 5, 5)]
    assert all(i["path"] != "link" for i in result["items"] + remainder["items"])


@pytest.mark.asyncio
async def test_failed_scan_preserves_baseline_and_reports_error(
    storage, tmp_path, monkeypatch
):
    service = FileChangeService(storage, tmp_path)
    root = service.files.area_root("alice", "session", "work")
    root.mkdir(parents=True)
    (root / "file").write_text("one")
    await service.observe("alice", "session")
    (root / "file").write_text("two")
    original = service.scan

    def fail(*args):
        raise Conflict("scan limit")

    monkeypatch.setattr(service, "scan", fail)
    await service.observe_safe("alice", "session")
    assert (await service.list("session"))["scan_error"]["message"] == "scan limit"
    assert (await service.list("session"))["items"] == []
    monkeypatch.setattr(service, "scan", original)
    await service.observe_safe("alice", "session")
    result = await service.list("session")
    assert result["scan_error"] is None
    assert result["items"][0]["change_type"] == "modified"


@pytest.mark.asyncio
async def test_task_delete_keeps_project_files_and_observation_records(
    storage, tmp_path
):
    from sqlalchemy import insert, select

    from egis_agentkit.storage import tables as t
    from egis_agentkit.services.projects import ProjectService

    _, _, alice, _ = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    tasks = TaskService(storage, tmp_path)
    first = await tasks.create(alice, source["id"], "private")
    second = await tasks.create(alice, source["id"], "private")
    other_project = await ProjectService(storage, tmp_path).create(alice, "Other")
    other = await tasks.create(
        alice, source["id"], "private", project_id=other_project["id"]
    )
    project_id = first["project_id"]
    assert second["project_id"] == project_id
    changes = FileChangeService(storage, tmp_path)
    for pid in (project_id, other["project_id"]):
        await changes.observe(alice.user_id, pid)
        for area in ("work", "artifacts", "uploads"):
            tasks.files.write(
                alice.user_id, pid, "file.txt", b"project file", area=area
            )
        await changes.observe(alice.user_id, pid)
        async with storage.engine.begin() as conn:
            await conn.execute(
                insert(t.file_scan_errors).values(
                    project_id=pid, message="old scan error"
                )
            )
    history = await changes.list(project_id)
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
    await tasks.delete(alice, first["session_id"], bus)
    assert await changes.list(project_id) == history
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
            assert tasks.files.read(alice.user_id, pid, area, "file.txt")[0] == b"project file"
    async with storage.engine.connect() as conn:
        for table in (
            t.file_baselines,
            t.file_fingerprints,
            t.file_changes,
            t.file_scan_errors,
        ):
            for pid in (project_id, other["project_id"]):
                assert (await conn.execute(select(table).where(table.c.project_id == pid))).all()
        members = (await conn.execute(select(t.project_sessions.c.session_id))).scalars().all()
        assert first["session_id"] not in members
        assert second["session_id"] in members
        assert other["session_id"] in members
    assert (await tasks.get(alice, second["session_id"]))["project_id"] == project_id
