"""Projects own shared files independently of native conversation lifetimes."""

import asyncio

import pytest
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from sqlalchemy import delete, insert, select
from test_domain import expert, identities

from egis_agentkit.errors import Conflict, NotFound
from egis_agentkit.services import AgentService
from egis_agentkit.services.file_changes import FileChangeService
from egis_agentkit.services.projects import ProjectService
from egis_agentkit.services.tasks import TaskService
from egis_agentkit.storage import tables as t


async def seed_bus(bus, session_id):
    await bus.log_append(MessageBusKeys.session_events(session_id), {"event": "old"})
    await bus.queue_push(MessageBusKeys.inbox(session_id), {"input": "old"})
    for key in (
        MessageBusKeys.inbox_consumer(session_id),
        MessageBusKeys.bg_tasks(session_id),
        MessageBusKeys.session_event_checkpoint(session_id),
        MessageBusKeys.projection_namespace(session_id),
    ):
        await bus.registry_set(key, "old", "value")


async def assert_bus_removed(bus, session_id):
    assert await bus.log_read(MessageBusKeys.session_events(session_id)) == []
    assert await bus.queue_drain(MessageBusKeys.inbox(session_id)) == []
    for key in (
        MessageBusKeys.inbox_consumer(session_id),
        MessageBusKeys.bg_tasks(session_id),
        MessageBusKeys.session_event_checkpoint(session_id),
        MessageBusKeys.projection_namespace(session_id),
    ):
        assert await bus.registry_getall(key) == {}


@pytest.mark.asyncio
async def test_default_project_is_unique_and_project_access_is_owner_scoped(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    projects = ProjectService(storage, tmp_path)
    defaults = await asyncio.gather(*(projects.default(alice) for _ in range(4)))
    assert len({item["id"] for item in defaults}) == 1
    default = defaults[0]
    assert default["is_default"] is True
    second = await projects.create(alice, "  Research  ")
    assert second["project_name"] == "Research"
    assert second["is_default"] is False
    assert second["status_type"] == "active"
    assert second["owner_id"] == alice.user_id
    assert {p["id"] for p in await projects.list(alice)} == {default["id"], second["id"]}
    assert await projects.list(bob) == []
    for action in (
        projects.get(bob, second["id"]),
        projects.rename(bob, second["id"], "Stolen"),
        projects.delete(bob, second["id"], InMemoryMessageBus()),
    ):
        with pytest.raises(NotFound):
            await action
    renamed = await projects.rename(alice, second["id"], "  New name  ")
    assert renamed["project_name"] == "New name"
    assert (await ProjectService(storage, tmp_path).get(alice, second["id"]))["project_name"] == "New name"
    for invalid_name in ("   ", "x" * 129):
        with pytest.raises(Conflict):
            await projects.create(alice, invalid_name)
        with pytest.raises(Conflict):
            await projects.rename(alice, second["id"], invalid_name)


@pytest.mark.asyncio
async def test_chats_share_project_root_and_other_projects_remain_isolated(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    bob_source = await AgentService(storage).save(bob, expert("bob"))
    projects = ProjectService(storage, tmp_path)
    tasks = TaskService(storage, tmp_path)
    project = await projects.create(alice, "Shared")
    separate = await projects.create(alice, "Separate")
    first = await tasks.create(alice, source["id"], "private", project_id=project["id"])
    second = await tasks.create(alice, source["id"], "private", project_id=project["id"])
    other = await tasks.create(alice, source["id"], "private", project_id=separate["id"])
    assert first["session_id"] != second["session_id"]
    assert first["project_id"] == second["project_id"] == project["id"]
    assert other["project_id"] == separate["id"]
    expected_root = tmp_path / "user" / "alice" / "projects" / project["id"]
    assert tasks.files.project_root("alice", project["id"]) == expected_root
    assert tasks.files.area_root("alice", project["id"], "work") == expected_root / ".work"
    assert tasks.files.area_root("alice", project["id"], "uploads") == expected_root
    for task in (first, second):
        native = await tasks.session(alice, task["session_id"])
        assert native.config.workspace_id == project["id"]
        assert native.config.cwd == str(expected_root / ".work")
    for area in ("uploads", "work", "artifacts"):
        tasks.files.write("alice", first["project_id"], "shared.txt", b"shared", area=area)
        assert tasks.files.read("alice", second["project_id"], area, "shared.txt")[0] == b"shared"
        assert tasks.files.list("alice", separate["id"], area) == []
    assert [item["path"] for item in tasks.files.list("alice", project["id"], "uploads")] == ["shared.txt"]
    with pytest.raises(NotFound):
        await tasks.create(bob, bob_source["id"], "private", project_id=project["id"])
    assert await tasks.list(bob) == []


@pytest.mark.asyncio
async def test_project_delete_removes_chats_bus_history_and_files_only_in_project(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    projects = ProjectService(storage, tmp_path)
    tasks = TaskService(storage, tmp_path)
    doomed = await projects.create(alice, "Remove")
    surviving = await projects.create(alice, "Keep")
    first = await tasks.create(alice, source["id"], "private", project_id=doomed["id"])
    second = await tasks.create(alice, source["id"], "private", project_id=doomed["id"])
    other = await tasks.create(alice, source["id"], "private", project_id=surviving["id"])
    bus = InMemoryMessageBus()
    changes = FileChangeService(storage, tmp_path)
    for task in (first, second, other):
        await seed_bus(bus, task["session_id"])
    for project in (doomed, surviving):
        pid = project["id"]
        await changes.observe("alice", pid)
        for area in ("uploads", "work", "artifacts"):
            tasks.files.write("alice", pid, "owned.txt", b"owned", area=area)
        await changes.observe("alice", pid)
        async with storage.transaction() as conn:
            await conn.execute(insert(t.file_scan_errors).values(project_id=pid, message="prior error"))
    other_history = await changes.list(surviving["id"])
    await projects.delete(alice, doomed["id"], bus)
    with pytest.raises(NotFound):
        await projects.get(alice, doomed["id"])
    for task in (first, second):
        assert await storage.get_session("alice", task["agent_id"], task["session_id"]) is None
        await assert_bus_removed(bus, task["session_id"])
        assert not await bus.is_locked(MessageBusKeys.session_lock(task["session_id"]))
    assert not tasks.files.project_root("alice", doomed["id"]).exists()
    async with storage.engine.connect() as conn:
        for table in (t.project_sessions, t.file_baselines, t.file_fingerprints, t.file_changes, t.file_scan_errors):
            assert not (await conn.execute(select(table).where(table.c.project_id == doomed["id"]))).all()
            assert (await conn.execute(select(table).where(table.c.project_id == surviving["id"]))).all()
    # A scan already authorized before deletion must not recreate removed state.
    await changes.observe_safe("alice", doomed["id"], discover=True)
    assert await changes.list(doomed["id"]) == {"items": [], "next_cursor": None, "scan_error": None}
    assert not tasks.files.project_root("alice", doomed["id"]).exists()
    async with storage.engine.connect() as conn:
        assert await conn.scalar(select(t.file_baselines.c.project_id).where(t.file_baselines.c.project_id == doomed["id"])) is None
    assert await changes.list(surviving["id"]) == other_history
    assert (await tasks.get(alice, other["session_id"]))["project_id"] == surviving["id"]
    assert len(await bus.log_read(MessageBusKeys.session_events(other["session_id"]))) == 1
    assert len(await bus.queue_drain(MessageBusKeys.inbox(other["session_id"]))) == 1
    for area in ("uploads", "work", "artifacts"):
        assert tasks.files.read("alice", surviving["id"], area, "owned.txt")[0] == b"owned"


@pytest.mark.asyncio
@pytest.mark.parametrize("blocker", ["session", "project"])
async def test_busy_project_delete_has_no_partial_removal(storage, tmp_path, blocker):
    _, _, alice, _ = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    projects = ProjectService(storage, tmp_path)
    tasks = TaskService(storage, tmp_path)
    first = await tasks.create(alice, source["id"], "private")
    second = await tasks.create(alice, source["id"], "private")
    pid = first["project_id"]
    bus = InMemoryMessageBus()
    changes = FileChangeService(storage, tmp_path)
    await changes.observe("alice", pid)
    tasks.files.write("alice", pid, "keep.txt", b"keep", area="uploads")
    await changes.observe("alice", pid)
    history = await changes.list(pid)
    for task in (first, second):
        await seed_bus(bus, task["session_id"])
    if blocker == "session":
        busy = max(first["session_id"], second["session_id"])
        assert await bus.try_lock(MessageBusKeys.session_lock(busy))
        with pytest.raises(Conflict) as error:
            await projects.delete(alice, pid, bus)
        assert error.value.status_code == 409
        await bus.unlock(MessageBusKeys.session_lock(busy))
    else:
        # A separate service/connection must see the same lifetime lock.
        async with ProjectService(storage, tmp_path).guard(alice, pid):
            with pytest.raises(Conflict) as error:
                await projects.delete(alice, pid, bus)
            assert error.value.status_code == 409
    assert (await projects.get(alice, pid))["status_type"] == "active"
    assert tasks.files.read("alice", pid, "uploads", "keep.txt")[0] == b"keep"
    assert await changes.list(pid) == history
    for task in (first, second):
        assert await storage.get_session("alice", task["agent_id"], task["session_id"])
        assert len(await bus.log_read(MessageBusKeys.session_events(task["session_id"]))) == 1
        assert not await bus.is_locked(MessageBusKeys.session_lock(task["session_id"]))
    async with storage.engine.connect() as conn:
        assert set((await conn.execute(select(t.project_sessions.c.session_id).where(t.project_sessions.c.project_id == pid))).scalars()) == {first["session_id"], second["session_id"]}
    await projects.delete(alice, pid, bus)
    assert not tasks.files.project_root("alice", pid).exists()


@pytest.mark.asyncio
async def test_legacy_sessions_are_adopted_idempotently_with_collision_history(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    projects = ProjectService(storage, tmp_path)
    tasks = TaskService(storage, tmp_path)
    legacy = [await tasks.create(alice, source["id"], "private") for _ in range(2)]
    pid = legacy[0]["project_id"]
    areas = ("uploads", "work", "artifacts")
    for area in areas:
        tasks.files.write("alice", pid, "nested/report.txt", b"current project", area=area)
    for task in legacy:
        sid = task["session_id"]
        native = await tasks.session(alice, sid)
        native.state.middle_context["egis_agentkit"].pop("project_id")
        native.config.workspace_id = sid
        native.config.cwd = str(tasks.files.user_root("alice") / "work" / sid)
        await storage.upsert_session("alice", native.agent_id, native.config, session_id=sid, state=native.state)
        async with storage.transaction() as conn:
            await conn.execute(delete(t.project_sessions).where(t.project_sessions.c.session_id == sid))
            await conn.execute(insert(t.file_changes), [dict(project_id=sid, area=area, path="nested/report.txt", change_type="created", size=len(sid)) for area in areas])
        for area in areas:
            old_root = tasks.files.user_root("alice") / area / sid / "nested"
            old_root.mkdir(parents=True)
            (old_root / "report.txt").write_bytes(sid.encode())
    # Model a crash after moving files but before committing native adoption.
    tasks.files.migrate_legacy_session("alice", legacy[0]["session_id"], pid)
    await projects.migrate_user(alice)
    histories = await FileChangeService(storage, tmp_path).list(pid)
    assert len(histories["items"]) == 6
    mapped_paths = {(item["area"], item["path"]) for item in histories["items"]}
    assert len(mapped_paths) == 6
    for task in legacy:
        native = await storage.read_session_unscoped("alice", task["agent_id"], task["session_id"])
        assert native.state.middle_context["egis_agentkit"]["project_id"] == pid
        assert native.config.workspace_id == pid
        assert native.config.cwd == str(tasks.files.area_root("alice", pid, "work"))
        for area in areas:
            assert not (tasks.files.user_root("alice") / area / task["session_id"]).exists()
    for area in areas:
        assert tasks.files.read("alice", pid, area, "nested/report.txt")[0] == b"current project"
        adopted = [tasks.files.read("alice", pid, area, path)[0] for recorded_area, path in mapped_paths if recorded_area == area]
        assert set(adopted) == {task["session_id"].encode() for task in legacy}
    await projects.migrate_user(alice)
    assert await FileChangeService(storage, tmp_path).list(pid) == histories
    async with storage.engine.connect() as conn:
        assert len((await conn.execute(select(t.project_sessions).where(t.project_sessions.c.project_id == pid))).all()) == 2
        for task in legacy:
            assert not (await conn.execute(select(t.file_changes).where(t.file_changes.c.project_id == task["session_id"]))).all()
