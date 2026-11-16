"""Projects own shared files independently of native conversation lifetimes."""

import asyncio

import pytest
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from sqlalchemy import func, select, text, update
from test_domain import expert, identities

from egis_agentkit.errors import Conflict, NotFound
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.project._file_manager import ProjectFileManager
from egis_agentkit.service.project._project_manager import ProjectManager
from egis_agentkit.service.session._session import SessionService
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
    projects = ProjectManager(storage, tmp_path)
    defaults = await asyncio.gather(*(projects.default(alice) for _ in range(4)))
    assert len({item["id"] for item in defaults}) == 1
    default = defaults[0]
    assert default["is_default"] is True
    second = await projects.create(alice, "  Research  ")
    assert second["project_name"] == "Research"
    assert second["is_default"] is False
    assert second["status_type"] == "active"
    assert second["owner_id"] == alice.user_id
    assert {p["id"] for p in await projects.list(alice)} == {
        default["id"],
        second["id"],
    }
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
    assert (await ProjectManager(storage, tmp_path).get(alice, second["id"]))[
        "project_name"
    ] == "New name"
    for invalid_name in ("   ", "x" * 129):
        with pytest.raises(Conflict):
            await projects.create(alice, invalid_name)
        with pytest.raises(Conflict):
            await projects.rename(alice, second["id"], invalid_name)


@pytest.mark.asyncio
async def test_chats_share_project_root_and_other_projects_remain_isolated(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    bob_source = await AgentManager(storage).add_definition(bob, expert("bob"))
    projects = ProjectManager(storage, tmp_path)
    tasks = SessionService(storage, tmp_path)
    project = await projects.create(alice, "Shared")
    separate = await projects.create(alice, "Separate")
    first = await tasks.add_session(
        alice, source["id"], "private", project_id=project["id"]
    )
    second = await tasks.add_session(
        alice, source["id"], "private", project_id=project["id"]
    )
    other = await tasks.add_session(
        alice, source["id"], "private", project_id=separate["id"]
    )
    assert first["session_id"] != second["session_id"]
    assert first["project_id"] == second["project_id"] == project["id"]
    assert other["project_id"] == separate["id"]
    expected_root = tmp_path / "user" / "alice" / "projects" / project["id"]
    assert tasks.files.project_root("alice", project["id"]) == expected_root
    assert (
        tasks.files.area_root("alice", project["id"], "work") == expected_root / ".work"
    )
    assert tasks.files.area_root("alice", project["id"], "uploads") == expected_root
    for task in (first, second):
        native = await tasks.get_session_record(alice, task["session_id"])
        from egis_agentkit.core.workspace._identity import workspace_id_for

        assert native.config.workspace_id == workspace_id_for(alice.user_id)
        assert native.config.cwd == str(expected_root / ".work")
        assert native.agent_id == task["agent_id"] != source["id"]
        assert (
            native.state.middle_context["egis_agentkit"]["definition_id"]
            == source["id"]
        )
    async with storage.engine.connect() as conn:
        assert dict(
            (await conn.execute(text("SELECT id, project_id FROM sessions"))).all()
        ) == {
            first["session_id"]: project["id"],
            second["session_id"]: project["id"],
            other["session_id"]: separate["id"],
        }
    for area in ("uploads", "work", "artifacts"):
        tasks.files.write(
            "alice", first["project_id"], "shared.txt", b"shared", area=area
        )
        assert (
            tasks.files.read("alice", second["project_id"], area, "shared.txt")[0]
            == b"shared"
        )
        assert tasks.files.list("alice", separate["id"], area) == []
    assert [
        item["path"] for item in tasks.files.list("alice", project["id"], "uploads")
    ] == ["shared.txt"]
    with pytest.raises(NotFound):
        await tasks.add_session(
            bob, bob_source["id"], "private", project_id=project["id"]
        )
    assert await tasks.list_sessions(bob) == []


@pytest.mark.asyncio
async def test_project_delete_removes_chats_bus_history_and_files_only_in_project(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    projects = ProjectManager(storage, tmp_path)
    tasks = SessionService(storage, tmp_path)
    doomed = await projects.create(alice, "Remove")
    surviving = await projects.create(alice, "Keep")
    first = await tasks.add_session(
        alice, source["id"], "private", project_id=doomed["id"]
    )
    second = await tasks.add_session(
        alice, source["id"], "private", project_id=doomed["id"]
    )
    other = await tasks.add_session(
        alice, source["id"], "private", project_id=surviving["id"]
    )
    bus = InMemoryMessageBus()
    changes = ProjectFileManager(storage, tmp_path)
    for task in (first, second, other):
        await seed_bus(bus, task["session_id"])
    for project in (doomed, surviving):
        pid = project["id"]
        await changes.observe("alice", pid)
        for area in ("uploads", "work", "artifacts"):
            tasks.files.write("alice", pid, "owned.txt", b"owned", area=area)
        await changes.observe("alice", pid)
        async with storage.transaction() as conn:
            await conn.execute(
                update(t.projects)
                .where(t.projects.c.id == pid)
                .values(scan_error="prior error", scan_error_at=func.now())
            )
    other_history = await changes.list_changes(surviving["id"])
    await projects.delete(alice, doomed["id"], bus)
    with pytest.raises(NotFound):
        await projects.get(alice, doomed["id"])
    for task in (first, second):
        assert (
            await storage.get_session("alice", task["agent_id"], task["session_id"])
            is None
        )
        await assert_bus_removed(bus, task["session_id"])
        assert not await bus.is_locked(MessageBusKeys.session_lock(task["session_id"]))
    assert not tasks.files.project_root("alice", doomed["id"]).exists()
    async with storage.engine.connect() as conn:
        for table in (t.file_fingerprints, t.file_changes):
            assert not (
                await conn.execute(
                    select(table).where(table.c.project_id == doomed["id"])
                )
            ).all()
            assert (
                await conn.execute(
                    select(table).where(table.c.project_id == surviving["id"])
                )
            ).all()
    # A scan already authorized before deletion must not recreate removed state.
    await changes.observe_safe("alice", doomed["id"], discover=True)
    assert await changes.list_changes(doomed["id"]) == {
        "items": [],
        "next_cursor": None,
        "scan_error": None,
    }
    assert not tasks.files.project_root("alice", doomed["id"]).exists()
    async with storage.engine.connect() as conn:
        assert (
            await conn.scalar(
                select(t.projects.c.id).where(t.projects.c.id == doomed["id"])
            )
            is None
        )
    assert await changes.list_changes(surviving["id"]) == other_history
    assert (await tasks.get_session(alice, other["session_id"]))[
        "project_id"
    ] == surviving["id"]
    assert (
        len(await bus.log_read(MessageBusKeys.session_events(other["session_id"]))) == 1
    )
    assert len(await bus.queue_drain(MessageBusKeys.inbox(other["session_id"]))) == 1
    for area in ("uploads", "work", "artifacts"):
        assert (
            tasks.files.read("alice", surviving["id"], area, "owned.txt")[0] == b"owned"
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("blocker", ["session", "project"])
async def test_busy_project_delete_has_no_partial_removal(storage, tmp_path, blocker):
    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    projects = ProjectManager(storage, tmp_path)
    tasks = SessionService(storage, tmp_path)
    project = await projects.create(alice, "Busy")
    first = await tasks.add_session(
        alice, source["id"], "private", project_id=project["id"]
    )
    second = await tasks.add_session(
        alice, source["id"], "private", project_id=project["id"]
    )
    pid = first["project_id"]
    bus = InMemoryMessageBus()
    changes = ProjectFileManager(storage, tmp_path)
    await changes.observe("alice", pid)
    tasks.files.write("alice", pid, "keep.txt", b"keep", area="uploads")
    await changes.observe("alice", pid)
    history = await changes.list_changes(pid)
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
        async with ProjectManager(storage, tmp_path).guard(alice, pid):
            with pytest.raises(Conflict) as error:
                await projects.delete(alice, pid, bus)
            assert error.value.status_code == 409
    assert (await projects.get(alice, pid))["status_type"] == "active"
    assert tasks.files.read("alice", pid, "uploads", "keep.txt")[0] == b"keep"
    assert await changes.list_changes(pid) == history
    for task in (first, second):
        assert await storage.get_session("alice", task["agent_id"], task["session_id"])
        assert (
            len(await bus.log_read(MessageBusKeys.session_events(task["session_id"])))
            == 1
        )
        assert not await bus.is_locked(MessageBusKeys.session_lock(task["session_id"]))
    assert {
        row.id for row in await storage.sessions.in_project(alice.user_id, pid)
    } == {
        first["session_id"],
        second["session_id"],
    }
    await projects.delete(alice, pid, bus)
    assert not tasks.files.project_root("alice", pid).exists()


@pytest.mark.asyncio
async def test_unbound_session_is_rejected_without_creating_project_membership(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    tasks = SessionService(storage, tmp_path)
    source = await AgentManager(storage).add_definition(alice, expert())
    task = await tasks.add_session(alice, source["id"], "private")
    sid = task["session_id"]
    native = await tasks.get_session_record(alice, sid)
    native.state.middle_context["egis_agentkit"].pop("project_id")
    await storage.update_session_state("alice", native.agent_id, sid, native.state)
    assert [p["id"] for p in await tasks.projects.list(alice)] == [task["project_id"]]
    with pytest.raises(NotFound):
        await tasks.get_session_record(alice, sid)

    async with storage.engine.connect() as conn:
        assert (
            await conn.scalar(
                text("SELECT project_id FROM sessions WHERE id=:sid"), {"sid": sid}
            )
            is None
        )


async def test_project_delete_rolls_back_session_and_history_on_directory_failure(
    storage, tmp_path, monkeypatch
):
    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    tasks = SessionService(storage, tmp_path)
    project = await tasks.projects.create(alice, "Remove")
    task = await tasks.add_session(
        alice, source["id"], "private", project_id=project["id"]
    )
    sid, pid = task["session_id"], task["project_id"]
    bus = InMemoryMessageBus()
    await seed_bus(bus, sid)
    changes = ProjectFileManager(storage, tmp_path)
    await changes.observe(alice.user_id, pid)
    tasks.files.write(alice.user_id, pid, "keep.txt", b"keep", area="uploads")
    await changes.observe(alice.user_id, pid)
    history = await changes.list_changes(pid)

    def fail_remove(*args):
        raise OSError("directory temporarily unavailable")

    with monkeypatch.context() as context:
        context.setattr(tasks.projects.files, "remove_project", fail_remove)
        with pytest.raises(OSError, match="directory temporarily unavailable"):
            await tasks.projects.delete(alice, pid, bus)

    assert (await tasks.projects.get(alice, pid, allow_deleting=True))[
        "status_type"
    ] == "deleting"
    assert await storage.get_session(alice.user_id, task["agent_id"], sid)
    assert await changes.list_changes(pid) == history
    assert await bus.log_read(MessageBusKeys.session_events(sid))
    assert not await bus.is_locked(MessageBusKeys.session_lock(sid))
    await tasks.projects.delete(alice, pid, bus)
    assert await storage.get_session(alice.user_id, task["agent_id"], sid) is None
    assert (await changes.list_changes(pid))["items"] == []
    await assert_bus_removed(bus, sid)


@pytest.mark.asyncio
async def test_default_project_cannot_be_deleted_even_after_rename(storage, tmp_path):
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from egis_agentkit.errors import Forbidden

    _, _, alice, _ = await identities(storage)
    workspaces = SimpleNamespace(destroy_workspaces=AsyncMock())
    projects = ProjectManager(storage, tmp_path, workspaces=workspaces)
    default = await projects.default(alice)
    source = await AgentManager(storage).add_definition(alice, expert())
    sessions = SessionService(storage, tmp_path, projects=projects)
    session = await sessions.add_session(
        alice, source["id"], "private", project_id=default["id"]
    )
    projects.files.write(
        alice.user_id, default["id"], "keep.txt", b"keep", area="uploads"
    )
    await projects.rename(alice, default["id"], "日常工作")
    with pytest.raises(Forbidden, match="默认项目不能删除"):
        await projects.delete(alice, default["id"], InMemoryMessageBus())
    current = await projects.default(alice)
    assert current["id"] == default["id"] and current["status_type"] == "active"
    assert (
        projects.files.read(alice.user_id, default["id"], "uploads", "keep.txt")[0]
        == b"keep"
    )
    assert await sessions.get_session_record(alice, session["session_id"])
    workspaces.destroy_workspaces.assert_not_awaited()
