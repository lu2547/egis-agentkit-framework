"""Project scans update changed files only and read scoped history in one query."""

from test_domain import identities
from test_identity_sql_queries import statements

from egis_agentkit.service.project._file_manager import ProjectFileManager
from egis_agentkit.service.project._project_manager import ProjectManager


async def test_unchanged_project_scan_does_not_rewrite_fifty_fingerprints(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    project = await ProjectManager(storage, tmp_path).create(alice, "Files")
    files = ProjectFileManager(storage, tmp_path)
    root = files.files.area_root(alice.user_id, project["id"], "work")
    for index in range(50):
        (root / f"{index}.txt").write_text("unchanged")
    await files.observe(alice.user_id, project["id"])
    with statements(storage) as queries:
        await files.observe(alice.user_id, project["id"])
    assert len(queries) <= 2  # one project lock and one scoped baseline JOIN
    assert not any(
        query.lstrip().startswith(("INSERT", "UPDATE", "DELETE")) for query in queries
    )
    assert (await files.list_changes(project["id"]))["items"] == []
    (root / "3.txt").write_text("modified")
    with statements(storage) as queries:
        await files.observe(alice.user_id, project["id"])
    assert len(queries) <= 4
    assert not any(query.lstrip().startswith("DELETE") for query in queries)
    result = await files.list_changes(project["id"])
    assert [(item["path"], item["change_type"]) for item in result["items"]] == [
        ("3.txt", "modified")
    ]
    assert len(await storage.project_files.fingerprints(project["id"])) == 50


async def test_file_history_joins_error_after_limiting_project_page(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    project = await ProjectManager(storage, tmp_path).create(alice, "History")
    files = ProjectFileManager(storage, tmp_path)
    await files.observe(alice.user_id, project["id"])
    files.files.write(alice.user_id, project["id"], "new.txt", b"new")
    await files.observe(alice.user_id, project["id"])
    async with files.locked([project["id"]]) as records:
        await records.record_scan_error(alice.user_id, project["id"], "scan failed")
    with statements(storage) as queries:
        page = await files.list_changes(project["id"], limit=1)
    assert len(queries) == 1
    assert page["items"][0]["path"] == "new.txt"
    assert page["scan_error"]["message"] == "scan failed"
    with statements(storage) as queries:
        empty = await files.list_changes(
            project["id"], cursor=page["items"][0]["change_id"]
        )
    assert len(queries) == 1
    assert empty["items"] == []
    assert empty["scan_error"]["message"] == "scan failed"


async def test_deleting_project_does_not_load_each_of_fifty_sessions(storage, tmp_path):
    from agentscope.app.storage._sql._mappers import _from_record
    from agentscope.app.storage._sql._tables import SessionRow
    from sqlalchemy.ext.asyncio import AsyncSession
    from test_domain import expert

    from egis_agentkit.core.message_bus import EgisMessageBus
    from egis_agentkit.service.agent._agent_manager import AgentManager
    from egis_agentkit.service.session._session import SessionService
    from egis_agentkit.storage.model import new_id

    _, _, alice, _ = await identities(storage)
    projects = ProjectManager(storage, tmp_path)
    project = await projects.create(alice, "Delete batch")
    source = await AgentManager(storage).add_definition(alice, expert())
    sessions = SessionService(storage, tmp_path, projects=projects)
    task = await sessions.add_session(
        alice, source["id"], "private", project_id=project["id"]
    )
    template = await sessions.get_session_record(alice, task["session_id"])
    async with AsyncSession(storage.engine) as db:
        db.add_all(
            [
                _from_record(
                    SessionRow, template.model_copy(update={"id": new_id()}, deep=True)
                )
                for _ in range(49)
            ]
        )
        await db.commit()
    async with EgisMessageBus() as bus:
        with statements(storage) as queries:
            await projects.delete(alice, project["id"], bus)
    reads = [
        query for query in queries if query.lstrip().startswith(("SELECT", "WITH"))
    ]
    assert len(reads) <= 15
    assert await storage.sessions.in_project(alice.user_id, project["id"]) == []
    assert await storage.projects.get(alice.user_id, project["id"]) is None


async def test_existing_default_and_rename_do_not_reread_project(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    projects = ProjectManager(storage, tmp_path)
    default = await projects.default(alice)
    with statements(storage) as queries:
        assert await projects.default(alice) == default
    assert len(queries) == 1
    project = await projects.create(alice, "Rename")
    with statements(storage) as queries:
        renamed = await projects.rename(alice, project["id"], "Renamed")
    assert renamed["id"] == project["id"]
    assert renamed["project_name"] == "Renamed"
    assert len(queries) == 3  # lock, scope check, UPDATE RETURNING
