import pytest
from agentscope.app.message_bus import MessageBusKeys
from test_domain import expert, identities

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.services import AgentService
from egis_agentkit.service.session.service import TaskService
from egis_agentkit.service.workspace.files import WorkspaceFiles


def test_user_workspace_is_scoped_and_rejects_symlinks(tmp_path):
    files = WorkspaceFiles(tmp_path)
    target = files.write_user("alice", "uploads/nested/a.txt", b"hello")
    assert target.read_bytes() == b"hello"
    assert [x["path"] for x in files.list_user("alice", "")] == [
        "artifacts",
        "uploads",
        "work",
    ]
    assert (
        files.list_user("alice", "uploads/nested")[0]["path"] == "uploads/nested/a.txt"
    )
    assert files.read_user("alice", "uploads/nested/a.txt")[0] == b"hello"
    with pytest.raises(Forbidden):
        files.read_user("alice", "../bob/uploads/a.txt")
    with pytest.raises(Forbidden):
        files.read_user("alice", "private/a.txt")
    (files.user_root("alice") / "uploads" / "link").symlink_to(
        tmp_path, target_is_directory=True
    )
    with pytest.raises(Forbidden):
        files.read_user("alice", "uploads/link/a.txt")
    assert not files.list_user("bob", "uploads")


@pytest.mark.asyncio
async def test_task_naming_and_delete_keeps_project_files(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    service = TaskService(storage, tmp_path)
    task = await service.create(alice, source["id"], "private")
    explicit = await service.create(alice, source["id"], "private", name="My title")
    record = await service.session(alice, task["session_id"])
    named = await service.session(alice, explicit["session_id"])
    assert record.config.naming.auto is True
    assert named.config.naming.auto is False and named.config.name == "My title"
    service.files.write("alice", task["project_id"], "keep.txt", b"keep")
    with pytest.raises(Forbidden):
        await service.delete(bob, task["session_id"], None)

    from agentscope.app.message_bus import InMemoryMessageBus

    await service.delete(alice, task["session_id"], InMemoryMessageBus())
    assert service.files.read("alice", task["project_id"], "work", "keep.txt")[0] == b"keep"
    assert len(await service.list(alice)) == 1


@pytest.mark.asyncio
async def test_workspace_http_routes_and_task_upload_persist(storage, tmp_path):
    import httpx
    from egis_agentkit_app.manager_router import router
    from egis_agentkit_app.security import current_user
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    from egis_agentkit.errors import AgentKitError
    from egis_agentkit.service.project.service import ProjectService

    _, _, alice, bob = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    projects = ProjectService(storage, tmp_path)
    tasks = TaskService(storage, tmp_path)
    first = await tasks.create(alice, source["id"], "private")
    same_project = await tasks.create(alice, source["id"], "private")
    isolated_project = await projects.create(alice, "Isolated")
    isolated = await tasks.create(
        alice, source["id"], "private", project_id=isolated_project["id"]
    )
    sid = first["session_id"]
    isolated_sid = isolated["session_id"]
    project_id = first["project_id"]
    app = FastAPI()
    app.include_router(router)

    @app.exception_handler(AgentKitError)
    async def domain_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    from egis_agentkit.service.workspace.changes import FileChangeService

    changes = FileChangeService(storage, tmp_path)
    from types import SimpleNamespace

    from egis_agentkit.service.workspace.preview import DocumentPreviewService
    from egis_agentkit.service.workspace.service import WorkspaceService

    files = WorkspaceFiles(tmp_path)
    app.state.egis_services = SimpleNamespace(
        workspace=WorkspaceService(files, projects, tasks, changes, DocumentPreviewService())
    )
    app.dependency_overrides[current_user] = lambda: alice
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        upload = await client.post(
            "/manager/workspace/files",
            files={"file": ("x.txt", b"abc")},
            data={"path": "folder"},
        )
        assert upload.status_code == 201
        assert upload.json()["path"] == "uploads/folder/x.txt"
        query_upload = await client.post(
            "/manager/workspace/files?path=uploads/sub", files={"file": ("q.txt", b"q")}
        )
        assert query_upload.json()["path"] == "uploads/sub/q.txt"
        listed = await client.get(
            "/manager/workspace/files", params={"path": "uploads/folder"}
        )
        assert listed.json()["items"][0]["path"] == "uploads/folder/x.txt"
        assert (
            await client.get(
                "/manager/workspace/file", params={"path": "uploads/folder/x.txt"}
            )
        ).content == b"abc"
        task_upload = await client.post(
            f"/manager/tasks/{sid}/files", files={"file": ("task.txt", b"original")}
        )
        assert task_upload.status_code == 201
        history = await client.get(f"/manager/tasks/{sid}/file-changes")
        assert history.status_code == 200
        assert [
            (item["path"], item["change_type"]) for item in history.json()["items"]
        ] == [("task.txt", "created")]
        assert history.json()["items"][0]["area"] == "uploads"
        assert task_upload.json()["area"] == "uploads"
        assert (
            await client.get(
                f"/manager/tasks/{sid}/file", params={"area": "uploads", "path": "task.txt"}
            )
        ).content == b"original"
        assert (
            await client.get(f"/manager/tasks/{sid}/files", params={"area": "work"})
        ).json()["items"] == []
        assert (
            await client.get(
                f"/manager/tasks/{same_project['session_id']}/file",
                params={"area": "uploads", "path": "task.txt"},
            )
        ).content == b"original"
        shared_history = await client.get(
            f"/manager/tasks/{same_project['session_id']}/file-changes"
        )
        assert shared_history.json()["items"] == history.json()["items"]
        assert (
            await client.get(f"/manager/tasks/{isolated_sid}/files", params={"area": "uploads"})
        ).json()["items"] == []
        files.write(
            "alice", project_id, "report.md", b"finished report"
        )
        delivered = await client.get(
            f"/manager/tasks/{sid}/files", params={"area": "artifacts"}
        )
        assert [item["path"] for item in delivered.json()["items"]] == ["report.md"]
        assert (
            await client.get(f"/manager/tasks/{isolated_sid}/files", params={"area": "artifacts"})
        ).json()["items"] == []
        files.write(
            "alice", project_id, "nested/summary.md", b"nested delivery"
        )
        nested = await client.get(
            f"/manager/tasks/{sid}/files", params={"area": "artifacts", "recursive": True}
        )
        assert [(item["path"], item["area"]) for item in nested.json()["items"]] == [
            ("nested/summary.md", "artifacts"),
            ("report.md", "artifacts"),
        ]
        assert all(not item["is_directory"] for item in nested.json()["items"])
        app.dependency_overrides[current_user] = lambda: bob
        assert (await client.get(f"/manager/tasks/{sid}/files")).status_code == 403
        assert (
            await client.post(
                f"/manager/tasks/{sid}/files", files={"file": ("denied.txt", b"no")}
            )
        ).status_code == 403
        assert not (changes.files.project_root("alice", project_id) / "denied.txt").exists()


@pytest.mark.asyncio
async def test_team_delete_rejects_member_run_then_cascades(storage, tmp_path):
    from egis_agentkit.extensions.message_bus import EgisMessageBus
    from egis_agentkit.service.team.definition import TeamService

    _, _, alice, _ = await identities(storage)
    member = await AgentService(storage).save(alice, expert())
    team = await TeamService(storage).save(
        alice,
        {
            "team_name": "writers",
            "team_display_name": "写作团",
            "orchestrator_config": {
                "agent_name": "coordinator",
                "agent_display_name": "主理人",
                "payload": {},
            },
            "members": [
                {"definition_id": member["id"], "resource_view_type": "private"}
            ],
        },
    )
    service = TaskService(storage, tmp_path)
    task = await service.create(alice, team["id"], "private")
    session = await service.session(alice, task["session_id"])
    native_team = await storage.get_team(alice.user_id, session.team_id)
    worker = native_team.data.members[0]
    from egis_agentkit.service.workspace.changes import FileChangeService

    changes = FileChangeService(storage, tmp_path)
    project_id = task["project_id"]
    await changes.observe(alice.user_id, project_id)
    for area in ("work", "artifacts", "uploads"):
        service.files.write(alice.user_id, project_id, "owned.txt", b"owned", area=area)
    await changes.observe(alice.user_id, project_id)
    history = await changes.list(project_id)
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    other = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus.migrate()
    key = MessageBusKeys.session_lock(worker.session_id)
    assert await other.try_lock(key)
    with pytest.raises(Conflict):
        await service.delete(alice, task["session_id"], bus)
    assert await storage.get_team(alice.user_id, session.team_id)
    assert all(
        service.files.area_root(alice.user_id, project_id, area).exists()
        for area in ("work", "artifacts", "uploads")
    )
    await other.unlock(key)
    await service.delete(alice, task["session_id"], bus)
    assert await storage.get_team(alice.user_id, session.team_id) is None
    assert (
        await storage.get_session(alice.user_id, worker.agent_id, worker.session_id)
        is None
    )
    assert all(
        service.files.read(alice.user_id, project_id, area, "owned.txt")[0] == b"owned"
        for area in ("work", "artifacts", "uploads")
    )
    assert await changes.list(project_id) == history
    await bus.aclose()
    await other.aclose()


@pytest.mark.asyncio
async def test_task_dto_reads_persisted_native_plan(storage, tmp_path):
    from agentscope.state import Task

    _, _, alice, _ = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    service = TaskService(storage, tmp_path)
    task = await service.create(alice, source["id"], "private")
    assert task["plan"] == []
    session = await service.session(alice, task["session_id"])
    session.state.tasks_context.tasks.append(
        Task(
            id="step-1",
            subject="Research",
            description="private detail",
            metadata={"secret": "hidden"},
            state="in_progress",
        )
    )
    await storage.update_session_state(
        alice.user_id, session.agent_id, session.id, session.state
    )
    assert (await service.get(alice, session.id))["plan"] == [
        {"id": "step-1", "subject": "Research", "state": "in_progress"}
    ]


@pytest.mark.asyncio
async def test_owner_can_delete_after_system_agent_grant_revoked(storage, tmp_path):
    from egis_agentkit.extensions.message_bus import EgisMessageBus
    from egis_agentkit.models import Grant
    from egis_agentkit.services import PermissionService

    users, admin, alice, bob = await identities(storage)
    source = await AgentService(storage).save(admin, expert("sys", "system"))
    permissions = PermissionService(storage)
    grant = await permissions.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=alice.user_id,
            resource_type="agent",
            resource_id=source["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="sys",
        ),
    )
    service = TaskService(storage, tmp_path)
    task = await service.create(alice, source["id"], "system")
    service.files.write(alice.user_id, task["project_id"], "keep.txt", b"keep")
    await permissions.revoke(admin, grant["id"])
    bus = EgisMessageBus()
    with pytest.raises(Forbidden):
        await service.get(alice, task["session_id"])
    with pytest.raises(Forbidden):
        await service.delete(bob, task["session_id"], bus)
    await users.update_user(admin, alice.model_copy(update={"is_enabled": False}))
    with pytest.raises(Forbidden):
        await service.delete(alice, task["session_id"], bus)
    await users.update_user(admin, alice)
    await service.delete(alice, task["session_id"], bus)
    assert service.files.read(
        alice.user_id, task["project_id"], "work", "keep.txt"
    )[0] == b"keep"
    assert (
        await storage.get_session(alice.user_id, task["agent_id"], task["session_id"])
        is None
    )
