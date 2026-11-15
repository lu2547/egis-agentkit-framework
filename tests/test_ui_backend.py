import pytest
from agentscope.app.message_bus import MessageBusKeys
from test_domain import expert, identities

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service._session import SessionService
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.storage.project._files import ProjectFileStorage


@pytest.mark.asyncio
async def test_task_naming_and_delete_keeps_project_files(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    service = SessionService(storage, tmp_path)
    task = await service.add_session(alice, source["id"], "private")
    explicit = await service.add_session(alice, source["id"], "private", name="My title")
    record = await service.get_session_record(alice, task["session_id"])
    named = await service.get_session_record(alice, explicit["session_id"])
    assert record.config.naming.auto is True
    assert named.config.naming.auto is False and named.config.name == "My title"
    service.files.write("alice", task["project_id"], "keep.txt", b"keep")
    with pytest.raises(Forbidden):
        await service.delete_session(bob, task["session_id"], None)

    from agentscope.app.message_bus import InMemoryMessageBus

    await service.delete_session(alice, task["session_id"], InMemoryMessageBus())
    assert (
        service.files.read("alice", task["project_id"], "work", "keep.txt")[0]
        == b"keep"
    )
    assert len(await service.list_sessions(alice)) == 1


@pytest.mark.asyncio
async def test_workspace_http_routes_and_project_upload_persist(storage, tmp_path):
    import httpx
    from egis_agentkit_app.bootstrap._router import manager_router as router
    from egis_agentkit_app.controller.base import current_user
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    from egis_agentkit.errors import AgentKitError
    from egis_agentkit.service.project._project_manager import ProjectManager

    _, _, alice, bob = await identities(storage)
    projects = ProjectManager(storage, tmp_path)
    tasks = SessionService(storage, tmp_path)
    project_id = (await projects.create(alice, "Main"))["id"]
    isolated_id = (await projects.create(alice, "Isolated"))["id"]
    app = FastAPI()
    app.include_router(router)

    @app.exception_handler(AgentKitError)
    async def domain_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    from egis_agentkit.service.project._file_manager import ProjectFileManager

    changes = ProjectFileManager(storage, tmp_path)
    from types import SimpleNamespace

    from egis_agentkit.service.project._document_preview import DocumentPreviewService
    from egis_agentkit.service.project._file_manager import ProjectFileManager

    files = ProjectFileStorage(tmp_path)
    app.state.egis_services = SimpleNamespace(
        project_files=ProjectFileManager(
            storage, tmp_path, files=files, projects=projects, sessions=tasks, preview=DocumentPreviewService()
        )
    )
    app.dependency_overrides[current_user] = lambda: alice
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        project_upload = await client.post(
            f"/manager/projects/{project_id}/files",
            files={"file": ("task.txt", b"original")},
        )
        assert project_upload.status_code == 201
        history = await client.get(f"/manager/projects/{project_id}/file-changes")
        assert history.status_code == 200
        assert [
            (item["path"], item["change_type"]) for item in history.json()["items"]
        ] == [("task.txt", "created")]
        assert history.json()["items"][0]["area"] == "uploads"
        assert project_upload.json()["area"] == "uploads"
        assert (
            await client.get(
                f"/manager/projects/{project_id}/file",
                params={"area": "uploads", "path": "task.txt"},
            )
        ).content == b"original"
        assert (
            await client.get(
                f"/manager/projects/{project_id}/files", params={"area": "work"}
            )
        ).json()["items"] == []
        assert (
            await client.get(
                f"/manager/projects/{isolated_id}/files", params={"area": "uploads"}
            )
        ).json()["items"] == []
        files.write("alice", project_id, "report.md", b"finished report")
        delivered = await client.get(
            f"/manager/projects/{project_id}/files", params={"area": "artifacts"}
        )
        assert [item["path"] for item in delivered.json()["items"]] == ["report.md"]
        assert (
            await client.get(
                f"/manager/projects/{isolated_id}/files", params={"area": "artifacts"}
            )
        ).json()["items"] == []
        files.write("alice", project_id, "nested/summary.md", b"nested delivery")
        nested = await client.get(
            f"/manager/projects/{project_id}/files",
            params={"area": "artifacts", "recursive": True},
        )
        assert [(item["path"], item["area"]) for item in nested.json()["items"]] == [
            ("nested/summary.md", "artifacts"),
            ("report.md", "artifacts"),
        ]
        assert all(not item["is_directory"] for item in nested.json()["items"])
        app.dependency_overrides[current_user] = lambda: bob
        assert (
            await client.get(f"/manager/projects/{project_id}/files")
        ).status_code == 404
        assert (
            await client.post(
                f"/manager/projects/{project_id}/files",
                files={"file": ("denied.txt", b"no")},
            )
        ).status_code == 404
        assert not (
            changes.files.project_root("alice", project_id) / "denied.txt"
        ).exists()


@pytest.mark.asyncio
async def test_team_delete_rejects_member_run_then_cascades(storage, tmp_path):
    from egis_agentkit.core.message_bus import EgisMessageBus
    from egis_agentkit.service.team._definition import TeamDefinition

    _, _, alice, _ = await identities(storage)
    member = await AgentManager(storage).add_definition(alice, expert())
    team = await TeamDefinition(storage).save(
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
    service = SessionService(storage, tmp_path)
    task = await service.add_session(alice, team["id"], "private")
    session = await service.get_session_record(alice, task["session_id"])
    native_team = await storage.get_team(alice.user_id, session.team_id)
    worker = native_team.data.members[0]
    from egis_agentkit.service.project._file_manager import ProjectFileManager

    changes = ProjectFileManager(storage, tmp_path)
    project_id = task["project_id"]
    await changes.observe(alice.user_id, project_id)
    for area in ("work", "artifacts", "uploads"):
        service.files.write(alice.user_id, project_id, "owned.txt", b"owned", area=area)
    await changes.observe(alice.user_id, project_id)
    history = await changes.list_changes(project_id)
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    other = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus.init_schema()
    key = MessageBusKeys.session_lock(worker.session_id)
    assert await other.try_lock(key)
    with pytest.raises(Conflict):
        await service.delete_session(alice, task["session_id"], bus)
    assert await storage.get_team(alice.user_id, session.team_id)
    assert all(
        service.files.area_root(alice.user_id, project_id, area).exists()
        for area in ("work", "artifacts", "uploads")
    )
    await other.unlock(key)
    await service.delete_session(alice, task["session_id"], bus)
    assert await storage.get_team(alice.user_id, session.team_id) is None
    assert (
        await storage.get_session(alice.user_id, worker.agent_id, worker.session_id)
        is None
    )
    assert all(
        service.files.read(alice.user_id, project_id, area, "owned.txt")[0] == b"owned"
        for area in ("work", "artifacts", "uploads")
    )
    assert await changes.list_changes(project_id) == history
    await bus.aclose()
    await other.aclose()


@pytest.mark.asyncio
async def test_task_dto_reads_persisted_native_plan(storage, tmp_path):
    from agentscope.state import Task

    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    service = SessionService(storage, tmp_path)
    task = await service.add_session(alice, source["id"], "private")
    assert task["plan"] == []
    session = await service.get_session_record(alice, task["session_id"])
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
    assert (await service.get_session(alice, session.id))["plan"] == [
        {"id": "step-1", "subject": "Research", "state": "in_progress"}
    ]


@pytest.mark.asyncio
async def test_owner_can_delete_after_system_agent_grant_revoked(storage, tmp_path):
    from egis_agentkit.core.message_bus import EgisMessageBus
    from egis_agentkit.storage.model import Grant

    users, admin, alice, bob = await identities(storage)
    source = await AgentManager(storage).add_definition(admin, expert("sys", "system"))
    grant = await GrantService(storage).grant(
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
    service = SessionService(storage, tmp_path)
    task = await service.add_session(alice, source["id"], "system")
    service.files.write(alice.user_id, task["project_id"], "keep.txt", b"keep")
    await GrantService(storage).revoke(admin, grant["id"])
    bus = EgisMessageBus()
    with pytest.raises(Forbidden):
        await service.get_session(alice, task["session_id"])
    with pytest.raises(Forbidden):
        await service.delete_session(bob, task["session_id"], bus)
    await users.update_user(admin, alice.model_copy(update={"is_enabled": False}))
    with pytest.raises(Forbidden):
        await service.delete_session(alice, task["session_id"], bus)
    await users.update_user(admin, alice)
    await service.delete_session(alice, task["session_id"], bus)
    assert (
        service.files.read(alice.user_id, task["project_id"], "work", "keep.txt")[0]
        == b"keep"
    )
    assert (
        await storage.get_session(alice.user_id, task["agent_id"], task["session_id"])
        is None
    )
