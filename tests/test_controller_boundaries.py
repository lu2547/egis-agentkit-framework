"""Pin the manager wire contract and the App/Framework responsibility boundary."""

import ast
import hashlib
import json
from importlib import import_module, util
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from egis_agentkit_app import manager_router
from egis_agentkit_app.security import current_user
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from test_domain import expert, identities
from test_workspace_service import make_workspace

from egis_agentkit.errors import AgentKitError, Forbidden, NotFound
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.access.sharing import ShareService
from egis_agentkit.service.agent.definition import AgentService
from egis_agentkit.service.team.definition import TeamService


def make_details(storage):
    module = "egis_agentkit.service.agent.detail"
    assert util.find_spec(module) is not None, "Definition detail authorization belongs in Framework"
    return import_module(module).DefinitionDetailService(
        storage, AgentService(storage), PermissionService(storage)
    )


def test_manager_openapi_is_unchanged():
    app = FastAPI()
    app.include_router(manager_router.router)
    # Contract includes the async Skill imports and per-reply artifacts APIs. Includes every method, status, tag,
    # operation ID, parameter, multipart field and request/response schema.
    digest = hashlib.sha256(json.dumps(app.openapi(), sort_keys=True, ensure_ascii=False).encode()).hexdigest()
    assert digest == "ebecc598cf8640371a5ac97dd24095b50ac39fb7634fea50399acf9ed3b2d78d"
    assert sum(len(operations) for operations in app.openapi()["paths"].values()) == 80


def test_controllers_do_not_own_storage_file_io_or_other_controllers():
    root = Path(manager_router.__file__).parent
    manager = ast.parse((root / "manager_router.py").read_text())
    assert not any(isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)) for node in ast.walk(manager)), "manager_router must only aggregate routers"
    domains = {"agent", "team", "access", "model", "session", "project", "workspace", "resource", "user"}
    for domain in domains:
        path = root / "controllers" / f"{domain}.py"
        assert path.is_file(), f"missing {domain} controller"
        tree = ast.parse(path.read_text())
        assert any(isinstance(node, ast.ImportFrom) and node.module == "dependencies" for node in ast.walk(tree))
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert node.module not in domains - {domain}, "controllers must not call one another"
                assert "storage" not in (node.module or "").split(".")
            if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Call):
                assert not (isinstance(node.value.func, ast.Name) and node.value.func.id == "services"), "use named service attributes"
            if isinstance(node, ast.Attribute):
                assert node.attr not in {"storage", "_parts", "guard", "locked", "observe_safe", "project_root", "user_root", "area_root", "read_user", "write_user"}
    def endpoints(router):
        for route in router.routes:
            if hasattr(route, "original_router"):
                yield from endpoints(route.original_router)
            else:
                yield route.endpoint

    for endpoint in endpoints(manager_router.router):
        assert endpoint.__module__.startswith("egis_agentkit_app.controllers.")


@pytest.mark.asyncio
async def test_definition_detail_separates_editability_from_use_and_rejects_wrong_type(storage):
    _, admin, alice, bob = await identities(storage)
    details = make_details(storage)
    agents = AgentService(storage)
    private = await agents.save(alice, expert())
    own = await details.get(alice, private["id"], "private", "agent")
    assert own["is_editable"] and own["is_deletable"] and own["is_usable"]
    with pytest.raises(Forbidden):
        await details.get(bob, private["id"], "private", "agent")
    with pytest.raises(NotFound):
        await details.get(alice, private["id"], "private", "team")
    with pytest.raises(NotFound):
        await details.get(alice, "missing", "private", "agent")
    system = await agents.save(admin, expert("egis-demo", "system", "system_writer"))
    visible = await details.get(alice, system["id"], "system", "agent")
    assert not visible["is_editable"] and not visible["is_deletable"] and not visible["is_usable"]
    admin_view = await details.get(admin, system["id"], "system", "agent")
    assert admin_view["is_editable"] and not admin_view["is_usable"]
    with pytest.raises(Forbidden):
        await details.get(bob, private["id"], "shared", "agent")
    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", private["id"])
    await shares.decide(admin, review["review_id"], True, "approved")
    shared = await details.get(bob, private["id"], "shared", "agent")
    assert shared["resource_view_type"] == "shared"
    assert shared["is_usable"] and not shared["is_editable"]


@pytest.mark.asyncio
async def test_team_detail_only_exposes_orchestrator_configuration_to_editors(storage):
    _, admin, alice, _ = await identities(storage)
    details = make_details(storage)
    member = await AgentService(storage).save(admin, expert("egis-demo", "system"))
    team = await TeamService(storage).save(admin, {
        "owner_type": "system", "owner_id": "egis-demo",
        "team_name": "writers", "team_display_name": "Writers",
        "orchestrator_config": {"agent_name": "coordinator", "agent_display_name": "Coordinator", "payload": {}},
        "members": [{"definition_id": member["id"], "resource_view_type": "system"}],
    })
    own = await details.get(admin, team["id"], "system", "team")
    assert set(own["orchestrator_config"]) == {"agent_name", "agent_display_name", "payload", "config_version"}
    assert own["orchestrator_config"]["agent_name"] == "coordinator"
    visible = await details.get(alice, team["id"], "system", "team")
    assert "orchestrator_config" not in visible
    assert not visible["is_usable"]


@pytest.mark.asyncio
async def test_controllers_use_named_services_and_preserve_file_http_contract(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    svc = make_workspace(storage, tmp_path, max_bytes=32)
    details = make_details(storage)
    agents = AgentService(storage)
    definition = await agents.save(alice, expert())
    task = await svc.tasks.create(alice, definition["id"], "private")
    sid, pid = task["session_id"], task["project_id"]
    app = FastAPI()
    # No dict access, raw files or storage are offered to the controllers.
    app.state.egis_services = SimpleNamespace(workspace=svc.workspace, definition_details=details)
    app.dependency_overrides[current_user] = lambda: alice
    app.include_router(manager_router.router)

    @app.exception_handler(AgentKitError)
    async def domain_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
        assert (await client.get(f"/manager/agents/{definition['id']}")).json()["is_editable"]
        assert (await client.get(f"/manager/teams/{definition['id']}")).status_code == 404
        for base in (f"/manager/projects/{pid}", f"/manager/tasks/{sid}"):
            uploaded = await client.post(base + "/files", params={"area": "work", "path": "folder"}, files={"file": ("报告.txt", b"report")})
            assert uploaded.status_code == 201, uploaded.text
            row = uploaded.json()
            assert row["area"] == "uploads" and row["path"].startswith("folder/报告")
            downloaded = await client.get(base + "/file", params={"area": "uploads", "path": row["path"]})
            assert downloaded.content == b"report"
            assert downloaded.headers["x-content-type-options"] == "nosniff"
            assert downloaded.headers["cache-control"] == "private, no-store"
            assert downloaded.headers["content-security-policy"].startswith("sandbox;")
            assert downloaded.headers["content-disposition"].startswith("inline; filename*=UTF-8''%E6%8A%A5")
            listing = await client.get(base + "/files", params={"recursive": True})
            assert row["path"] in {item["path"] for item in listing.json()["items"]}
            assert (await client.get(base + "/file-changes", params={"limit": 0})).status_code == 422
            changes = await client.get(base + "/file-changes", params={"limit": 1})
            assert len(changes.json()["items"]) == 1
            assert (await client.post(base + "/files", files={"file": ("large", b"x" * 33)})).status_code == 409
            svc.files.write(alice.user_id, pid, "delivery.txt", b"result")
            delivery = await client.post(base + "/deliver", json={"path": "delivery.txt"})
            assert delivery.status_code == 201 and delivery.json()["area"] == "artifacts"
        uploaded = await client.post("/manager/workspace/files", params={"path": "query"}, data={"path": "form"}, files={"file": ("input.txt", b"input")})
        assert uploaded.status_code == 201 and uploaded.json()["path"] == "uploads/query/input.txt"
        form = await client.post("/manager/workspace/files", data={"path": "form"}, files={"file": ("input.txt", b"input")})
        assert form.json()["path"] == "uploads/form/input.txt"
        assert (await client.get("/manager/workspace/file", params={"path": form.json()["path"]})).content == b"input"
        assert (await client.get("/manager/workspace/files", params={"path": "uploads/form"})).json()["items"][0]["name"] == "input.txt"
        assert (await client.post("/manager/workspace/files", params={"path": "work"}, files={"file": ("input.txt", b"input")})).status_code == 403
        app.dependency_overrides[current_user] = lambda: bob
        assert (await client.get(f"/manager/tasks/{sid}/files")).status_code == 403
        assert (await client.get(f"/manager/projects/{pid}/files")).status_code == 404
