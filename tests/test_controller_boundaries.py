"""Pin the manager wire contract and the App/Framework responsibility boundary."""

import ast
from pathlib import Path
from types import SimpleNamespace

import httpx
import pytest
from egis_agentkit_app.bootstrap import _router as manager_router
from egis_agentkit_app.controller.base import current_user
from fastapi import FastAPI
from fastapi.responses import JSONResponse
from test_domain import COMPANY, agent_changes, expert, identities
from test_workspace_service import make_workspace

from egis_agentkit.errors import AgentKitError, Forbidden, NotFound
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.storage.model import Grant


def test_manager_openapi_carries_optional_recipient_in_share_requests():
    app = FastAPI()
    app.include_router(manager_router.router)
    spec = app.openapi()
    paths = spec["paths"]
    assert set(paths["/manager/share-requests"]) == {"post", "get"}
    assert set(paths["/manager/share-recipients"]) == {"get"}
    assert set(paths["/manager/admin/grants"]) == {"post", "get"}
    assert set(paths["/manager/admin/grant-resources"]) == {"get"}
    assert set(paths["/manager/admin/grants/{identifier}"]) == {"delete"}
    schemas = spec["components"]["schemas"]
    assert set(schemas["ShareWrite"]["required"]) == {
        "resource_type",
        "resource_id",
    }
    assert set(schemas["ShareWrite"]["properties"]) == {
        "resource_type",
        "resource_id",
        "target",
    }
    assert set(schemas["DecisionWrite"]["properties"]) == {"is_approved", "reason"}


def test_manager_openapi_pins_version_and_excludes_tools_from_access_writes():
    app = FastAPI()
    app.include_router(manager_router.router)
    spec = app.openapi()
    paths = spec["paths"]
    for collection in ("agents", "teams"):
        params = paths[f"/manager/{collection}/{{identifier}}"]["get"]["parameters"]
        version = next(
            (param for param in params if param["name"] == "resource_version"), None
        )
        assert version is not None and not version["required"]
        assert set(paths[f"/manager/{collection}/{{identifier}}/versions"]) == {"get"}
    assert "/manager/tools/{identifier}/install" not in paths
    assert "/manager/tools/{identifier}/installation" not in paths

    schemas = spec["components"]["schemas"]
    assert "resource_version" not in schemas["GrantWrite"]["properties"]
    for schema in ("ShareWrite", "GrantWrite"):
        properties = schemas[schema]["properties"]
        assert set(properties["resource_type"]["enum"]) == {
            "agent",
            "team",
            "skill",
            "mcp",
        }
    assert set(schemas["ShareTarget"]["properties"]["principal_type"]["enum"]) == {
        "user",
        "group",
    }
    assert set(schemas["GrantWrite"]["properties"]["principal_type"]["enum"]) == {
        "user",
        "group",
    }


def test_controllers_do_not_own_storage_file_io_or_other_controllers():
    root = Path(manager_router.__file__).parent.parent
    manager = ast.parse(Path(manager_router.__file__).read_text())
    assert not any(
        isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef))
        for node in ast.walk(manager)
    ), "manager_router must only aggregate routers"
    domains = {
        "agent",
        "team",
        "access",
        "model",
        "session",
        "project",
        "resource",
        "user",
    }
    for domain in domains:
        package = root / "controller" / domain
        assert (package / "__init__.py").is_file(), f"missing {domain} controller"
    for path in (root / "controller").rglob("_*.py"):
        if path.name in {"__init__.py", "_model.py"} or path.parent.name == "base":
            continue
        tree = ast.parse(path.read_text())
        if path.parent.name != "health":
            assert any(
                isinstance(node, ast.ImportFrom) and node.module == "base"
                for node in ast.walk(tree)
            ) or path.parent.name == "remote_agent"
        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom):
                assert node.module not in domains - {path.parent.name}, (
                    "controllers must not call one another"
                )
                assert "bootstrap" not in (node.module or "").split(".")
                if "storage" in (node.module or "").split("."):
                    assert node.module == "egis_agentkit.storage.model"
            if isinstance(node, ast.Subscript) and isinstance(node.value, ast.Call):
                assert not (
                    isinstance(node.value.func, ast.Name)
                    and node.value.func.id == "services"
                ), "use named service attributes"
            if isinstance(node, ast.Attribute):
                assert node.attr not in {
                    "storage",
                    "_parts",
                    "guard",
                    "locked",
                    "observe_safe",
                    "project_root",
                    "user_root",
                    "area_root",
                }

    def endpoints(router):
        for route in router.routes:
            if hasattr(route, "original_router"):
                yield from endpoints(route.original_router)
            else:
                yield route.endpoint

    for endpoint in endpoints(manager_router.router):
        assert endpoint.__module__.startswith("egis_agentkit_app.controller.")


@pytest.mark.asyncio
async def test_definition_detail_separates_editability_from_use_and_rejects_wrong_type(
    storage,
):
    _, admin, alice, bob = await identities(storage)
    agents = AgentManager(storage)
    queries = AgentManager(storage)
    private = await agents.add_definition(alice, expert())
    own = await queries.get_definition(alice, private["id"], "private")
    assert own["is_editable"] and own["is_deletable"] and own["is_usable"]
    with pytest.raises(Forbidden):
        await queries.get_definition(bob, private["id"], "private")
    with pytest.raises(NotFound):
        await TeamDefinition(storage).read(alice, private["id"], "private")
    with pytest.raises(NotFound):
        await queries.get_definition(alice, "missing", "private")
    system = await agents.add_definition(admin, expert("egis-demo", "system", "system_writer"))
    with pytest.raises(Forbidden):
        await queries.get_definition(alice, system["id"], "system")
    admin_view = await queries.get_definition(admin, system["id"], "system")
    assert admin_view["is_editable"] and not admin_view["is_usable"]
    with pytest.raises(Forbidden):
        await queries.get_definition(bob, private["id"], "shared")
    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", private["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "approved")
    shared = await queries.get_definition(bob, private["id"], "shared")
    assert shared["resource_view_type"] == "shared"
    assert shared["is_usable"] and not shared["is_editable"]


@pytest.mark.asyncio
async def test_team_detail_only_exposes_orchestrator_configuration_to_editors(storage):
    _, admin, alice, _ = await identities(storage)
    member = await AgentManager(storage).add_definition(admin, expert("egis-demo", "system"))
    teams = TeamDefinition(storage)
    team = await teams.save(
        admin,
        {
            "owner_type": "system",
            "owner_id": "egis-demo",
            "team_name": "writers",
            "team_display_name": "Writers",
            "orchestrator_config": {
                "agent_name": "coordinator",
                "agent_display_name": "Coordinator",
                "payload": {},
            },
            "members": [
                {"definition_id": member["id"], "resource_view_type": "system"}
            ],
        },
    )
    own = await teams.read(admin, team["id"], "system")
    assert set(own["orchestrator_config"]) == {
        "agent_name",
        "agent_display_name",
        "payload",
        "owner_version",
    }
    assert own["orchestrator_config"]["agent_name"] == "coordinator"
    with pytest.raises(Forbidden):
        await teams.read(alice, team["id"], "system")
    await GrantService(storage).grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=alice.user_id,
            resource_type="team",
            resource_id=team["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="egis-demo",
        ),
    )
    visible = await teams.read(alice, team["id"], "system", version="1")
    assert visible["resource_version"] == "1" and visible["is_usable"]
    assert "orchestrator_config" not in visible


@pytest.mark.asyncio
async def test_team_list_and_delete_use_team_service():
    calls = []
    user = object()

    class Teams:
        async def list(self, actor):
            calls.append(("list", actor))
            return [{"id": "team-one"}]

        async def delete(self, actor, identifier):
            calls.append(("delete", actor, identifier))

    app = FastAPI()
    app.state.egis_services = SimpleNamespace(teams=Teams())
    app.dependency_overrides[current_user] = lambda: user
    app.include_router(manager_router.router)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        assert (await client.get("/manager/teams")).json() == {
            "items": [{"id": "team-one"}]
        }
        assert (await client.delete("/manager/teams/team-one")).status_code == 204
    assert calls == [("list", user), ("delete", user, "team-one")]


@pytest.mark.asyncio
async def test_controllers_use_named_services_and_preserve_file_http_contract(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    svc = make_workspace(storage, tmp_path, max_bytes=32)
    agents = AgentManager(storage)
    definition = await agents.add_definition(alice, expert())
    task = await svc.tasks.add_session(alice, definition["id"], "private")
    sid, pid = task["session_id"], task["project_id"]
    app = FastAPI()
    # No dict access, raw files or storage are offered to the controllers.
    app.state.egis_services = SimpleNamespace(
        project_files=svc.workspace,
        agents=AgentManager(storage),
        personal=PersonalManager(storage),
        teams=TeamDefinition(storage),
    )
    app.dependency_overrides[current_user] = lambda: alice
    app.include_router(manager_router.router)

    @app.exception_handler(AgentKitError)
    async def domain_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        expert_detail = f"/manager/agents/{definition['id']}"
        await agents.update_definition(alice, (expert().model_copy(
                update={
                    "id": definition["id"],
                    "agent_display_name": "新版专家",
                }
            )).id, agent_changes(expert().model_copy(
                update={
                    "id": definition["id"],
                    "agent_display_name": "新版专家",
                }
            )))
        assert (await client.get(expert_detail)).json()["is_editable"]
        historical = (
            await client.get(expert_detail, params={"resource_version": "1"})
        ).json()
        assert historical["agent_display_name"] == "写作专家"
        assert historical["resource_version"] == "1"
        assert not historical["is_editable"]
        assert (await client.get(expert_detail + "/versions")).json()["items"] == [
            {"resource_version": "2", "resource_view_type": "private"},
            {"resource_version": "1", "resource_view_type": "private"},
        ]
        app.dependency_overrides[current_user] = lambda: bob
        assert (await client.get(expert_detail)).status_code == 403
        assert (
            await client.get(expert_detail, params={"view": "unknown"})
        ).status_code == 422
        app.dependency_overrides[current_user] = lambda: alice
        assert (
            await client.get(f"/manager/teams/{definition['id']}")
        ).status_code == 404
        base = f"/manager/projects/{pid}"
        uploaded = await client.post(
            base + "/files",
            params={"path": "folder"},
            files={"file": ("报告.txt", b"report")},
        )
        assert uploaded.status_code == 201, uploaded.text
        row = uploaded.json()
        assert row["area"] == "uploads" and row["path"].startswith("folder/报告")
        downloaded = await client.get(
            base + "/file", params={"area": "uploads", "path": row["path"]}
        )
        assert downloaded.content == b"report"
        assert downloaded.headers["x-content-type-options"] == "nosniff"
        assert downloaded.headers["cache-control"] == "private, no-store"
        assert downloaded.headers["content-security-policy"].startswith("sandbox;")
        assert downloaded.headers["content-disposition"].startswith(
            "inline; filename*=UTF-8''%E6%8A%A5"
        )
        listing = await client.get(base + "/files", params={"recursive": True})
        assert row["path"] in {item["path"] for item in listing.json()["items"]}
        assert (
            await client.get(base + "/file-changes", params={"limit": 0})
        ).status_code == 422
        changes = await client.get(base + "/file-changes", params={"limit": 1})
        assert len(changes.json()["items"]) == 1
        assert (
            await client.post(base + "/files", files={"file": ("large", b"x" * 33)})
        ).status_code == 409
        svc.files.write(alice.user_id, pid, "delivery.txt", b"result")
        delivery = await client.post(base + "/deliver", json={"path": "delivery.txt"})
        assert delivery.status_code == 201 and delivery.json()["area"] == "artifacts"
        duplicate = await client.post(
            base + "/files",
            params={"path": "folder"},
            files={"file": ("报告.txt", b"updated")},
        )
        assert duplicate.status_code == 409
        overwritten = await client.post(
            base + "/files",
            params={"path": "folder", "overwrite": True},
            files={"file": ("报告.txt", b"updated")},
        )
        assert overwritten.status_code == 201
        assert (
            await client.get(
                base + "/file", params={"area": "uploads", "path": row["path"]}
            )
        ).content == b"updated"
        assert (await client.get(f"/manager/tasks/{sid}/files")).status_code == 404
        assert (await client.get("/manager/workspace/files")).status_code == 404
        app.dependency_overrides[current_user] = lambda: bob
        assert (await client.get(f"/manager/projects/{pid}/files")).status_code == 404


@pytest.mark.asyncio
async def test_access_http_requires_recipient_and_authorizes_admin_actions(storage):
    _, admin, alice, bob = await identities(storage)
    definition = await AgentManager(storage).add_definition(alice, expert())
    app = FastAPI()
    app.state.egis_services = SimpleNamespace(
        sharing=ShareService(storage), grants=GrantService(storage)
    )
    actor = alice
    app.dependency_overrides[current_user] = lambda: actor
    app.include_router(manager_router.router)

    @app.exception_handler(AgentKitError)
    async def domain_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        root = {
            "resource_type": "agent",
            "resource_id": definition["id"],
        }
        assert (
            await client.post("/manager/share-requests", json=root)
        ).status_code == 409
        targets = (await client.get("/manager/share-recipients")).json()["items"]
        assert {
            "principal_type": "group",
            "principal_id": "company",
            "display_name": "全公司",
        } in targets
        assert all("role_type" not in item for item in targets)
        selected = {"principal_type": "user", "principal_id": bob.user_id}
        submitted = await client.post(
            "/manager/share-requests", json={**root, "target": selected}
        )
        assert submitted.status_code == 201
        assert submitted.json()["resource_version"] == "1"
        assert submitted.json()["review_type"] == "share"
        assert "snapshot" not in submitted.json()
        review_id = submitted.json()["review_id"]
        grant = {
            **root,
            **selected,
            "resource_view_type": "shared",
            "resource_owner_type": "user",
            "resource_owner_id": alice.user_id,
        }
        decision = f"/manager/admin/share-reviews/{review_id}/decision"
        assert (await client.get("/manager/admin/grants")).status_code == 403
        assert (await client.get("/manager/admin/grant-resources")).status_code == 403
        assert (
            await client.post("/manager/admin/grants", json=grant)
        ).status_code == 403
        assert (
            await client.post(decision, json={"is_approved": True})
        ).status_code == 403

        actor = admin
        resources = (await client.get("/manager/admin/grant-resources")).json()["items"]
        assert [
            row["resource_id"] for row in resources if row["resource_type"] == "agent"
        ] == [definition["id"]]
        assert (
            await client.post(
                decision,
                json={
                    "is_approved": True,
                    "principal_type": "all_users",
                    "principal_id": "*",
                },
            )
        ).status_code == 422
        assert (
            await client.post(decision, json={"is_approved": True})
        ).status_code == 200
        assert (
            await client.post("/manager/admin/grants", json={**grant, "id": "chosen"})
        ).status_code == 422
        response = await client.post("/manager/admin/grants", json=grant)
        assert response.status_code == 200
        assert response.json()["granted_by"] == admin.user_id
        assert "resource_version" not in response.json()
        items = (await client.get("/manager/admin/grants")).json()["items"]
        assert {item["grant_source_type"] for item in items} == {
            "manual",
            "owner",
            "share",
        }
