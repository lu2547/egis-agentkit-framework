import asyncio

import pytest
from cryptography.fernet import Fernet

from egis_agentkit.client.nacos._settings import AccessConfig
from egis_agentkit.errors import AgentKitError, Forbidden, NotFound
from egis_agentkit.service._user import UserService
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage._schema import upgrade_schema
from egis_agentkit.storage.model import Asset
from egis_agentkit.storage.model._resource import MCP_STORAGE_VERSION

CONFIG = {
    "resource_id": "reports",
    "config": {
        "transport": "streamable_http",
        "url": "https://reports.example/mcp",
        "auth_type": "api_key",
    },
}


async def test_first_login_joins_company_without_restoring_removed_membership(storage):
    users = UserService(storage, AccessConfig(company_group_id="corporate"))
    admin = await users.sync_identity("admin", "Admin")
    await users.provision_admin(admin.user_id, actor="test")
    admin = await users.get(admin.user_id)
    alice = await users.sync_identity("alice", "Alice")
    assert alice.group_ids == ["corporate"]
    await users.update_user(admin, alice.model_copy(update={"group_ids": []}))
    assert not (await users.sync_identity("alice", "New name")).group_ids


async def test_managed_connector_keeps_one_configuration_and_gates_installation(
    storage, tmp_path
):
    users = UserService(storage)
    admin = await users.sync_identity("admin", "Admin")
    await users.provision_admin(admin.user_id, actor="test")
    admin = await users.get(admin.user_id)
    alice = await users.sync_identity("alice", "Alice")
    resources = ResourceService(storage, tmp_path, credential_key=Fernet.generate_key())
    with pytest.raises(Forbidden):
        await resources.save_mcp(alice, CONFIG)

    await resources.save_mcp(admin, CONFIG)
    (card,) = await resources.list(alice, "mcp")
    assert not card["is_authorized"]
    with pytest.raises(Forbidden):
        await resources.install(alice, "mcp", "reports")

    await resources.mcp_auth_action(alice, "reports", "set_api_key", key="test-key")
    assert (await resources.install(alice, "mcp", "reports"))["is_installed"]
    await resources.remove_installation(alice, "mcp", "reports")
    assert (await resources.mcp_auth_action(alice, "reports", "status"))["authorized"]

    await asyncio.gather(
        *(
            resources.save_mcp(admin, {**CONFIG, "display_name": f"Reports {i}"})
            for i in range(3)
        )
    )
    (asset,) = await storage.assets.list_versions("mcp")
    assert asset["publication_revision"] == 4
    assert asset["owner_id"] == "system"
    assert (await resources.mcp_auth_action(alice, "reports", "status"))["authorized"]

    await resources.save_mcp(
        admin,
        {**CONFIG, "config": {**CONFIG["config"], "url": "https://new.example/mcp"}},
    )
    assert not (await resources.mcp_auth_action(alice, "reports", "status"))[
        "authorized"
    ]
    await resources.set_asset_enabled(admin, "mcp", "reports", None, False, admin=True)
    assert await resources.list(alice, "mcp") == []
    await resources.set_asset_enabled(admin, "mcp", "reports", None, True, admin=True)
    assert len(await resources.list(alice, "mcp")) == 1
    await users.update_user(admin, alice.model_copy(update={"group_ids": []}))
    assert await resources.list(await users.get("alice"), "mcp") == []


async def test_upgrade_disables_legacy_connectors_and_keeps_reviewed_configuration(
    storage, tmp_path
):
    users = UserService(storage)
    admin = await users.sync_identity("admin", "Admin")
    await users.provision_admin(admin.user_id, actor="test")
    admin = await users.get(admin.user_id)
    resources = ResourceService(storage, tmp_path)
    legacy_rows = [
        ("reports", MCP_STORAGE_VERSION, 1, "system", "system"),
        ("reports", "1.1.0", 2, "system", "system"),
        ("private", MCP_STORAGE_VERSION, 1, "user", "alice"),
        ("legacy-singleton", MCP_STORAGE_VERSION, 1, "system", "system"),
    ]
    async with storage.definition_write() as write:
        for identifier, version, revision, owner_type, owner_id in legacy_rows:
            await write.assets.insert_asset(
                Asset(
                    resource_type="mcp",
                    resource_id=identifier,
                    resource_version=version,
                    publication_revision=revision,
                    owner_type=owner_type,
                    owner_id=owner_id,
                    payload={"config": CONFIG["config"]},
                ),
                actor="fixture",
            )

    preview = await upgrade_schema(storage.engine)
    assert preview.tables["MCP 历史配置"] == "pending"
    assert all(row["is_enabled"] for row in await storage.assets.list_versions("mcp"))

    result = await upgrade_schema(storage.engine, database=preview.database, apply=True)
    assert result.tables["MCP 历史配置"] == "upgraded"
    archived = await storage.assets.list_versions("mcp")
    assert len(archived) == len(legacy_rows)
    assert all(not row["is_enabled"] for row in archived)
    assert (await storage.assets.get("mcp", "private", MCP_STORAGE_VERSION))[
        "owner_id"
    ] == "alice"

    with pytest.raises(NotFound):
        await resources.set_asset_enabled(admin, "mcp", "reports", None, True, admin=True)
    await resources.save_mcp(admin, CONFIG)
    await resources.set_asset_enabled(admin, "mcp", "reports", None, True, admin=True)
    assert (await upgrade_schema(storage.engine)).tables["MCP 历史配置"] == "current"
    (card,) = await resources.admin_list(admin, "mcp")
    assert card["publication_revision"] == 3


async def test_connector_routes_use_stable_ids_and_enforce_company_access(
    storage, tmp_path
):
    from types import SimpleNamespace

    import httpx
    from egis_agentkit_app.bootstrap._router import manager_router
    from egis_agentkit_app.controller.base import current_user
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    users = UserService(storage)
    admin = await users.sync_identity("admin", "Admin")
    await users.provision_admin(admin.user_id, actor="test")
    admin = await users.get(admin.user_id)
    alice = await users.sync_identity("alice", "Alice")
    resources = ResourceService(storage, tmp_path, credential_key=Fernet.generate_key())
    app = FastAPI()
    app.include_router(manager_router)
    app.state.egis_services = SimpleNamespace(resources=resources)

    @app.exception_handler(AgentKitError)
    async def domain_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        app.dependency_overrides[current_user] = lambda: alice
        assert (
            await client.post("/manager/connectors", json=CONFIG)
        ).status_code == 405
        assert (
            await client.post("/manager/admin/resources/mcp", json=CONFIG)
        ).status_code == 403
        app.dependency_overrides[current_user] = lambda: admin
        result = await client.post("/manager/admin/resources/mcp", json=CONFIG)
        assert result.status_code == 201
        assert "resource_version" not in result.json()
        assert (await client.post(
            "/manager/admin/resources/mcp",
            json={**CONFIG, "resource_version": "1.0.0"},
        )).status_code == 422

        app.dependency_overrides[current_user] = lambda: alice
        cards = (await client.get("/manager/connectors")).json()["items"]
        assert [item["resource_id"] for item in cards] == ["reports"]
        assert not cards[0]["is_authorized"]
        assert (await client.get(
            "/manager/connectors/reports?version=1.0.0"
        )).status_code == 422
        assert (
            await client.post("/manager/connectors/reports/install")
        ).status_code == 403
        assert (
            await client.get("/manager/connectors/reports/versions")
        ).status_code == 404
        assert (
            await client.post("/manager/connectors/reports/test")
        ).status_code == 403
        assert (
            await client.put(
                "/manager/connectors/reports/authorization/key",
                json={"key": "test-key"},
            )
        ).status_code == 200
        assert (
            await client.post("/manager/connectors/reports/install")
        ).status_code == 200
        assert (
            await client.delete("/manager/connectors/reports/installation")
        ).status_code == 204
        assert (await client.get("/manager/connectors/reports/authorization")).json()[
            "authorized"
        ]

        await users.update_user(admin, alice.model_copy(update={"group_ids": []}))
        assert (await client.get("/manager/connectors")).json() == {"items": []}
        assert (await client.get("/manager/connectors/reports")).status_code == 403
