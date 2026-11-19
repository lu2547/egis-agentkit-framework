"""Cover admin pages that include resource versions and filtered review work."""

import io
from types import SimpleNamespace

import httpx
import pytest
from egis_agentkit_app.bootstrap._router import manager_router
from egis_agentkit_app.controller.base import current_user, register_exception_handlers
from fastapi import FastAPI
from test_app_http import app_snapshot
from test_domain import expert, identities
from test_managed_connectors import CONFIG
from test_resources import archive
from test_skill_imports import bundle

from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill_import import SkillImportService
from egis_agentkit.storage.model import AgentDefinition, Asset
from egis_agentkit.storage.model._resource import MCP_STORAGE_VERSION


@pytest.fixture
async def management(storage, tmp_path):
    _, admin, alice, _ = await identities(storage)
    config = SimpleNamespace(
        snapshot=app_snapshot(
            tmp_path,
            admin={
                "pagination": {
                    "default_page_size": 1,
                    "max_page_size": 2,
                }
            },
        )
    )
    resources = ResourceService(storage, tmp_path)
    imports, agents = SkillImportService(storage, resources), AgentManager(storage)
    app = FastAPI()
    app.include_router(manager_router)
    register_exception_handlers(app)
    app.state.egis_config = config
    app.state.egis_services = SimpleNamespace(
        resources=resources, skill_imports=imports, agents=agents
    )
    app.dependency_overrides[current_user] = lambda: admin
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client, app, resources, imports, agents, admin, alice


async def test_skill_admin_pages_enrich_the_page_with_binding_and_grant(management):
    client, app, resources, _, agents, admin, alice = management
    for identifier in ("alpha", "beta"):
        await resources.upload_skill(
            alice, archive(identifier), "1.0.0", resource_id=identifier
        )
    record = expert().model_dump()
    record["payload"]["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": "alpha",
            "resource_version": "1.0.0",
            "resource_view_type": "private",
        }
    ]
    writer = await agents.add_definition(alice, AgentDefinition(**record))
    url = "/manager/admin/resources/skill"
    first = (await client.get(url, params={"page": 1})).json()
    assert first["page"] == 1 and first["page_size"] == 1 and first["has_next"]
    (alpha,) = first["items"]
    assert alpha["resource_id"] == "alpha"
    assert alpha["bound_agents"] == [{"id": writer["id"], "name": "写作专家"}]
    assert alpha["grant_sources"][0]["principal_id"] == "alice"
    final = (await client.get(url, params={"page": 2})).json()
    assert final["page"] == 2 and not final["has_next"]
    assert final["items"][0]["resource_id"] == "beta"
    assert not final["items"][0]["bound_agents"]
    assert (await client.get(url)).json()["items"] == [alpha, final["items"][0]]
    assert (await client.get(url, params={"page_size": 3})).status_code == 422
    assert (await client.get(url, params={"page": 0})).status_code == 422
    app.dependency_overrides[current_user] = lambda: alice
    assert (await client.get(url, params={"page": 1})).status_code == 403


async def test_managed_mcp_filter_is_applied_before_pagination(storage, management):
    client, _, resources, _, _, admin, _ = management
    legacy = Asset(
        resource_type="mcp",
        resource_id="aa-legacy",
        resource_version=MCP_STORAGE_VERSION,
        owner_type="system",
        owner_id="system",
        payload={},
    )
    async with storage.definition_write() as write:
        await write.assets.insert_asset(legacy, actor=admin.user_id)
    for identifier in ("directory", "reports"):
        await resources.save_mcp(admin, {**CONFIG, "resource_id": identifier})
    url = "/manager/admin/resources/mcp"
    first = (await client.get(url, params={"page": 1})).json()
    final = (await client.get(url, params={"page": 2})).json()
    assert first["items"][0]["resource_id"] == "directory" and first["has_next"]
    assert final["items"][0]["resource_id"] == "reports" and not final["has_next"]


async def test_system_import_reviews_are_paginated_after_queue_filter(management):
    client, _, _, imports, _, admin, alice = management
    await imports.enqueue(alice, io.BytesIO(bundle()), filename="private.zip")
    for name in ("first.zip", "second.zip"):
        await imports.enqueue(
            admin, io.BytesIO(bundle()), filename=name, owner_type="system"
        )
    for _ in range(3):
        assert await imports.run_once()
    url = "/manager/admin/skill-imports"
    first = (await client.get(url, params={"page": 1})).json()
    final = (await client.get(url, params={"page": 2})).json()
    assert first["items"][0]["filename"] == "first.zip" and first["has_next"]
    assert final["items"][0]["filename"] == "second.zip" and not final["has_next"]
    assert all(
        item["status"] == "pending_approval" for item in first["items"] + final["items"]
    )


async def test_remote_management_pages_return_only_editable_remote_summaries(
    management,
):
    client, app, _, _, agents, admin, alice = management
    for name, transport, endpoint in (
        ("first", "http_sync", "https://reports.example/run"),
        ("second", "websocket", "wss://reports.example/run"),
    ):
        await agents.add_definition(
            admin,
            AgentDefinition(
                owner_type="system",
                owner_id="system",
                agent_type="remote",
                agent_name=name,
                agent_display_name=name,
                payload={
                    "remote_config": {
                        "transport": transport,
                        "endpoint": endpoint,
                        "credential_ref": "EGIS_REMOTE_AGENT_TEST",
                    }
                },
            ),
        )
    await agents.add_definition(alice, expert())
    url = "/manager/admin/remote-agents"
    first = (await client.get(url)).json()
    final = (await client.get(url, params={"page": 2})).json()
    assert first["has_next"] and not final["has_next"]
    assert {
        row["agent_name"]: row["payload"]["remote_config"]
        for row in first["items"] + final["items"]
    } == {
        "first": {"transport": "http_sync"},
        "second": {"transport": "websocket"},
    }
    assert all(row["is_editable"] for row in first["items"] + final["items"])
    app.dependency_overrides[current_user] = lambda: alice
    assert (await client.get(url, params={"page": 1})).status_code == 403
