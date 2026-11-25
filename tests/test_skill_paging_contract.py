"""HTTP selectors and hubs bound SQL before rendering any skill cards."""

from types import SimpleNamespace

import httpx
from fastapi import FastAPI
from sqlalchemy import event
from test_domain import identities

from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import Asset


async def seed(storage, owner_type="user", owner_id="alice", count=24):
    async with storage.definition_write() as write:
        for index in range(count):
            await write.assets.insert_asset(
                Asset(
                    resource_type="skill",
                    resource_id=f"skill-{index:02}",
                    resource_version="1.0.0",
                    publication_revision=1,
                    owner_type=owner_type,
                    owner_id=owner_id,
                    checksum_sha256="a" * 64,
                    payload={
                        "display_name": "Report",
                        "description": "Paged",
                        "category": "other",
                    },
                ),
                actor=owner_id,
            )


def app_for(resources, user):
    from egis_agentkit_app.controller.base import (
        current_user,
        register_exception_handlers,
    )
    from egis_agentkit_app.controller.resource import router

    app = FastAPI()
    app.include_router(router, prefix="/manager")
    register_exception_handlers(app)
    app.dependency_overrides[current_user] = lambda: user
    app.state.egis_services = SimpleNamespace(resources=resources)
    return app


async def test_skills_http_always_pages_searches_and_bounds_explicit_selection(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await seed(storage)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(resources, alice)),
        base_url="http://test",
    ) as client:
        first = (await client.get("/manager/skills")).json()
        assert len(first["items"]) == 10
        assert first["next_cursor"]
        last = (
            await client.get(
                "/manager/skills", params={"cursor": first["next_cursor"], "limit": 100}
            )
        ).json()
        assert len(last["items"]) == 14 and last["next_cursor"] is None
        search = (
            await client.get("/manager/skills", params={"query": "skill-23"})
        ).json()
        assert [row["resource_id"] for row in search["items"]] == ["skill-23"]
        selected = (
            await client.get(
                "/manager/skills",
                params=[("resource_ids", "skill-23"), ("resource_ids", "skill-21")],
            )
        ).json()
        assert [row["resource_id"] for row in selected["items"]] == [
            "skill-21",
            "skill-23",
        ]
        for params in ({"limit": 101}, [("resource_ids", str(i)) for i in range(101)]):
            assert (
                await client.get("/manager/skills", params=params)
            ).status_code == 422


async def test_skill_hub_filters_and_limits_in_database_before_reading_payloads(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await seed(storage)
    statements = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        if "ak_skill_detail" in statement and "ORDER BY" in statement:
            statements.append((statement, parameters))

    event.listen(storage.engine.sync_engine, "before_cursor_execute", capture)
    try:
        page = await resources.skill_hub.list_skills("alice", limit=2)
        assert [card.id for card in page.cards] == ["skill-00", "skill-01"]
        assert page.next_cursor
        second = await resources.skill_hub.list_skills(
            "alice", limit=2, cursor=page.next_cursor
        )
        assert [card.id for card in second.cards] == ["skill-02", "skill-03"]
        filtered = await resources.skill_hub.list_skills("alice", q="skill-23", limit=2)
        assert [card.id for card in filtered.cards] == ["skill-23"]
    finally:
        event.remove(storage.engine.sync_engine, "before_cursor_execute", capture)
    assert statements and all("LIMIT" in query for query, _ in statements)


async def test_grant_catalog_requires_admin_and_only_lists_system_skills(
    storage, tmp_path
):
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await seed(storage, count=1)
    await resources.save_skill(
        admin,
        {
            "resource_id": "system-skill",
            "markdown": "# System",
            "owner_type": "system",
            "owner_id": "system",
        },
    )
    await resources.save_skill(
        admin,
        {
            "resource_id": "disabled-system",
            "markdown": "# Disabled",
            "owner_type": "system",
            "owner_id": "system",
        },
    )
    await resources.set_asset_enabled(
        admin, "skill", "disabled-system", "1.0.0", False, admin=True
    )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(resources, admin)),
        base_url="http://test",
    ) as client:
        page = await client.get(
            "/manager/skills", params={"purpose": "grant", "owner_type": "system"}
        )
        assert page.status_code == 200
        assert [row["resource_id"] for row in page.json()["items"]] == ["system-skill"]
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(resources, alice)),
        base_url="http://test",
    ) as client:
        assert (
            await client.get("/manager/skills", params={"purpose": "grant"})
        ).status_code == 403


async def test_skill_version_and_admin_http_defaults_are_bounded(storage, tmp_path):
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    async with storage.definition_write() as write:
        for revision in range(1, 25):
            await write.assets.insert_asset(
                Asset(
                    resource_type="skill",
                    resource_id="history",
                    resource_version=f"{revision}.0.0",
                    publication_revision=revision,
                    owner_type="user",
                    owner_id="alice",
                    checksum_sha256="a" * 64,
                    payload={"display_name": "history"},
                ),
                actor="alice",
            )
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(resources, alice)),
        base_url="http://test",
    ) as client:
        first = (await client.get("/manager/skills/history/versions")).json()
        assert len(first["items"]) == 10
        assert first["items"][0]["resource_version"] == "24.0.0"
        second = (
            await client.get(
                "/manager/skills/history/versions",
                params={"cursor": first["next_cursor"], "limit": 100},
            )
        ).json()
        assert len(second["items"]) == 14 and second["next_cursor"] is None
        filtered = (
            await client.get(
                "/manager/skills/history/versions", params={"query": "23.0.0"}
            )
        ).json()
        assert [row["resource_version"] for row in filtered["items"]] == ["23.0.0"]
        assert (
            await client.get("/manager/skills/history/versions", params={"limit": 101})
        ).status_code == 422
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(resources, admin)),
        base_url="http://test",
    ) as client:
        first = (await client.get("/manager/admin/resources/skill")).json()
        assert len(first["items"]) == 20 and first["has_next"]
        filtered = (
            await client.get(
                "/manager/admin/resources/skill",
                params={"query": "history", "page_size": 2},
            )
        ).json()
        assert len(filtered["items"]) == 2
        assert filtered["page_size"] == 2


async def test_runtime_label_lookup_is_exact_visible_and_bounded(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await seed(storage)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app_for(resources, alice)),
        base_url="http://test",
    ) as client:
        name = resources._runtime_name(alice, "skill", "skill-23", "1.0.0")
        foreign_name = resources._runtime_name(bob, "skill", "skill-22", "1.0.0")
        result = await client.get(
            "/manager/skills",
            params=[("runtime_names", name), ("runtime_names", foreign_name)],
        )
        assert result.status_code == 200
        assert [row["resource_id"] for row in result.json()["items"]] == ["skill-23"]
        assert (
            await client.get(
                "/manager/skills", params=[("runtime_names", "invalid")] * 101
            )
        ).status_code == 422
