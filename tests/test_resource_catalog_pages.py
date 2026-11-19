from types import SimpleNamespace

import httpx
import pytest
from fastapi import FastAPI
from test_domain import identities

from egis_agentkit.client.nacos._settings import SkillConfig
from egis_agentkit.errors import InvalidRequest
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import Asset, Grant


def identifiers(page):
    return [item["resource_id"] for item in page["items"]]


async def test_skill_catalog_pages_only_current_visible_filtered_skills(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)

    async def publish(identifier, owner, category, *, description="", revision=1):
        asset = Asset(
            resource_type="skill",
            resource_id=identifier,
            resource_version=f"{revision}.0.0",
            publication_revision=revision,
            owner_type="user",
            owner_id=owner,
            checksum_sha256="a" * 64,
            payload={"category": category, "description": description},
        )
        async with storage.transaction() as conn:
            await storage.assets.insert_asset(asset, actor=owner, conn=conn)

    await publish("hidden", bob.user_id, "秘密")
    await publish("skill-00", alice.user_id, "历史", description="old-only")
    for index in range(24):
        await publish(
            "skill-09.legacy" if index == 9 else f"skill-{index:02}",
            alice.user_id,
            "分析" if index == 22 else "写作",
            description="100X REVIEW" if index == 23 else "100% REVIEW",
            revision=2 if index == 0 else 1,
        )
    await resources.install(alice, "skill", "skill-22")
    await resources.update_installation(alice, "skill", "skill-22", False)

    first = await resources.catalog_page(
        alice, "skill", query=" 100% review ", category="写作"
    )
    assert identifiers(first) == [
        *(f"skill-{i:02}" for i in range(9)),
        "skill-09.legacy",
    ]
    assert first["categories"] == ["写作", "分析"]
    assert first["has_shareable"] is True
    second = await resources.catalog_page(
        alice,
        "skill",
        query="100% review",
        category="写作",
        cursor=first["next_cursor"],
    )
    assert identifiers(second) == [f"skill-{i:02}" for i in range(10, 20)]
    last = await resources.catalog_page(
        alice,
        "skill",
        query="100% review",
        category="写作",
        cursor=second["next_cursor"],
    )
    assert last.keys() == {"items", "next_cursor"}
    assert identifiers(last) == ["skill-20", "skill-21"]
    assert last["next_cursor"] is None
    assert (
        identifiers(await resources.catalog_page(alice, "skill", query="old-only"))
        == []
    )
    mine = await resources.catalog_page(alice, "skill", mine=True)
    assert identifiers(mine) == ["skill-22"]
    assert mine["items"][0]["is_installed"] and not mine["items"][0]["is_enabled"]
    for user, kind, query, category, mine in (
        (bob, "skill", "100% review", "写作", False),
        (alice, "mcp", "100% review", "写作", False),
        (alice, "skill", "different", "写作", False),
        (alice, "skill", "100% review", "分析", False),
        (alice, "skill", "100% review", "写作", True),
    ):
        with pytest.raises(InvalidRequest):
            await resources.catalog_page(
                user,
                kind,
                cursor=first["next_cursor"],
                query=query,
                category=category,
                mine=mine,
            )

    resources.config = SimpleNamespace(
        snapshot=SimpleNamespace(skill=SkillConfig(catalog={"defaultPageSize": 2}))
    )
    assert identifiers(await resources.catalog_page(alice, "skill")) == [
        "skill-00",
        "skill-01",
    ]
    assert len(await resources.list(alice, "skill")) == 24


async def test_skill_catalog_refills_if_a_shared_grant_is_revoked_during_read(
    storage, tmp_path, monkeypatch
):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    for identifier, owner, category in (
        ("first-shared", bob, "共享"),
        ("second-own", alice, "我的"),
    ):
        await resources.save_skill(
            owner,
            {
                "resource_id": identifier,
                "markdown": "# Catalog Skill",
                "category": category,
            },
        )
    await resources.set_asset_enabled(alice, "skill", "second-own", "1.0.0", False)
    grants = GrantService(storage)
    grant = await grants.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=alice.user_id,
            resource_type="skill",
            resource_id="first-shared",
            resource_view_type="shared",
            resource_owner_type="user",
            resource_owner_id=bob.user_id,
        ),
    )
    await resources.save_skill(
        bob,
        {
            "resource_id": "first-shared",
            "resource_version": "2.0.0",
            "base_version": "1.0.0",
            "markdown": "# Private Update",
            "category": "私有更新",
        },
    )
    before = await resources.catalog_page(alice, "skill", limit=1)
    assert identifiers(before) == ["first-shared"]
    assert before["items"][0]["resource_version"] == "1.0.0"
    assert before["categories"] == ["共享", "我的"]

    shared_asset = resources._shared_asset

    async def revoke_before_render(user, asset):
        await grants.revoke(admin, grant["id"])
        return await shared_asset(user, asset)

    monkeypatch.setattr(resources, "_shared_asset", revoke_before_render)
    page = await resources.catalog_page(alice, "skill", limit=1)
    assert identifiers(page) == ["second-own"]
    assert page["items"][0]["is_editable"] and not page["items"][0]["is_usable"]
    assert page["categories"] == ["我的"]
    assert page["next_cursor"] is None


async def test_connector_catalog_pages_company_granted_managed_connectors(
    storage, tmp_path
):
    users, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    for index in range(12):
        await resources.save_mcp(
            admin,
            {
                "resource_id": f"connector-{index:02}",
                "display_name": "Catalog Connector",
                "config": {
                    "transport": "streamable_http",
                    "url": "https://example.com/mcp",
                },
            },
        )
    await resources.set_asset_enabled(
        admin, "mcp", "connector-00", None, False, admin=True
    )
    await users.update_user(admin, bob.model_copy(update={"group_ids": []}))

    first = await resources.catalog_page(alice, "mcp", query="CATALOG")
    assert first.keys() == {"items", "next_cursor"}
    assert identifiers(first) == [f"connector-{i:02}" for i in range(1, 11)]
    last = await resources.catalog_page(
        alice, "mcp", query="catalog", cursor=first["next_cursor"]
    )
    assert identifiers(last) == ["connector-11"]
    assert last["next_cursor"] is None
    assert identifiers(await resources.catalog_page(bob, "mcp")) == []


async def test_tool_catalog_filters_after_selecting_latest_visible_version(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)

    def unused_factory(scope):
        raise AssertionError("Catalog listing does not run tools")

    for version, description in [("2.0.0", "old-only"), ("10.0.0", "Current release")]:
        resources.tool_registry.deploy(
            "custom-tool",
            version,
            unused_factory,
            description=description,
            input_schema={"type": "object"},
        )
    await resources.seed_builtin_tools()

    first = await resources.catalog_page(alice, "tool", category="系统默认")
    assert len(first["items"]) == 10
    assert all(item["is_system_default"] for item in first["items"])
    assert first["categories"] == ["系统默认", "专家工具"]
    last = await resources.catalog_page(
        alice, "tool", category="系统默认", cursor=first["next_cursor"]
    )
    assert not set(identifiers(first)).intersection(identifiers(last))
    expert = await resources.catalog_page(
        alice, "tool", category="专家工具", query="current"
    )
    assert identifiers(expert) == ["custom-tool"]
    assert expert["items"][0]["resource_version"] == "10.0.0"
    assert (
        identifiers(await resources.catalog_page(alice, "tool", query="old-only")) == []
    )


async def test_manager_resource_routes_opt_in_to_catalog_pages(storage, tmp_path):
    from egis_agentkit_app.controller.base import (
        current_user,
        register_exception_handlers,
    )
    from egis_agentkit_app.controller.resource import router

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.seed_builtin_tools()
    app = FastAPI()
    app.include_router(router, prefix="/manager")
    register_exception_handlers(app)
    app.dependency_overrides[current_user] = lambda: alice
    app.state.egis_services = SimpleNamespace(resources=resources)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for path in ("skills", "connectors", "tools"):
            legacy = await client.get(f"/manager/{path}")
            assert legacy.status_code == 200
            assert set(legacy.json()) == {"items"}
            page = await client.get(f"/manager/{path}", params={"catalog": "true"})
            assert page.status_code == 200
            assert len(page.json()["items"]) == min(10, len(legacy.json()["items"]))
            assert all(item in legacy.json()["items"] for item in page.json()["items"])
            assert bool(page.json()["next_cursor"]) == (path == "tools")

        filtered = await client.get(
            "/manager/tools",
            params={"catalog": "true", "query": "PowerShell", "limit": 1},
        )
        assert filtered.status_code == 200
        assert identifiers(filtered.json()) == ["PowerShell"]
        invalid = await client.get(
            "/manager/tools", params={"catalog": "true", "cursor": "invalid"}
        )
        assert invalid.status_code == 422
        invalid = await client.get(
            "/manager/skills", params={"catalog": "true", "query": "\0"}
        )
        assert invalid.status_code == 422
