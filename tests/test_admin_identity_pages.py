from datetime import datetime, timezone
from types import SimpleNamespace
from uuid import UUID

import httpx
import pytest
import pytest_asyncio
from egis_agentkit_app.controller.access import router as access_router
from egis_agentkit_app.controller.base import current_user, register_exception_handlers
from egis_agentkit_app.controller.user import router as user_router
from fastapi import FastAPI, Request
from test_domain import identities

from egis_agentkit.service._virtual_user import VirtualUserService
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Group, ShareReview


@pytest_asyncio.fixture
async def admin_client(storage):
    users, admin, alice, bob = await identities(storage)
    app = FastAPI()
    app.include_router(user_router, prefix="/manager")
    app.include_router(access_router, prefix="/manager")
    register_exception_handlers(app)
    app.state.egis_services = SimpleNamespace(
        users=users,
        virtual_users=VirtualUserService(storage.virtual_users),
        grants=GrantService(storage),
        sharing=ShareService(storage),
    )
    people = {person.user_id: person for person in (admin, alice, bob)}

    async def test_user(request: Request):
        return people[request.headers.get("x-test-user", "admin")]

    app.dependency_overrides[current_user] = test_user
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client


async def _get(client, path, *, actor="admin", **params):
    response = await client.get(
        "/manager/" + path, params=params, headers={"x-test-user": actor}
    )
    assert response.status_code == 200, response.text
    return response.json()


@pytest.mark.asyncio
async def test_group_pages_count_members_and_user_pages_filter_before_limit(
    storage, admin_client,
):
    client = admin_client
    async with storage.users.write() as users:
        for group_id in ("two", "one", "company"):
            await users.save_group(
                Group(group_id=group_id, group_name="Same name"), actor="admin"
            )
    people = (await _get(client, "admin/users"))["items"]
    for person in (row for row in people if row["user_id"] != "admin"):
        user = {
            key: person[key]
            for key in ("user_id", "username", "group_ids", "role_type", "is_enabled")
        }
        user["group_ids"].append("one")
        user["is_enabled"] = user["user_id"] != "alice"
        response = await client.put(
            f"/manager/admin/users/{user['user_id']}", json=user
        )
        assert response.status_code == 200, response.text

    groups = await _get(client, "admin/groups", page=1, page_size=2)
    assert [(row["group_id"], row["member_count"]) for row in groups["items"]] == [
        ("company", 3),
        ("one", 2),
    ]
    assert groups["has_next"] is True
    assert all("users" not in row for row in groups["items"])
    next_groups = await _get(client, "admin/groups", page=2, page_size=2)
    (empty_group,) = next_groups["items"]
    assert (empty_group["group_id"], empty_group["member_count"]) == ("two", 0)
    assert next_groups["has_next"] is False
    legacy = await _get(client, "admin/groups")
    assert set(legacy) == {"items"}
    members = next(row["users"] for row in legacy["items"] if row["group_id"] == "one")
    assert {row["user_id"] for row in members} == {"alice", "bob"}

    users = await _get(client, "admin/users", group_id="one", page=1, page_size=1)
    assert [(row["user_id"], row["is_enabled"]) for row in users["items"]] == [
        ("alice", False)
    ]
    assert {key: value for key, value in users.items() if key != "items"} == {
        "page": 1,
        "page_size": 1,
        "has_next": True,
    }
    next_users = await _get(client, "admin/users", group_id="one", page=2, page_size=1)
    assert [row["user_id"] for row in next_users["items"]] == ["bob"]
    assert next_users["has_next"] is False
    empty = await _get(client, "admin/users", group_id="two", page=1, page_size=1)
    assert empty["items"] == []


@pytest.mark.asyncio
async def test_group_creation_generates_id_and_update_cannot_create(admin_client):
    path = "/manager/admin/groups"
    body = {"group_name": "  研发组  ", "is_enabled": True}
    first = await admin_client.post(path, json=body)
    second = await admin_client.post(path, json=body)
    assert first.status_code == second.status_code == 201
    group_id = first.json()["group_id"]
    assert UUID(group_id).version == 4
    assert second.json()["group_id"] != group_id
    assert first.json()["group_name"] == "研发组"
    assert (
        await admin_client.post(path, json={**body, "group_id": "manual"})
    ).status_code == 422
    assert (
        await admin_client.post(path, json={"group_name": " "})
    ).status_code == 422
    updated = await admin_client.put(
        f"{path}/{group_id}", json={**body, "group_id": group_id, "group_name": "新名称"}
    )
    assert updated.status_code == 200
    assert updated.json()["group_id"] == group_id
    assert updated.json()["group_name"] == "新名称"
    assert (
        await admin_client.put(f"{path}/missing", json={**body, "group_id": "missing"})
    ).status_code == 404


@pytest.mark.asyncio
async def test_recipient_search_matches_type_name_or_id_before_paging(
    storage, admin_client
):
    async with storage.users.write() as users:
        for identifier, name, active in (
            ("research", "数据分析", True),
            ("analytics", "数据平台", True),
            ("disabled", "数据归档", False),
        ):
            await users.save_group(
                Group(group_id=identifier, group_name=name, is_enabled=active),
                actor="admin",
            )
    first = await _get(
        admin_client, "share-recipients", principal_type="group",
        q=" 数据 ", page=1, page_size=1,
    )
    assert first["has_next"] is True
    second = await _get(
        admin_client, "share-recipients", principal_type="group",
        q="数据", page=2, page_size=1,
    )
    assert second["has_next"] is False
    assert {row["principal_id"] for row in first["items"] + second["items"]} == {
        "research", "analytics",
    }
    user = await _get(admin_client, "share-recipients", principal_type="user", q="ALIC")
    assert user["items"] == [{
        "principal_type": "user", "principal_id": "alice", "display_name": "小明",
    }]
    group = await _get(
        admin_client, "share-recipients", principal_type="group", q="ANALYTICS"
    )
    assert [row["principal_id"] for row in group["items"]] == ["analytics"]
    escaped = await _get(admin_client, "share-recipients", principal_type="group", q="%")
    assert escaped["items"] == []


@pytest.mark.asyncio
async def test_virtual_user_pages_keep_metadata_without_credentials(admin_client):
    client = admin_client
    for name in ("Beta", "Alpha"):
        response = await client.post(
            "/manager/admin/virtual-users", json={"username": name}
        )
        assert response.status_code == 201, response.text

    first = await _get(client, "admin/virtual-users", page=1, page_size=1)
    assert first["has_next"] is True
    (account,) = first["items"]
    assert account["username"] == "Alpha"
    assert set(account) == {"user_id", "username", "is_enabled"}
    second = await _get(client, "admin/virtual-users", page=2, page_size=1)
    assert [row["username"] for row in second["items"]] == ["Beta"]
    assert second["has_next"] is False
    legacy = await _get(client, "admin/virtual-users")
    assert set(legacy) == {"items"}
    assert len(legacy["items"]) == 2


@pytest.mark.asyncio
async def test_grant_page_includes_source_even_when_parent_is_on_another_page(
    storage,
    admin_client,
):
    granted_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    common = {
        "principal_type": "user",
        "principal_id": "alice",
        "resource_view_type": "system",
        "resource_owner_type": "system",
        "resource_owner_id": "engineering",
        "granted_by": "admin",
        "granted_at": granted_at,
    }
    async with storage.engine.begin() as conn:
        await conn.execute(
            t.resource_permission.insert().values(
                **common,
                id="root",
                resource_type="agent",
                resource_id="parent-resource",
                grant_source_type="manual",
                revoked_at=granted_at,
            )
        )
        await conn.execute(
            t.resource_permission.insert().values(
                **common,
                id="child",
                resource_type="skill",
                resource_id="child-resource",
                grant_source_type="dependency",
                root_grant_id="root",
                parent_grant_id="root",
                agent_id="parent-resource",
            )
        )

    first = await _get(admin_client, "admin/grants", page=1, page_size=1)
    (child,) = first["items"]
    assert child["id"] == "child"
    assert child["principal_display_name"] == "小明"
    assert (child["parent_resource_type"], child["parent_resource_id"]) == (
        "agent",
        "parent-resource",
    )
    assert first["has_next"] is True
    second = await _get(admin_client, "admin/grants", page=2, page_size=1)
    (parent,) = second["items"]
    assert parent["id"] == "root"
    assert (
        parent["parent_resource_type"] is None and parent["parent_resource_id"] is None
    )
    assert second["has_next"] is False
    assert (
        "parent_resource_type"
        not in (await _get(admin_client, "admin/grants"))["items"][0]
    )
    assert [
        row["id"]
        for row in await storage.access.active_grants_for_resources(
            "skill", ["child-resource", "parent-resource"]
        )
    ] == ["child"]
    assert (
        await storage.access.active_grants_for_resources("agent", ["parent-resource"])
        == []
    )
    assert (
        await storage.access.active_grants_for_resources("agent", ["child-resource"])
        == []
    )
    assert await storage.access.active_grants_for_resources("skill", ["missing"]) == []


@pytest.mark.asyncio
async def test_share_review_pages_keep_status_and_scope_with_stable_order(
    storage,
    admin_client,
):
    submitted_at = datetime(2026, 1, 1, tzinfo=timezone.utc)
    async with storage.engine.begin() as conn:
        for review_id, owner, status in (
            ("b", "alice", "pending"),
            ("a", "bob", "rejected"),
        ):
            review = ShareReview(
                review_id=review_id,
                resource_type="agent",
                resource_id="example",
                resource_version="1",
                owner_id=owner,
                submitted_by=owner,
                review_status_type=status,
                share_scope_type="user",
                principal_id="admin",
                submitted_at=submitted_at,
            )
            await conn.execute(t.approvals.insert().values(**review.model_dump()))

    first = await _get(admin_client, "admin/share-reviews", page=1, page_size=1)
    (review,) = first["items"]
    assert (
        review["review_id"],
        review["review_status_type"],
        review["version_label"],
    ) == (
        "a",
        "rejected",
        "1.0",
    )
    assert first["has_next"] is True
    second = await _get(admin_client, "admin/share-reviews", page=2, page_size=1)
    assert [row["review_id"] for row in second["items"]] == ["b"]
    assert second["has_next"] is False
    legacy = await _get(admin_client, "admin/share-reviews")
    assert set(legacy) == {"items"}
    assert {row["review_id"] for row in legacy["items"]} == {"a", "b"}
    personal = await _get(admin_client, "share-requests", actor="alice")
    assert set(personal) == {"items"}
    assert [row["review_id"] for row in personal["items"]] == ["b"]


@pytest.mark.asyncio
async def test_identity_admin_pages_require_admin(admin_client):
    for path in ("users", "groups", "virtual-users", "grants", "share-reviews"):
        response = await admin_client.get(
            f"/manager/admin/{path}",
            params={"page": 1},
            headers={"x-test-user": "alice"},
        )
        assert response.status_code == 403, (path, response.text)
