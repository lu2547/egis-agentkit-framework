"""Batch endpoints keep independent commits while joining current write checks."""

from types import SimpleNamespace

import httpx
import pytest
from egis_agentkit_app.controller.access import router
from egis_agentkit_app.controller.base import current_user, register_exception_handlers
from fastapi import FastAPI, Request
from test_domain import COMPANY, identities
from test_permission_queries import queries
from test_resources import put_asset

from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._share import ShareService


@pytest.mark.parametrize("count", [1, 10, 50])
async def test_skill_batch_write_checks_are_joined_and_results_unchanged(
    storage, count
):
    _, admin, alice, bob = await identities(storage)
    for index in range(count):
        await put_asset(
            storage, rid=f"private{index:02}", owner_type="user", owner_id="alice"
        )
        await put_asset(storage, rid=f"system{index:02}")
    sharing, grants = ShareService(storage), GrantService(storage)
    app = FastAPI()
    app.include_router(router, prefix="/manager")
    register_exception_handlers(app)
    app.state.egis_services = SimpleNamespace(sharing=sharing, grants=grants)

    async def identity(request: Request):
        return admin if request.headers["x-user"] == "admin" else alice

    app.dependency_overrides[current_user] = identity
    target = COMPANY.model_dump()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        with queries(storage) as selected:
            response = await client.post(
                "/manager/share-requests/batch",
                headers={"x-user": "alice"},
                json={
                    "resource_ids": [f"private{index:02}" for index in range(count)],
                    "target": target,
                },
            )
        assert response.status_code == 200 and all(
            item["success"] for item in response.json()["items"]
        )
        print(f"ACCESS_BATCH_SQL submit count={count} queries={len(selected)}")
        assert len(selected) <= 3 * count, len(selected)
        with queries(storage) as selected:
            reviews = await sharing.list(alice)
        assert [row["resource_id"] for row in reviews] == [
            f"private{index:02}" for index in reversed(range(count))
        ]
        assert len(selected) == 1
        with queries(storage) as selected:
            response = await client.post(
                "/manager/admin/share-reviews/batch/decision",
                headers={"x-user": "admin"},
                json={
                    "review_ids": [row["review_id"] for row in reviews],
                    "is_approved": True,
                },
            )
        assert response.status_code == 200 and all(
            item["success"] for item in response.json()["items"]
        )
        print(f"ACCESS_BATCH_SQL approve count={count} queries={len(selected)}")
        assert len(selected) <= 8 * count, len(selected)
        with queries(storage) as selected:
            response = await client.post(
                "/manager/admin/grants/batch",
                headers={"x-user": "admin"},
                json={
                    "resources": [
                        {
                            "resource_id": f"system{index:02}",
                            "resource_owner_id": "demo",
                        }
                        for index in range(count)
                    ],
                    "target": target,
                },
            )
        assert response.status_code == 200 and all(
            item["success"] for item in response.json()["items"]
        )
        print(f"ACCESS_BATCH_SQL grant count={count} queries={len(selected)}")
        assert len(selected) <= 3 * count, len(selected)
    grants = await storage.access.list_grants()
    assert sum(row["grant_source_type"] == "share" for row in grants) == count
    assert sum(row["grant_source_type"] == "manual" for row in grants) == count


async def test_batch_observes_recipient_disable_after_an_earlier_commit(
    storage, monkeypatch
):
    from sqlalchemy import update

    from egis_agentkit.storage import tables as t

    _, admin, _, bob = await identities(storage)
    for identifier in ("first", "second"):
        await put_asset(storage, rid=identifier)
    grants = GrantService(storage)
    original = grants.grant

    async def change_after_commit(user, data, **context):
        result = await original(user, data, **context)
        if context.get("write") is None and data.resource_id == "first":
            async with storage.transaction() as conn:
                await conn.execute(
                    update(t.users)
                    .where(t.users.c.user_id == bob.user_id)
                    .values(is_enabled=False)
                )
        return result

    monkeypatch.setattr(grants, "grant", change_after_commit)
    app = FastAPI()
    app.include_router(router, prefix="/manager")
    register_exception_handlers(app)
    app.state.egis_services = SimpleNamespace(grants=grants)
    app.dependency_overrides[current_user] = lambda: admin
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        response = await client.post(
            "/manager/admin/grants/batch",
            json={
                "resources": [
                    {"resource_id": identifier, "resource_owner_id": "demo"}
                    for identifier in ("first", "second")
                ],
                "target": {"principal_type": "user", "principal_id": "bob"},
            },
        )
    assert response.status_code == 200
    assert response.json()["items"] == [
        {"id": "first", "success": True},
        {
            "id": "second",
            "success": False,
            "status_code": 404,
            "detail": "授权用户不存在或已停用",
        },
    ]
    rows = await storage.access.list_grants()
    assert [(row["resource_id"], row["principal_id"]) for row in rows] == [
        ("first", "bob")
    ]
