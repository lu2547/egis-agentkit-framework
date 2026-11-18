from types import SimpleNamespace

import httpx
import pytest
from egis_agentkit_app.controller.access import router
from egis_agentkit_app.controller.base import current_user, register_exception_handlers
from fastapi import FastAPI, Request
from test_domain import COMPANY, identities

from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.resource._catalog import ResourceService


@pytest.mark.asyncio
async def test_skill_batches_use_existing_permissions_and_return_partial_results(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    people = {user.user_id: user for user in (admin, alice, bob)}
    resources = ResourceService(storage, tmp_path)
    sharing = ShareService(storage)
    grants = GrantService(storage)
    app = FastAPI()
    app.include_router(router, prefix="/manager")
    register_exception_handlers(app)
    app.state.egis_services = SimpleNamespace(sharing=sharing, grants=grants)

    async def test_user(request: Request):
        return people[request.headers["x-test-user"]]

    app.dependency_overrides[current_user] = test_user
    for user, resource_id, owner in (
        (alice, "private-one", {}),
        (alice, "private-two", {}),
        (bob, "other-owners-skill", {}),
        (admin, "system-one", {"owner_type": "system", "owner_id": "engineering"}),
    ):
        await resources.save_skill(
            user,
            {
                "resource_id": resource_id,
                "resource_version": "1.0.0",
                "markdown": "# Test skill",
                **owner,
            },
        )

    share_path = "/manager/share-requests/batch"
    decision_path = "/manager/admin/share-reviews/batch/decision"
    grant_path = "/manager/admin/grants/batch"
    target = COMPANY.model_dump()
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        for ids in (
            [],
            [f"private-{index}" for index in range(51)],
            ["private-one"] * 2,
        ):
            response = await client.post(
                share_path,
                headers={"x-test-user": "alice"},
                json={"resource_ids": ids, "target": target},
            )
            assert response.status_code == 422, response.text
        response = await client.post(
            share_path,
            headers={"x-test-user": "alice"},
            json={
                "resource_ids": ["private-one", "other-owners-skill", "private-two"],
                "target": target,
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["items"] == [
            {"id": "private-one", "success": True},
            {
                "id": "other-owners-skill",
                "success": False,
                "status_code": 403,
                "detail": "只能申请共享自己的资源",
            },
            {"id": "private-two", "success": True},
        ]
        reviews = {row["resource_id"]: row for row in await sharing.list(alice)}
        assert set(reviews) == {"private-one", "private-two"}
        review_ids = [
            reviews[key]["review_id"] for key in ("private-one", "private-two")
        ]
        assert all(
            row["grant_source_type"] == "owner"
            for row in await grants.list_grants(admin)
        )

        decision = {
            "review_ids": [review_ids[0], "missing-review", review_ids[1]],
            "is_approved": True,
            "reason": "Batch decision",
        }
        grant = {
            "resources": [
                {"resource_id": "private-one", "resource_owner_id": "alice"},
                {"resource_id": "system-one", "resource_owner_id": "engineering"},
            ],
            "target": target,
        }
        for path, body in ((decision_path, decision), (grant_path, grant)):
            response = await client.post(
                path, headers={"x-test-user": "alice"}, json=body
            )
            assert response.status_code == 403, response.text
        assert {row["review_status_type"] for row in await sharing.list(alice)} == {
            "pending"
        }

        response = await client.post(
            decision_path, headers={"x-test-user": "admin"}, json=decision
        )
        assert response.status_code == 200, response.text
        first, missing, last = response.json()["items"]
        assert [first, last] == [
            {"id": review_id, "success": True} for review_id in review_ids
        ]
        assert missing["id"] == "missing-review" and missing["status_code"] == 404
        assert {row["review_reason"] for row in await sharing.list(alice)} == {
            "Batch decision"
        }

        response = await client.post(
            grant_path, headers={"x-test-user": "admin"}, json=grant
        )
        assert response.status_code == 200, response.text
        denied, granted = response.json()["items"]
        assert denied["id"] == "private-one" and denied["status_code"] == 403
        assert granted == {"id": "system-one", "success": True}

    assert {
        (row["resource_id"], row["grant_source_type"], row["principal_id"])
        for row in await grants.list_grants(admin)
        if row["grant_source_type"] != "owner"
    } == {
        ("private-one", "share", "company"),
        ("private-two", "share", "company"),
        ("system-one", "manual", "company"),
    }
