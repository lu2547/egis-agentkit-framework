import pytest
from sqlalchemy import select
from test_domain import COMPANY, identities

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage import tables as t


@pytest.mark.asyncio
async def test_private_skill_requires_review_and_approval_grants_exact_version(
    storage, tmp_path
):
    users, admin, alice, bob = await identities(storage)
    alice = await users.sync_identity(alice.user_id, "技能作者")
    resources = ResourceService(storage, tmp_path)
    shares = ShareService(storage)
    data = {
        "resource_id": "private-asset",
        "resource_version": "1.0.0",
        "markdown": "# Private",
    }
    own = await resources.save_skill(alice, data)
    assert own["is_usable"]
    assert await resources.list(bob, "skill") == []
    with pytest.raises(Forbidden):
        await shares.submit(bob, "skill", data["resource_id"], target=COMPANY)
    review = await shares.submit(alice, "skill", data["resource_id"], target=COMPANY)
    assert review["resource_version"] == "1.0.0"
    assert "snapshot" not in review
    assert await resources.list(bob, "skill") == []
    approved = await shares.decide(admin, review["review_id"], True, "")
    assert approved["is_current"] and approved["resource_version"] == "1.0.0"
    assert await shares.decide(admin, review["review_id"], True, "") == approved
    (shared_skill,) = await resources.list(bob, "skill")
    assert shared_skill["owner_display_name"] == "技能作者"
    detail = await resources.detail(bob, "skill", data["resource_id"], "1.0.0")
    assert detail["is_usable"]
    assert detail["owner_display_name"] == "技能作者"
    async with storage.engine.connect() as conn:
        grants = (await conn.execute(select(t.resource_permission))).mappings().all()
    assert {(row["grant_source_type"], row["principal_type"]) for row in grants} == {
        ("owner", "user"),
        ("share", "group"),
    }
    shared = next(row for row in grants if row["grant_source_type"] == "share")
    assert shared["source_review_id"] == review["review_id"]
    assert "resource_version" not in shared
    newer = {**data, "resource_version": "1.1.0", "base_version": "1.0.0"}
    await resources.save_skill(alice, newer)
    with pytest.raises(Forbidden):
        await resources.detail(bob, "skill", data["resource_id"], "1.1.0")


@pytest.mark.asyncio
async def test_managed_connector_rejects_share_requests(storage, tmp_path):
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_mcp(
        admin,
        {
            "resource_id": "managed-connector",
            "config": {
                "transport": "streamable_http",
                "url": "https://example.com/mcp",
            },
        },
    )
    shares = ShareService(storage)
    with pytest.raises(Conflict, match="不支持共享"):
        await shares.submit(alice, "mcp", "managed-connector", target=COMPANY)
    with pytest.raises(Conflict, match="不支持共享"):
        await shares.submit(admin, "mcp", "managed-connector")
    assert await shares.list(admin, admin=True) == []


@pytest.mark.asyncio
async def test_approval_rejects_disabled_asset_version(storage, tmp_path):
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_skill(
        alice,
        {"resource_id": "report", "resource_version": "1.0.0", "markdown": "# Report"},
    )
    shares = ShareService(storage)
    review = await shares.submit(alice, "skill", "report", target=COMPANY)
    await resources.set_asset_enabled(alice, "skill", "report", "1.0.0", False)
    with pytest.raises(Conflict):
        await shares.decide(admin, review["review_id"], True, "")


@pytest.mark.asyncio
async def test_asset_update_moves_one_shared_publication(storage, tmp_path):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    shares = ShareService(storage)
    data = {"resource_id": "versions", "resource_version": "1.0.0", "markdown": "# One"}
    await resources.save_skill(alice, data)
    with pytest.raises(Conflict):
        await shares.submit(alice, "skill", "versions")
    first = await shares.submit(alice, "skill", "versions", target=COMPANY)
    await shares.decide(admin, first["review_id"], True, "")
    await resources.save_skill(
        alice,
        {
            **data,
            "resource_version": "2.0.0",
            "base_version": "1.0.0",
            "markdown": "# Two",
        },
    )
    second = await shares.submit(alice, "skill", "versions")
    assert (await shares.decide(admin, second["review_id"], True, ""))[
        "resource_version"
    ] == "2.0.0"
    assert (await resources.detail(bob, "skill", "versions", "2.0.0"))["is_usable"]
    with pytest.raises(Forbidden):
        await resources.detail(bob, "skill", "versions", "1.0.0")
    async with storage.engine.connect() as conn:
        grants = (await conn.execute(select(t.resource_permission))).mappings().all()
    assert {
        row["source_review_id"] for row in grants if row["grant_source_type"] == "share"
    } == {first["review_id"]}
    await resources.set_asset_enabled(alice, "skill", "versions", "2.0.0", False)
    with pytest.raises(Forbidden):
        await resources.detail(bob, "skill", "versions", "2.0.0")
    with pytest.raises(Conflict):
        await shares.submit(alice, "skill", "versions", target=COMPANY)
