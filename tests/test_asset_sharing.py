import pytest
from sqlalchemy import select, update
from test_domain import identities
from test_resources import Registry, service

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.models import Asset
from egis_agentkit.services import ShareService
from egis_agentkit.storage import tables as t


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["skill", "mcp"])
async def test_private_asset_requires_review_and_approval_grants_exact_version(
    storage, tmp_path, kind
):
    _, admin, alice, bob = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    shares = ShareService(storage)
    data = {"resource_id": "private-asset", "resource_version": "1.0.0"}
    data.update(
        {"markdown": "# Private"}
        if kind == "skill"
        else {
            "config": {"transport": "streamable_http", "url": "http://example.com/mcp"}
        }
    )
    async with http:
        own = await (
            resources.save_skill(alice, data)
            if kind == "skill"
            else resources.save_mcp(alice, data)
        )
        assert own["is_usable"]
        assert await resources.list(bob, kind) == []
        with pytest.raises(Forbidden):
            await shares.submit(bob, kind, data["resource_id"], "1.0.0")
        review = await shares.submit(alice, kind, data["resource_id"], "1.0.0")
        asset = await resources._asset(kind, data["resource_id"], "1.0.0")
        assert review["snapshot"]["asset"] == {k: asset[k] for k in Asset.model_fields}
        assert review["source_versions"] == {data["resource_id"]: "1.0.0"}
        assert await resources.list(bob, kind) == []
        approved = await shares.decide(admin, review["review_id"], True, "")
        assert approved["is_current"] and approved["shared_version"] == 1
        assert (await shares.decide(admin, review["review_id"], True, ""))[
            "shared_version"
        ] == 1
        assert (await resources.detail(bob, kind, data["resource_id"], "1.0.0"))[
            "is_usable"
        ]
        async with storage.engine.connect() as conn:
            grants = (await conn.execute(select(t.grants))).mappings().all()
        assert len(grants) == 1
        assert (
            grants[0]["principal_type"] == "all_users"
            and grants[0]["grant_source_type"] == "share"
        )
        assert grants[0]["source_review_id"] == review["review_id"]
        newer = {**data, "resource_version": "1.1.0"}
        await (
            resources.save_skill(alice, newer)
            if kind == "skill"
            else resources.save_mcp(alice, newer)
        )
        with pytest.raises(Forbidden):
            await resources.detail(bob, kind, data["resource_id"], "1.1.0")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "change", [{"is_enabled": False}, {"checksum_sha256": "a" * 64}]
)
async def test_approval_rejects_disabled_or_changed_asset(storage, change):
    _, admin, alice, _ = await identities(storage)
    asset = Asset(
        resource_type="skill",
        resource_id="report",
        resource_version="1.0.0",
        owner_type="user",
        owner_id="alice",
        checksum_sha256="b" * 64,
    )
    async with storage.transaction() as conn:
        await conn.execute(
            t.assets.insert().values(
                **asset.model_dump(), created_by="alice", updated_by="alice"
            )
        )
    shares = ShareService(storage)
    review = await shares.submit(alice, "skill", "report", "1.0.0")
    async with storage.transaction() as conn:
        await conn.execute(
            update(t.assets).where(t.assets.c.resource_id == "report").values(**change)
        )
    with pytest.raises(Conflict):
        await shares.decide(admin, review["review_id"], True, "")


@pytest.mark.asyncio
async def test_asset_submission_requires_enabled_exact_version_and_reapproval_replaces_current(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    shares = ShareService(storage)
    data = {"resource_id": "versions", "resource_version": "1.0.0", "markdown": "# One"}
    async with http:
        await resources.save_skill(alice, data)
        with pytest.raises(Conflict):
            await shares.submit(alice, "skill", "versions")
        first = await shares.submit(alice, "skill", "versions", "1.0.0")
        await shares.decide(admin, first["review_id"], True, "")
        await resources.save_skill(
            alice, {**data, "resource_version": "2.0.0", "markdown": "# Two"}
        )
        second = await shares.submit(alice, "skill", "versions", "2.0.0")
        assert (await shares.decide(admin, second["review_id"], True, ""))[
            "shared_version"
        ] == 2
        assert (await resources.detail(bob, "skill", "versions", "2.0.0"))["is_usable"]
        with pytest.raises(Forbidden):
            await resources.detail(bob, "skill", "versions", "1.0.0")
        async with storage.engine.connect() as conn:
            grants = (await conn.execute(select(t.grants))).mappings().all()
        assert len(grants) == 1 and grants[0]["source_review_id"] == second["review_id"]
        async with storage.transaction() as conn:
            await conn.execute(
                update(t.assets)
                .where(
                    t.assets.c.resource_id == "versions",
                    t.assets.c.resource_version == "2.0.0",
                )
                .values(is_enabled=False)
            )
        with pytest.raises(Conflict):
            await shares.submit(alice, "skill", "versions", "2.0.0")
