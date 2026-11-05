from types import SimpleNamespace

import pytest
from sqlalchemy import update
from test_domain import expert, identities
from test_resources import Registry, service

from egis_agentkit.errors import Forbidden
from egis_agentkit.models import Asset
from egis_agentkit.services import AgentService, PermissionService, ShareService
from egis_agentkit.services.snapshots import SnapshotService
from egis_agentkit.storage import tables as t


async def prepare(storage, kind):
    users, admin, alice, bob = await identities(storage)
    async with storage.transaction() as conn:
        for version, digest in [("1.0.0", "a" * 64), ("2.0.0", "b" * 64)]:
            asset = Asset(
                resource_type=kind,
                resource_id="bob-asset",
                resource_version=version,
                owner_type="user",
                owner_id="bob",
                checksum_sha256=digest,
            )
            await conn.execute(
                t.assets.insert().values(
                    **asset.model_dump(), created_by="bob", updated_by="bob"
                )
            )
    shares = ShareService(storage)
    review = await shares.submit(bob, kind, "bob-asset", "1.0.0")
    await shares.decide(admin, review["review_id"], True, "")
    return users, admin, alice, bob, shares


def binding(kind, version):
    return {
        "resource_type": kind,
        "resource_id": "bob-asset",
        "resource_version": version,
        "resource_view_type": "shared",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["skill", "mcp"])
async def test_new_binding_rejects_unreviewed_version(storage, kind):
    _, _, alice, _, _ = await prepare(storage, kind)
    value = expert()
    value.payload["bindings"] = [binding(kind, "2.0.0")]
    with pytest.raises(Forbidden):
        await AgentService(storage).save(alice, value)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["skill", "mcp"])
async def test_fresh_snapshot_rejects_legacy_unreviewed_binding(storage, kind):
    _, _, alice, _, _ = await prepare(storage, kind)
    value = expert()
    value.payload["bindings"] = [binding(kind, "1.0.0")]
    row = await AgentService(storage).save(alice, value)
    payload = {**row["payload"], "bindings": [binding(kind, "2.0.0")]}
    async with storage.transaction() as conn:
        await conn.execute(
            update(t.definitions)
            .where(t.definitions.c.id == row["id"])
            .values(payload=payload)
        )
    with pytest.raises(Forbidden):
        await SnapshotService(storage).capture(row["id"], "private")


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["skill", "mcp"])
async def test_approved_parent_preserves_old_asset_after_new_share(
    storage, tmp_path, kind
):
    users, admin, alice, bob, shares = await prepare(storage, kind)
    value = expert()
    value.payload["bindings"] = [binding(kind, "1.0.0")]
    row = await AgentService(storage).save(alice, value)
    parent = await shares.submit(alice, "agent", row["id"])
    await shares.decide(admin, parent["review_id"], True, "")
    later = await shares.submit(bob, kind, "bob-asset", "2.0.0")
    await shares.decide(admin, later["review_id"], True, "")
    charlie = await users.sync_identity("charlie", "Charlie")
    snapshot = await SnapshotService(storage).capture(row["id"], "shared")
    asset_key = snapshot["nodes"][snapshot["root_key"]]["edges"][0]
    assert snapshot["nodes"][asset_key]["data"]["resource_version"] == "1.0.0"
    await PermissionService(storage).validate_snapshot(charlie, snapshot)
    resources, http = await service(storage, tmp_path, Registry())
    async with http:
        asset = await resources._asset(kind, "bob-asset", "1.0.0")
        scope = SimpleNamespace(
            user_id="charlie",
            snapshot=snapshot,
            definition=snapshot["root"],
            binding={"agent_node_key": snapshot["root_key"]},
        )
        assert await resources._usable(charlie, asset, scope)
    with pytest.raises(Forbidden):
        await SnapshotService(storage).capture(row["id"], "private")


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value", [("checksum_sha256", "c" * 64), ("owner_id", "charlie")]
)
async def test_fresh_shared_edge_rejects_changed_approved_asset_identity(
    storage, field, value
):
    _, _, alice, _, _ = await prepare(storage, "skill")
    async with storage.transaction() as conn:
        await conn.execute(
            update(t.assets)
            .where(
                t.assets.c.resource_id == "bob-asset",
                t.assets.c.resource_version == "1.0.0",
            )
            .values(**{field: value})
        )
    candidate = expert()
    candidate.payload["bindings"] = [binding("skill", "1.0.0")]
    with pytest.raises(Forbidden):
        await AgentService(storage).save(alice, candidate)
