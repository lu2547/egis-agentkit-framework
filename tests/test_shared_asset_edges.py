from types import SimpleNamespace

import pytest
from test_domain import COMPANY, expert, identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset


async def prepare(storage, kind):
    users, admin, alice, bob = await identities(storage)
    async def publish(version, revision, digest):
        async with storage.transaction() as conn:
            asset = Asset(
                resource_type=kind,
                resource_id="bob-asset",
                resource_version=version,
                publication_revision=revision,
                owner_type="user",
                owner_id="bob",
                checksum_sha256=digest,
            )
            await conn.execute(
                t.assets.insert().values(
                    **asset.model_dump(), created_by="bob", updated_by="bob"
                )
            )
    await publish("1.0.0", 1, "a" * 64)
    shares = ShareService(storage)
    review = await shares.submit(bob, kind, "bob-asset", target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "")
    await publish("2.0.0", 2, "b" * 64)
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
        await AgentManager(storage).add_definition(alice, value)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["skill", "mcp"])
async def test_execution_rejects_stored_unreviewed_binding(storage, kind):
    _, _, alice, _, _ = await prepare(storage, kind)
    value = expert()
    value.payload["bindings"] = [binding(kind, "1.0.0")]
    row = await AgentManager(storage).add_definition(alice, value)
    value.payload["bindings"] = [binding(kind, "2.0.0")]
    async with storage.definition_write() as write:
        saved = await write.definitions.update_definition(
            row["id"], value.model_dump(mode="json"), actor=alice.user_id
        )
    snapshot = await storage.definitions.get_definition_view(
        saved["id"], "private", version=saved["resource_version"]
    )
    with pytest.raises(Forbidden):
        await PermissionService(storage).require_execution(alice, snapshot)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["skill", "mcp"])
async def test_approved_parent_preserves_old_asset_after_new_share(
    storage, tmp_path, kind
):
    users, admin, alice, bob, shares = await prepare(storage, kind)
    value = expert()
    value.payload["bindings"] = [binding(kind, "1.0.0")]
    row = await AgentManager(storage).add_definition(alice, value)
    parent = await shares.submit(
        alice, "agent", row["id"], target=COMPANY
    )
    await shares.decide(admin, parent["review_id"], True, "")
    later = await shares.submit(bob, kind, "bob-asset")
    await shares.decide(admin, later["review_id"], True, "")
    charlie = await users.sync_identity("charlie", "Charlie")
    await users.update_user(
        admin, charlie.model_copy(update={"group_ids": [COMPANY.principal_id]})
    )
    charlie = await users.get(charlie.user_id)
    permissions = PermissionService(storage)
    decision = await permissions.require(charlie, "agent", row["id"], "shared")
    snapshot = await storage.definitions.get_definition_view(
        row["id"],
        "shared",
        version=decision.resource_version,
        **decision.source_options,
    )
    assert snapshot["payload"]["bindings"][0]["resource_version"] == "1.0.0"
    await permissions.require_execution(charlie, snapshot)
    resources = ResourceService(storage, tmp_path)
    asset = await resources._asset(kind, "bob-asset", "1.0.0")
    scope = SimpleNamespace(
        user_id="charlie",
        root_definition=snapshot,
        definition=snapshot,
        binding={"agent_definition_id": snapshot["id"]},
    )
    assert await resources._usable(charlie, asset, scope)
    assert await storage.definitions.get_definition_view(row["id"], "private")


@pytest.mark.asyncio
async def test_revoked_shared_asset_version_cannot_be_bound(storage):
    _, admin, alice, _, _ = await prepare(storage, "skill")
    grants = GrantService(storage)
    (grant,) = await grants.list_grants(admin)
    await grants.revoke(admin, grant["id"])
    candidate = expert()
    candidate.payload["bindings"] = [binding("skill", "1.0.0")]
    with pytest.raises(Forbidden):
        await AgentManager(storage).add_definition(alice, candidate)
