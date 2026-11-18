from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from test_domain import COMPANY, expert, identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset, ResourceRef


async def prepare(storage):
    users, admin, alice, bob = await identities(storage)

    async def publish(version, revision, digest):
        async with storage.transaction() as conn:
            asset = Asset(
                resource_type="skill",
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
    review = await shares.submit(bob, "skill", "bob-asset", target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "")
    await publish("2.0.0", 2, "b" * 64)
    return users, admin, alice, bob, shares


def binding(version):
    return {
        "resource_type": "skill",
        "resource_id": "bob-asset",
        "resource_version": version,
        "resource_view_type": "shared",
    }


@pytest.mark.asyncio
async def test_new_binding_rejects_unreviewed_version(storage):
    _, _, alice, _, _ = await prepare(storage)
    value = expert()
    value.payload["bindings"] = [binding("2.0.0")]
    with pytest.raises(Forbidden):
        await AgentManager(storage).add_definition(alice, value)


@pytest.mark.asyncio
async def test_execution_rejects_stored_unreviewed_binding(storage):
    _, _, alice, _, _ = await prepare(storage)
    value = expert()
    value.payload["bindings"] = [binding("1.0.0")]
    row = await AgentManager(storage).add_definition(alice, value)
    value.payload["bindings"] = [binding("2.0.0")]
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
async def test_approved_parent_preserves_old_skill_after_new_share(storage, tmp_path):
    users, admin, alice, bob, shares = await prepare(storage)
    value = expert()
    value.payload["bindings"] = [binding("1.0.0")]
    row = await AgentManager(storage).add_definition(alice, value)
    parent = await shares.submit(
        alice, "agent", row["id"], target=COMPANY
    )
    await shares.decide(admin, parent["review_id"], True, "")
    later = await shares.submit(bob, "skill", "bob-asset")
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
    asset = await resources._asset("skill", "bob-asset", "1.0.0")
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
    _, admin, alice, _, _ = await prepare(storage)
    grants = GrantService(storage)
    (grant,) = await grants.list_grants(admin)
    await grants.revoke(admin, grant["id"])
    candidate = expert()
    candidate.payload["bindings"] = [binding("1.0.0")]
    with pytest.raises(Forbidden):
        await AgentManager(storage).add_definition(alice, candidate)


@pytest.mark.asyncio
async def test_shared_expert_uses_current_connector_configuration(storage, tmp_path):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    config = {"transport": "streamable_http", "url": "https://example.com/old"}
    await resources.save_mcp(admin, {"resource_id": "reports", "config": config})

    value = expert()
    value.payload["bindings"] = [
        {
            "resource_type": "mcp",
            "resource_id": "reports",
            "resource_view_type": "system",
        }
    ]
    agents = AgentManager(storage)
    row = await agents.add_definition(alice, value)
    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", row["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "")
    grants = await resources.grants.list_grants(admin)
    (connector_grant,) = [grant for grant in grants if grant["resource_type"] == "mcp"]
    assert connector_grant["principal_id"] == COMPANY.principal_id
    assert connector_grant["root_grant_id"] is None
    snapshot = await agents.get_definition_for_use(bob, row["id"], "shared")
    refs = snapshot["payload"]["bindings"]
    assert refs == [
        {"resource_type": "mcp", "resource_id": "reports", "resource_view_type": "system"}
    ]
    scope = SimpleNamespace(
        user_id=bob.user_id, root_definition=snapshot, definition=snapshot
    )
    (before,) = await resources.mcps_for_refs(bob, refs, authorization_scope=scope)
    assert before.mcp_config.url == config["url"]

    endpoint = "https://example.com/new"
    await resources.save_mcp(
        admin, {"resource_id": "reports", "config": {**config, "url": endpoint}}
    )
    (after,) = await resources.mcps_for_refs(bob, refs, authorization_scope=scope)
    assert after.mcp_config.url == endpoint
    assert after.name == before.name


@pytest.mark.parametrize(
    "fields", [{"resource_version": "1.0.0"}, {"resource_view_type": "shared"}]
)
def test_connector_binding_requires_current_id_only_format(fields):
    with pytest.raises(ValidationError):
        ResourceRef.model_validate({"resource_type": "mcp", "resource_id": "reports", **fields})
