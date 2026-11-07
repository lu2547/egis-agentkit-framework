import pytest
from sqlalchemy.dialects.postgresql import insert
from test_domain import expert, identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.models import Asset, Grant, Group
from egis_agentkit.services import AgentService, PermissionService, ShareService
from egis_agentkit.service.access.snapshot import SnapshotService
from egis_agentkit.storage import tables as t


@pytest.mark.asyncio
async def test_group_revocation_does_not_use_stale_turn_user(storage):
    users, admin, alice, _ = await identities(storage)
    row = await AgentService(storage).save(admin, expert("sys", "system"))
    await users.save_group(admin, Group(group_id="g", group_name="研发"))
    alice.group_ids = ["g"]
    await users.update_user(admin, alice)
    permissions = PermissionService(storage)
    await permissions.grant(
        admin,
        Grant(
            principal_type="group",
            principal_id="g",
            resource_type="agent",
            resource_id=row["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="sys",
        ),
    )
    assert (await permissions.explain(alice, "agent", row["id"], "system")).is_allowed
    await users.update_user(admin, alice.model_copy(update={"group_ids": []}))
    assert not (
        await permissions.explain(alice, "agent", row["id"], "system")
    ).is_allowed


@pytest.mark.asyncio
async def test_unreviewed_binding_does_not_expand_shared_inherited_use(storage):
    _, admin, alice, bob = await identities(storage)
    permissions, agents = PermissionService(storage), AgentService(storage)
    async with storage.transaction() as conn:
        for name in ("a", "b"):
            asset = Asset(
                resource_type="skill",
                resource_id=name,
                resource_version="1.0.0",
                owner_type="system",
                owner_id="sys",
            )
            await conn.execute(
                insert(t.assets).values(
                    **asset.model_dump(), created_by="admin", updated_by="admin"
                )
            )
    for name in ("a", "b"):
        await permissions.grant(
            admin,
            Grant(
                principal_type="user",
                principal_id="alice",
                resource_type="skill",
                resource_id=name,
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="sys",
            ),
        )
    value = expert()
    value.payload["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": "a",
            "resource_version": "1.0.0",
            "resource_view_type": "system",
        }
    ]
    row = await agents.save(alice, value)
    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", row["id"])
    await shares.decide(admin, review["review_id"], True, "通过")
    value.id = row["id"]
    value.payload["bindings"][0]["resource_id"] = "b"
    await agents.save(alice, value)
    assert (await permissions.explain(bob, "skill", "a", "system")).is_allowed
    assert not (await permissions.explain(bob, "skill", "b", "system")).is_allowed


@pytest.mark.asyncio
async def test_private_binding_does_not_keep_revoked_external_permission(storage):
    _, admin, alice, _ = await identities(storage)
    asset = Asset(
        resource_type="skill",
        resource_id="external",
        resource_version="1.0.0",
        owner_type="system",
        owner_id="sys",
    )
    async with storage.transaction() as conn:
        await conn.execute(
            insert(t.assets).values(
                **asset.model_dump(), created_by="admin", updated_by="admin"
            )
        )
    permissions = PermissionService(storage)
    grant = await permissions.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id="alice",
            resource_type="skill",
            resource_id="external",
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="sys",
        ),
    )
    value = expert()
    value.payload["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": "external",
            "resource_version": "1.0.0",
            "resource_view_type": "system",
        }
    ]
    row = await AgentService(storage).save(alice, value)
    snapshot = await SnapshotService(storage).capture(row["id"], "private")
    await permissions.validate_snapshot(alice, snapshot)
    await permissions.revoke(admin, grant["id"])
    with pytest.raises(Forbidden):
        await permissions.validate_snapshot(alice, snapshot)


@pytest.mark.asyncio
async def test_shared_team_members_keep_distinct_pinned_resource_versions(monkeypatch):
    from egis_agentkit.service.access import snapshot as module

    async def raw_view(self, identifier, view, *, conn=None):
        return {
            "id": identifier,
            "definition_type": "agent",
            "config_version": 1,
            "payload": {"bindings": []},
        }

    from types import SimpleNamespace

    monkeypatch.setattr(module.DefinitionViews, "raw_view", raw_view)
    service = SnapshotService(SimpleNamespace(engine=None))

    async def capture(identifier, view, *, conn=None):
        version = {"first": "1.0.0", "second": "2.0.0"}[identifier]
        root = f"agent:shared:{identifier}"
        asset = f"skill:system:report:{version}"
        return {
            "root_key": root,
            "nodes": {
                root: {
                    "resource_type": "agent",
                    "resource_id": identifier,
                    "resource_view_type": "shared",
                    "data": {},
                    "edges": [asset],
                },
                asset: {
                    "resource_type": "skill",
                    "resource_id": "report",
                    "resource_view_type": "system",
                    "data": {"resource_version": version},
                    "edges": [],
                },
            },
        }

    monkeypatch.setattr(service, "capture", capture)
    team = {
        "definition_type": "team",
        "id": "team",
        "config_version": 1,
        "payload": {
            "orchestrator": {
                "definition_id": "leader",
                "resource_view_type": "private",
            },
            "members": [
                {"definition_id": i, "resource_view_type": "shared"}
                for i in ("first", "second")
            ],
        },
    }
    # Simulate existing frozen shared snapshots with the legacy keys currently generated by capture.
    original_capture = capture

    async def legacy_capture(*args, **kwargs):
        sub = await original_capture(*args, **kwargs)
        asset_key = next(k for k in sub["nodes"] if k.startswith("skill:"))
        node = sub["nodes"].pop(asset_key)
        legacy_key = "skill:system:report"
        sub["nodes"][legacy_key] = node
        sub["nodes"][sub["root_key"]]["edges"] = [legacy_key]
        return sub

    monkeypatch.setattr(service, "capture", legacy_capture)
    snapshot = {"nodes": {}, "source_versions": {}}
    await service._visit(team, "private", snapshot, set(), object())
    for identifier, version in [("first", "1.0.0"), ("second", "2.0.0")]:
        edge = snapshot["nodes"][f"agent:shared:{identifier}"]["edges"][0]
        assert snapshot["nodes"][edge]["data"]["resource_version"] == version


@pytest.mark.asyncio
async def test_team_capture_retains_both_versions_from_real_approved_experts(storage):
    from egis_agentkit.service.team.definition import TeamService

    _, admin, alice, bob = await identities(storage)
    async with storage.transaction() as conn:
        for version in ("1.0.0", "2.0.0"):
            asset = Asset(
                resource_type="skill",
                resource_id="report",
                resource_version=version,
                owner_type="system",
                owner_id="sys",
            )
            await conn.execute(
                insert(t.assets).values(
                    **asset.model_dump(), created_by="admin", updated_by="admin"
                )
            )
    permissions = PermissionService(storage)
    await permissions.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id="alice",
            resource_type="skill",
            resource_id="report",
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="sys",
        ),
    )
    agents, shares = AgentService(storage), ShareService(storage)
    members = []
    for name, version in [("first", "1.0.0"), ("second", "2.0.0")]:
        value = expert(name=name)
        value.payload["bindings"] = [
            {
                "resource_type": "skill",
                "resource_id": "report",
                "resource_version": version,
                "resource_view_type": "system",
            }
        ]
        saved = await agents.save(alice, value)
        review = await shares.submit(alice, "agent", saved["id"])
        await shares.decide(admin, review["review_id"], True, "approved")
        members.append({"definition_id": saved["id"], "resource_view_type": "shared"})
    team = await TeamService(storage).save(
        alice,
        {
            "team_name": "testteam",
            "team_display_name": "Test",
            "orchestrator_config": {
                "agent_name": "leader",
                "agent_display_name": "Leader",
            },
            "members": members,
        },
    )
    review = await shares.submit(alice, "team", team["id"])
    await shares.decide(admin, review["review_id"], True, "approved")
    snapshot = await SnapshotService(storage).capture(team["id"], "shared")
    for member, version in zip(members, ("1.0.0", "2.0.0")):
        key = "agent:shared:" + member["definition_id"]
        edge = snapshot["nodes"][key]["edges"][0]
        assert snapshot["nodes"][edge]["data"]["resource_version"] == version
    await permissions.validate_snapshot(bob, snapshot)


@pytest.mark.asyncio
async def test_asset_merge_rejects_different_content_for_same_version(storage):
    from egis_agentkit.errors import Conflict
    from egis_agentkit.service.access.snapshot import node_key

    _, admin, _, _ = await identities(storage)
    asset = Asset(
        resource_type="skill",
        resource_id="report",
        resource_version="1.0.0",
        owner_type="system",
        owner_id="sys",
        checksum_sha256="a" * 64,
    )
    async with storage.transaction() as conn:
        await conn.execute(
            insert(t.assets).values(
                **asset.model_dump(), created_by="admin", updated_by="admin"
            )
        )
    value = expert("sys", "system")
    value.payload["bindings"] = [
        {
            "resource_type": "skill",
            "resource_id": "report",
            "resource_version": "1.0.0",
            "resource_view_type": "system",
        }
    ]
    row = await AgentService(storage).save(admin, value)
    async with storage.engine.connect() as conn:
        snapshot = await SnapshotService(storage).capture(
            row["id"], "system", conn=conn
        )
        key = node_key("skill", "report", "system", "1.0.0")
        snapshot["nodes"][key]["data"]["checksum_sha256"] = "b" * 64
        del snapshot["nodes"][snapshot["root_key"]]
        with pytest.raises(Conflict):
            await SnapshotService(storage)._visit(row, "system", snapshot, set(), conn)
