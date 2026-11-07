from pathlib import Path
from types import SimpleNamespace

import pytest
from test_domain import identities
from test_resources import Registry, archive, put_asset, service

from egis_agentkit.errors import Forbidden
from egis_agentkit.models import Definition, Grant
from egis_agentkit.services import AgentService, PermissionService
from egis_agentkit.service.resource.service import skill_archive_digest
from egis_agentkit.service.access.snapshot import SnapshotService


@pytest.mark.asyncio
async def test_bound_agent_cache_shared_across_users_and_isolated_by_definition(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    await put_asset(storage, checksum=skill_archive_digest(archive()))
    agents = AgentService(storage)

    async def create(name, implementation=None):
        data = Definition(
            owner_type="system",
            owner_id="sys",
            agent_name=name,
            agent_display_name=name,
            payload={
                "bindings": [
                    {
                        "resource_type": "skill",
                        "resource_id": "report",
                        "resource_version": "1.0.0",
                        "resource_view_type": "system",
                    }
                ],
                "implementation": implementation,
            },
        )
        row = await agents.save(admin, data)
        await PermissionService(storage).grant(
            admin,
            Grant(
                principal_type="all_users",
                principal_id="*",
                resource_type="agent",
                resource_id=row["id"],
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="sys",
            ),
        )
        snapshot = await SnapshotService(storage).capture(row["id"], "system")
        return row, snapshot

    first, snap = await create(
        "first", {"assembly_ref": "demo", "assembly_version": "1.2.3"}
    )
    second, snap2 = await create("second")

    def scope(user, row, snapshot):
        return SimpleNamespace(
            user_id=user.user_id,
            definition=row,
            snapshot=snapshot,
            binding={"agent_node_key": snapshot["root_key"]},
        )

    async with http:
        a = await resources.materialize_skill(
            alice, "report", "1.0.0", authorization_scope=scope(alice, first, snap)
        )
        first_path = Path(a.dir)
        before = (first_path / "SKILL.md").stat().st_ino
        b = await resources.materialize_skill(
            bob, "report", "1.0.0", authorization_scope=scope(bob, first, snap)
        )
        assert Path(b.dir) == first_path
        assert (first_path / "SKILL.md").stat().st_ino == before
        assert (
            first_path.parent
            == tmp_path / "common" / first["id"] / "1.2.3" / "skills" / "report"
        )
        c = await resources.materialize_skill(
            alice, "report", "1.0.0", authorization_scope=scope(alice, second, snap2)
        )
        assert (
            Path(c.dir).parent
            == tmp_path / "common" / second["id"] / "config-1" / "skills" / "report"
        )
        assert c.dir != a.dir


@pytest.mark.asyncio
async def test_private_skill_personal_and_unscoped_use_user_cache(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    await put_asset(
        storage,
        owner_type="user",
        owner_id="alice",
        checksum=skill_archive_digest(archive()),
    )
    async with http:
        result = await resources.materialize_skill(alice, "report", "1.0.0")
        assert (
            Path(result.dir).parent == tmp_path / "user" / "alice" / "skills" / "report"
        )
        await resources.install(alice, "skill", "report", "1.0.0")
        personal = await AgentService(storage).personal(alice)
        snapshot = await SnapshotService(storage).capture(personal["id"], "private")
        scope = SimpleNamespace(
            user_id="alice",
            definition=personal,
            snapshot=snapshot,
            binding={"agent_node_key": snapshot["root_key"]},
        )
        assert (
            await resources.materialize_skill(
                alice, "report", "1.0.0", authorization_scope=scope
            )
        ).dir == result.dir


@pytest.mark.asyncio
async def test_skill_cache_rejects_symlinked_user_ancestor(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    await put_asset(
        storage,
        owner_type="user",
        owner_id="alice",
        checksum=skill_archive_digest(archive()),
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "user").symlink_to(outside, target_is_directory=True)
    async with http:
        with pytest.raises(Forbidden):
            await resources.materialize_skill(alice, "report", "1.0.0")
    assert not list(outside.iterdir())


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "rid,version", [("../escape", "1.0.0"), ("report", "../escape")]
)
async def test_persisted_unsafe_asset_paths_are_rejected(
    storage, tmp_path, rid, version
):
    from egis_agentkit.models import Asset
    from egis_agentkit.storage import tables as t

    _, _, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    asset = Asset(
        resource_type="skill",
        resource_id=rid,
        resource_version=version,
        owner_type="user",
        owner_id="alice",
    )
    async with storage.transaction() as conn:
        await conn.execute(
            t.assets.insert().values(
                **asset.model_dump(), created_by="alice", updated_by="alice"
            )
        )
    async with http:
        with pytest.raises((ValueError, Forbidden)):
            await resources.materialize_skill(alice, rid, version)
    assert not (tmp_path / "user").exists() and not (tmp_path / "common").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("id", "../escape"),
        ("config_version", "../escape"),
        ("assembly_version", "../escape"),
        ("id", ".."),
        ("assembly_version", "a\\b"),
    ],
)
async def test_unsafe_agent_cache_segments_are_rejected(
    storage, tmp_path, field, value
):
    from copy import deepcopy

    _, admin, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    await put_asset(storage, checksum=skill_archive_digest(archive()))
    row = await AgentService(storage).save(
        admin,
        Definition(
            owner_type="system",
            owner_id="sys",
            agent_name="agent",
            agent_display_name="Agent",
            payload={
                "bindings": [
                    {
                        "resource_type": "skill",
                        "resource_id": "report",
                        "resource_version": "1.0.0",
                        "resource_view_type": "system",
                    }
                ]
            },
        ),
    )
    await PermissionService(storage).grant(
        admin,
        Grant(
            principal_type="user",
            principal_id="alice",
            resource_type="agent",
            resource_id=row["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="sys",
        ),
    )
    snapshot = await SnapshotService(storage).capture(row["id"], "system")
    definition = deepcopy(row)
    if field == "assembly_version":
        definition["payload"]["implementation"] = {
            "assembly_ref": "demo",
            "assembly_version": value,
        }
    else:
        definition[field] = value
    scope = SimpleNamespace(
        user_id="alice",
        definition=definition,
        snapshot=snapshot,
        binding={"agent_node_key": snapshot["root_key"]},
    )
    async with http:
        with pytest.raises((ValueError, Forbidden)):
            await resources.materialize_skill(
                alice, "report", "1.0.0", authorization_scope=scope
            )
    assert not (tmp_path / "common").exists()


@pytest.mark.asyncio
async def test_bound_agent_cache_rejects_symlinked_definition_ancestor(
    storage, tmp_path
):
    _, admin, alice, _ = await identities(storage)
    resources, http = await service(storage, tmp_path, Registry())
    await put_asset(storage, checksum=skill_archive_digest(archive()))
    row = await AgentService(storage).save(
        admin,
        Definition(
            owner_type="system",
            owner_id="sys",
            agent_name="agent",
            agent_display_name="Agent",
            payload={
                "bindings": [
                    {
                        "resource_type": "skill",
                        "resource_id": "report",
                        "resource_version": "1.0.0",
                        "resource_view_type": "system",
                    }
                ]
            },
        ),
    )
    await PermissionService(storage).grant(
        admin,
        Grant(
            principal_type="user",
            principal_id="alice",
            resource_type="agent",
            resource_id=row["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="sys",
        ),
    )
    snapshot = await SnapshotService(storage).capture(row["id"], "system")
    scope = SimpleNamespace(
        user_id="alice",
        definition=row,
        snapshot=snapshot,
        binding={"agent_node_key": snapshot["root_key"]},
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "common").mkdir()
    (tmp_path / "common" / row["id"]).symlink_to(outside, target_is_directory=True)
    async with http:
        with pytest.raises(Forbidden):
            await resources.materialize_skill(
                alice, "report", "1.0.0", authorization_scope=scope
            )
    assert not list(outside.iterdir())


def cache_service_for_race(tmp_path):
    from egis_agentkit.service.resource.service import ResourceService

    service = ResourceService(None, None, None, None, tmp_path)
    asset = {
        "resource_type": "skill",
        "resource_id": "report",
        "resource_version": "1.0.0",
        "owner_type": "system",
        "owner_id": "sys",
        "checksum_sha256": skill_archive_digest(archive()),
        "is_enabled": True,
        "payload": {},
    }

    async def get_asset(*args, **kwargs):
        return asset

    async def require(*args, **kwargs):
        pass

    async def download(*args, **kwargs):
        return archive()

    service._asset, service._require, service.download_skill = (
        get_asset,
        require,
        download,
    )
    return service


def test_two_instances_concurrently_materialize_one_verified_cache(
    tmp_path, monkeypatch
):
    import asyncio
    import errno
    import os
    import threading
    from concurrent.futures import ThreadPoolExecutor

    from egis_agentkit.service.resource import service as module

    barrier = threading.Barrier(2)
    real_rename = os.rename

    def simultaneous_rename(source, destination):
        barrier.wait(timeout=5)
        try:
            return real_rename(source, destination)
        except OSError as exc:
            if exc.errno in (errno.EEXIST, errno.ENOTEMPTY):
                raise OSError(errno.ENOTEMPTY, "competing destination") from None
            raise

    monkeypatch.setattr(module.os, "rename", simultaneous_rename)
    services = [cache_service_for_race(tmp_path), cache_service_for_race(tmp_path)]
    user = SimpleNamespace(user_id="alice")

    def run(svc):
        result = asyncio.run(svc.materialize_skill(user, "report", "1.0.0"))
        path = Path(result.dir)
        return (
            path,
            (path / "SKILL.md").stat().st_ino,
            (path / "references/checklist.md").read_text(),
        )

    with ThreadPoolExecutor(max_workers=2) as pool:
        futures = [pool.submit(run, svc) for svc in services]
        first, second = [future.result(timeout=10) for future in futures]
    assert first == second
    assert first[2] == "Actual attached content"


@pytest.mark.asyncio
@pytest.mark.parametrize("winner", ["bad-content", "symlink"])
async def test_competing_invalid_cache_winner_is_rejected(
    tmp_path, monkeypatch, winner
):
    import errno

    from egis_agentkit.errors import Conflict
    from egis_agentkit.service.resource import service as module

    service = cache_service_for_race(tmp_path)
    outside = tmp_path / "outside"
    outside.mkdir()

    def collision(source, destination):
        destination = Path(destination)
        if winner == "symlink":
            destination.symlink_to(outside, target_is_directory=True)
        else:
            destination.mkdir()
            (destination / "SKILL.md").write_text("unexpected")
        raise OSError(errno.ENOTEMPTY, "competing destination")

    monkeypatch.setattr(module.os, "rename", collision)
    with pytest.raises((Forbidden, Conflict)):
        await service.materialize_skill(
            SimpleNamespace(user_id="alice"), "report", "1.0.0"
        )
