from pathlib import Path
from types import SimpleNamespace

import pytest
from test_domain import COMPANY, identities
from test_resources import archive, put_asset

from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.errors import Forbidden
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill import skill_archive_digest
from egis_agentkit.storage.model import AgentDefinition, Grant


@pytest.mark.asyncio
async def test_bound_agent_cache_shared_across_users_and_isolated_by_definition(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage, checksum=skill_archive_digest(archive()))
    agents = AgentManager(storage)

    async def create(name):
        data = AgentDefinition(
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
            },
        )
        row = await agents.add_definition(admin, data)
        await GrantService(storage).grant(
            admin,
            Grant(
                principal_type=COMPANY.principal_type,
                principal_id=COMPANY.principal_id,
                resource_type="agent",
                resource_id=row["id"],
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="sys",
            ),
        )
        snapshot = await storage.definitions.get_definition_view(row["id"], "system")
        return row, snapshot

    first, snap = await create("first")
    second, snap2 = await create("second")

    def scope(user, row, snapshot):
        return SimpleNamespace(
            user_id=user.user_id,
            definition=row,
            root_definition=snapshot,
            binding={"agent_definition_id": snapshot["id"]},
        )

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
        == tmp_path / "common" / first["id"] / "config-1" / "skills" / "report"
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
    resources = ResourceService(storage, tmp_path)
    await put_asset(
        storage,
        owner_type="user",
        owner_id="alice",
        checksum=skill_archive_digest(archive()),
    )
    result = await resources.materialize_skill(alice, "report", "1.0.0")
    assert Path(result.dir).parent == tmp_path / "user" / "alice" / "skills" / "report"
    await resources.install(alice, "skill", "report")
    personal = await PersonalManager(storage).get_or_add_definition(alice)
    snapshot = await storage.definitions.get_definition_view(personal["id"], "private")
    scope = SimpleNamespace(
        user_id="alice",
        definition=personal,
        root_definition=snapshot,
        binding={"agent_definition_id": snapshot["id"]},
    )
    assert (
        await resources.materialize_skill(
            alice, "report", "1.0.0", authorization_scope=scope
        )
    ).dir == result.dir


@pytest.mark.asyncio
async def test_skill_cache_rejects_symlinked_user_ancestor(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(
        storage,
        owner_type="user",
        owner_id="alice",
        checksum=skill_archive_digest(archive()),
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "user").symlink_to(outside, target_is_directory=True)
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
    from egis_agentkit.storage import tables as t
    from egis_agentkit.storage.model import Asset

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
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
    with pytest.raises((ValueError, Forbidden)):
        await resources.materialize_skill(alice, rid, version)
    assert not (tmp_path / "user").exists() and not (tmp_path / "common").exists()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field,value",
    [
        ("id", "../escape"),
        ("version", "../escape"),
        ("id", ".."),
    ],
)
async def test_unsafe_agent_cache_segments_are_rejected(
    storage, tmp_path, field, value
):
    from copy import deepcopy

    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage, checksum=skill_archive_digest(archive()))
    row = await AgentManager(storage).add_definition(
        admin,
        AgentDefinition(
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
    await GrantService(storage).grant(
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
    snapshot = await storage.definitions.get_definition_view(row["id"], "system")
    definition = deepcopy(row)
    definition[field] = value
    scope = SimpleNamespace(
        user_id="alice",
        definition=definition,
        root_definition=snapshot,
        binding={"agent_definition_id": snapshot["id"]},
    )
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
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage, checksum=skill_archive_digest(archive()))
    row = await AgentManager(storage).add_definition(
        admin,
        AgentDefinition(
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
    await GrantService(storage).grant(
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
    snapshot = await storage.definitions.get_definition_view(row["id"], "system")
    scope = SimpleNamespace(
        user_id="alice",
        definition=row,
        root_definition=snapshot,
        binding={"agent_definition_id": snapshot["id"]},
    )
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "common").mkdir()
    (tmp_path / "common" / row["id"]).symlink_to(outside, target_is_directory=True)
    with pytest.raises(Forbidden):
        await resources.materialize_skill(
            alice, "report", "1.0.0", authorization_scope=scope
        )
    assert not list(outside.iterdir())


def cache_service_for_race(tmp_path):
    storage = SimpleNamespace(assets=None, engine=None, mcp_credentials=None)
    service = ResourceService(storage, tmp_path)
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

    from egis_agentkit.service.resource import _catalog as module

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
    from egis_agentkit.service.resource import _catalog as module

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
