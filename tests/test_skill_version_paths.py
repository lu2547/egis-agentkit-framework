from pathlib import Path
from types import SimpleNamespace

import pytest
from test_domain import COMPANY, identities
from test_resources import archive, put_asset

from egis_agentkit.core.skill._archive import (
    calculate_archive_digest,
    calculate_files_digest,
)
from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import AgentDefinition, Grant
from egis_agentkit.storage.skill._version import SkillVersionStorage


@pytest.mark.parametrize(
    "owner_type, location",
    [
        ("system", "common/skills/report/1.0.0"),
        ("user", "user/alice/skills/report/1.0.0"),
    ],
)
def test_read_runtime_returns_verified_canonical_version(tmp_path, owner_type, location):
    versions = SkillVersionStorage(tmp_path)
    files = {"SKILL.md": b"# Report", "scripts/check.py": b"print('report')"}
    checksum = calculate_files_digest(files)
    versions.publish(
        {
            "resource_id": "report",
            "resource_version": "1.0.0",
            "owner_type": owner_type,
            "owner_id": "alice",
            "checksum_sha256": checksum,
        },
        files,
        max_bytes=1024,
    )
    source = versions.root / location
    assert versions.read_runtime(source, checksum=checksum, max_bytes=1024) == files
    with pytest.raises(Conflict):
        versions.read_runtime(source, checksum=checksum, max_bytes=8)
    (source / "SKILL.md").write_text("Changed")
    with pytest.raises(Conflict):
        versions.read_runtime(source, checksum=checksum, max_bytes=1024)


def test_read_runtime_rejects_other_workspace_directories(tmp_path):
    versions = SkillVersionStorage(tmp_path)
    source = versions.root / "user/alice/projects/report"
    source.mkdir(parents=True)
    files = {"SKILL.md": b"# Report"}
    (source / "SKILL.md").write_bytes(files["SKILL.md"])
    with pytest.raises(Forbidden):
        versions.read_runtime(
            source, checksum=calculate_files_digest(files), max_bytes=1024
        )


@pytest.mark.asyncio
async def test_bound_agents_resolve_the_same_system_version_directory(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(storage, checksum=calculate_archive_digest(archive()))
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
    assert first_path == tmp_path / "common/skills/report/1.0.0"
    c = await resources.materialize_skill(
        alice, "report", "1.0.0", authorization_scope=scope(alice, second, snap2)
    )
    assert Path(c.dir) == first_path
    assert not (tmp_path / "common" / first["id"]).exists()
    assert not (tmp_path / "common" / second["id"]).exists()


@pytest.mark.asyncio
async def test_personal_and_unscoped_access_resolve_the_owner_version_directory(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(
        storage,
        owner_type="user",
        owner_id="alice",
        checksum=calculate_archive_digest(archive()),
    )
    result = await resources.materialize_skill(alice, "report", "1.0.0")
    assert Path(result.dir) == tmp_path / "user/alice/skills/report/1.0.0"
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
async def test_skill_read_rejects_symlinked_user_ancestor(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await put_asset(
        storage,
        owner_type="user",
        owner_id="alice",
        checksum=calculate_archive_digest(archive()),
    )
    outside = tmp_path / "outside"
    (tmp_path / "user").rename(outside)
    (tmp_path / "user").symlink_to(outside, target_is_directory=True)
    with pytest.raises(Forbidden):
        await resources.materialize_skill(alice, "report", "1.0.0")
    assert (outside / "alice/skills/report/1.0.0/SKILL.md").is_file()


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
        publication_revision=1,
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
async def test_shared_skill_stays_at_owner_path_and_restores_exact_version(
    storage, tmp_path
):
    from egis_agentkit.service.access._share import ShareService
    from egis_agentkit.storage.model import ShareTarget

    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="report")
    with pytest.raises(Forbidden):
        await resources.materialize_skill(bob, "report", "1.0.0")
    sharing = ShareService(storage)
    review = await sharing.submit(
        alice, "skill", "report",
        target=ShareTarget(principal_type="user", principal_id="bob"),
    )
    await sharing.decide(admin, review["review_id"], True, "approved")

    version = await resources.materialize_skill(bob, "report", "1.0.0")
    canonical = tmp_path / "user/alice/skills/report/1.0.0"
    assert Path(version.dir) == canonical
    assert not (tmp_path / "common/skills/report").exists()
    assert not (tmp_path / "user/bob/skills/report").exists()
    pin = await resources.skill_pin(
        bob,
        {
            "resource_type": "skill",
            "resource_id": "report",
            "resource_version": "1.0.0",
            "resource_view_type": "shared",
        },
        version,
    )
    await resources.save_skill(
        alice,
        {
            "resource_id": "report",
            "resource_version": "1.1.0",
            "base_version": "1.0.0",
            "markdown": "# Newer version",
        },
    )
    restored = await resources.restore_skill(bob, pin, authorization_scope=None)
    assert Path(restored.dir) == canonical
    assert restored.markdown.endswith("# Report")
    (canonical / "SKILL.md").write_text("Changed")
    with pytest.raises(Conflict):
        await resources.restore_skill(bob, pin, authorization_scope=None)
