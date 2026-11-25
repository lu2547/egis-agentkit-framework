"""Shared versions survive an author NAS being absent, with no source fallback."""

import shutil
from pathlib import Path

import pytest
from test_domain import identities
from test_resources import archive

from egis_agentkit.core.skill._archive import calculate_files_digest, read_archive_files
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import ShareTarget
from egis_agentkit.storage.skill._version import SkillVersionStorage


def test_shared_locator_keeps_owner_and_rejects_unknown_scope(tmp_path):
    versions = SkillVersionStorage(tmp_path)
    files = {"SKILL.md": b"# report", "scripts/run.py": b"print(1)"}
    asset = dict(
        resource_id="report",
        resource_version="1.0.0",
        owner_type="user",
        owner_id="alice",
        checksum_sha256=calculate_files_digest(files),
    )
    versions.publish(asset, files, max_bytes=1024)
    versions.publish_shared(asset, max_bytes=1024)
    shared = {**asset, "storage_scope": "shared"}
    path, stored, _ = versions.read(shared, max_bytes=1024)
    assert path == tmp_path / "common/skills/report/1.0.0"
    assert stored == files
    assert asset["owner_id"] == "alice"
    shutil.rmtree(tmp_path / "user")
    versions.publish_shared(asset, max_bytes=1024)
    assert versions.read(shared, max_bytes=1024)[1] == files
    with pytest.raises(Forbidden):
        versions.read({**asset, "storage_scope": "../escape"}, max_bytes=1024)
    with pytest.raises(NotFound):
        versions.read(asset, max_bytes=1024)


def test_shared_publication_rejects_conflicts_and_symlinks(tmp_path):
    versions = SkillVersionStorage(tmp_path)
    files = {"SKILL.md": b"# report"}
    asset = dict(
        resource_id="report",
        resource_version="1.0.0",
        owner_type="user",
        owner_id="alice",
        checksum_sha256=calculate_files_digest(files),
    )
    versions.publish(asset, files, max_bytes=1024)
    versions.publish_shared(asset, max_bytes=1024)
    (tmp_path / "common/skills/report/1.0.0/SKILL.md").write_bytes(b"tampered")
    with pytest.raises(Conflict):
        versions.publish_shared(asset, max_bytes=1024)
    shutil.rmtree(tmp_path / "common")
    outside = tmp_path / "outside"
    outside.mkdir()
    (tmp_path / "common").symlink_to(outside, target_is_directory=True)
    with pytest.raises(Forbidden):
        versions.publish_shared(asset, max_bytes=1024)
    assert not list(outside.iterdir())


async def test_shared_read_download_hub_and_restore_use_only_common_on_second_nas(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    author_nas = tmp_path / "author-nas"
    resources = ResourceService(storage, author_nas)
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="report")
    asset = await storage.assets.get("skill", "report", "1.0.0")
    await resources.publish_shared_skill(asset)
    sharing = ShareService(storage, publish_skill=resources.publish_shared_skill)
    review = await sharing.submit(
        alice,
        "skill",
        "report",
        target=ShareTarget(principal_type="user", principal_id="bob"),
    )
    await sharing.decide(admin, review["review_id"], True, "approved")
    other_nas = tmp_path / "other-nas"
    shutil.copytree(author_nas / "common", other_nas / "common")
    other = ResourceService(storage, other_nas)
    version = await other.materialize_skill(bob, "report", "1.0.0")
    assert Path(version.dir) == other_nas / "common/skills/report/1.0.0"
    assert read_archive_files(
        await other.download_skill(bob, "report", "1.0.0")
    ) == read_archive_files(archive())
    assert (await other.detail(bob, "skill", "report", "1.0.0"))["payload"][
        "markdown"
    ].endswith("# Report")
    card = await other.skill_hub.get_skill_version("bob", "report", "1.0.0")
    assert card.markdown.endswith("# Report")
    pin = await other.skill_pin(
        bob,
        dict(
            resource_type="skill",
            resource_id="report",
            resource_version="1.0.0",
            resource_view_type="shared",
        ),
        version,
    )
    assert (
        await other.restore_skill(bob, pin, authorization_scope=None)
    ).dir == version.dir
    assert not (other_nas / "user").exists()
    assert (await storage.assets.get("skill", "report", "1.0.0"))["owner_id"] == "alice"
    with pytest.raises(NotFound):
        await other.materialize_skill(alice, "report", "1.0.0")


async def test_shared_read_never_silently_uses_unpublished_author_source(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="report")
    sharing = ShareService(storage, publish_skill=resources.publish_shared_skill)
    review = await sharing.submit(
        alice,
        "skill",
        "report",
        target=ShareTarget(principal_type="user", principal_id="bob"),
    )
    await sharing.decide(admin, review["review_id"], True, "approved")
    if (tmp_path / "common").exists():
        shutil.rmtree(tmp_path / "common")
    with pytest.raises(NotFound):
        await resources.materialize_skill(bob, "report", "1.0.0")
    assert (tmp_path / "user/alice/skills/report/1.0.0/SKILL.md").exists()


async def test_explicit_backfill_previews_and_publishes_approved_versions_only(
    storage, tmp_path
):
    from egis_agentkit.storage import tables as t
    from egis_agentkit.storage.migration import _shared_skills

    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="report")
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="private")
    sharing = ShareService(storage, publish_skill=resources.publish_shared_skill)
    review = await sharing.submit(
        alice,
        "skill",
        "report",
        target=ShareTarget(principal_type="user", principal_id="bob"),
    )
    await sharing.decide(admin, review["review_id"], True, "approved")
    if (tmp_path / "common").exists():
        shutil.rmtree(tmp_path / "common")
    before = await storage.assets.get("skill", "report", "1.0.0")
    preview = await _shared_skills.backfill_shared_skills(resources)
    assert [(row["resource_id"], row["resource_version"]) for row in preview] == [
        ("report", "1.0.0")
    ]
    assert not (tmp_path / "common").exists()
    assert (
        await _shared_skills.backfill_shared_skills(resources, resource_ids=["private"])
        == []
    )
    assert await _shared_skills.backfill_shared_skills(resources, apply=True) == preview
    assert await _shared_skills.backfill_shared_skills(resources, apply=True) == preview
    assert (tmp_path / "common/skills/report/1.0.0/SKILL.md").is_file()
    assert not (tmp_path / "common/skills/private").exists()
    assert await storage.assets.get("skill", "report", "1.0.0") == before
    async with storage.engine.connect() as conn:
        status = await conn.scalar(
            t.approvals.select()
            .with_only_columns(t.approvals.c.review_status_type)
            .where(t.approvals.c.review_id == review["review_id"])
        )
    assert status == "approved"


async def test_failed_publication_keeps_review_pending_and_retries_without_grants(
    storage, tmp_path, monkeypatch
):
    import errno
    import os

    from sqlalchemy import select

    from egis_agentkit.errors import AgentKitError
    from egis_agentkit.storage import tables as t

    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="report")
    shares = ShareService(storage, publish_skill=resources.publish_shared_skill)
    review = await shares.submit(
        alice,
        "skill",
        "report",
        target=ShareTarget(principal_type="user", principal_id="bob"),
    )

    def fail(*args, **kwargs):
        raise OSError(errno.EIO, "NAS unavailable")

    with monkeypatch.context() as patch:
        patch.setattr(os, "rename", fail)
        with pytest.raises(AgentKitError) as error:
            await shares.decide(admin, review["review_id"], True, "")
        assert error.value.status_code == 503
    asset = await storage.assets.get("skill", "report", "1.0.0")
    assert asset["shared_version"] is None
    async with storage.engine.connect() as conn:
        status = await conn.scalar(
            select(t.approvals.c.review_status_type).where(
                t.approvals.c.review_id == review["review_id"]
            )
        )
        grants = (
            await conn.execute(
                select(t.resource_permission).where(
                    t.resource_permission.c.resource_id == "report",
                    t.resource_permission.c.grant_source_type != "owner",
                )
            )
        ).all()
    assert status == "pending" and grants == []
    await shares.decide(admin, review["review_id"], True, "retried")
    assert (
        Path((await resources.materialize_skill(bob, "report", "1.0.0")).dir)
        == tmp_path / "common/skills/report/1.0.0"
    )


async def test_owner_restores_shared_pin_and_avatar_without_private_directory(
    storage, tmp_path
):
    import hashlib

    from egis_agentkit.storage import tables as t

    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="report")
    asset = await storage.assets.get("skill", "report", "1.0.0")
    avatar = b'<svg xmlns="http://www.w3.org/2000/svg"/>'
    resources.skill_versions.write_avatar(asset, avatar)
    async with storage.transaction() as conn:
        await conn.execute(
            t.skill_detail.update()
            .where(t.skill_detail.c.skill_id == "report")
            .values(avatar_checksum=hashlib.sha256(avatar).hexdigest())
        )
    shares = ShareService(storage, publish_skill=resources.publish_shared_skill)
    review = await shares.submit(
        alice,
        "skill",
        "report",
        target=ShareTarget(principal_type="user", principal_id="bob"),
    )
    await shares.decide(admin, review["review_id"], True, "")
    private = await resources.materialize_skill(alice, "report", "1.0.0")
    pin = await resources.skill_pin(
        alice,
        dict(
            resource_type="skill",
            resource_id="report",
            resource_version="1.0.0",
            resource_view_type="shared",
        ),
        private,
    )
    shutil.rmtree(tmp_path / "user")
    assert (
        Path((await resources.restore_skill(alice, pin, authorization_scope=None)).dir)
        == tmp_path / "common/skills/report/1.0.0"
    )
    assert await resources.read_skill_avatar(bob, "report", "1.0.0") == avatar


async def test_owned_share_metadata_identifies_approved_version_after_private_update(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.upload_skill(alice, archive(), "1.0.0", resource_id="report")
    initial = await resources.detail(alice, "skill", "report", "1.0.0")
    assert (
        initial["share_version"] == "1.0.0" and initial["share_version_label"] == "1.0"
    )
    shares = ShareService(storage, publish_skill=resources.publish_shared_skill)
    review = await shares.submit(
        alice,
        "skill",
        "report",
        target=ShareTarget(principal_type="user", principal_id="bob"),
    )
    await shares.decide(admin, review["review_id"], True, "")
    await resources.save_skill(
        alice,
        dict(
            resource_id="report",
            resource_version="2.0.0",
            base_version="1.0.0",
            markdown="# Private update",
        ),
    )
    page = await resources.catalog_page(alice, "skill")
    item = page["items"][0]
    assert item["resource_version"] == "2.0.0"
    assert item["share_version"] == "1.0.0" and item["share_version_label"] == "1.0"
