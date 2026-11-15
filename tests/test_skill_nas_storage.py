"""技能版本包写受管共享文件目录，数据库只保存包定位与摘要。"""

import asyncio
import hashlib
from pathlib import Path

import pytest
from sqlalchemy import inspect, select
from test_domain import identities
from test_resources import archive

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage import tables as t


def archive_storage(root):
    from egis_agentkit.storage.skill._archive import SkillArchiveStorage

    return SkillArchiveStorage(root, max_bytes=1024 * 1024)


def test_archive_is_atomically_written_and_verified(tmp_path):
    store = archive_storage(tmp_path / "nas")
    content = archive()
    location = store.write_archive(content)
    assert location["archive_sha256"] == hashlib.sha256(content).hexdigest()
    assert location["archive_size_bytes"] == len(content)
    assert store.read_archive(location) == content
    (store.root / location["archive_path"]).write_bytes(b"corrupt")
    with pytest.raises(Conflict):
        store.read_archive(location)


@pytest.mark.asyncio
async def test_same_package_has_one_complete_file_under_concurrent_writes(tmp_path):
    store = archive_storage(tmp_path / "nas")
    content = archive()
    locations = await asyncio.gather(
        *(asyncio.to_thread(store.write_archive, content) for _ in range(5))
    )
    assert all(location == locations[0] for location in locations)
    assert len(list(store.root.glob("*.zip"))) == 1
    assert not list(store.root.glob(".upload-*"))


def test_symlink_archive_cannot_be_read_or_overwritten(tmp_path):
    store = archive_storage(tmp_path / "nas")
    content = archive()
    digest = hashlib.sha256(content).hexdigest()
    store.root.mkdir()
    outside = tmp_path / "outside.zip"
    outside.write_bytes(content)
    (store.root / (digest + ".zip")).symlink_to(outside)
    with pytest.raises((Conflict, Forbidden)):
        store.write_archive(content)
    assert outside.read_bytes() == content


def test_failed_publish_does_not_leave_a_partial_package(tmp_path, monkeypatch):
    import os

    store = archive_storage(tmp_path / "nas")

    def fail(*args, **kwargs):
        raise OSError("NAS unavailable")

    monkeypatch.setattr(os, "replace", fail)
    with pytest.raises(OSError):
        store.write_archive(archive())
    assert not list(store.root.glob("*.zip"))
    assert not list(store.root.glob(".upload-*"))


@pytest.mark.asyncio
async def test_upload_download_and_cold_materialization_use_nas_metadata(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    saved = await resources.upload_skill(alice, archive(), "1.0.0")
    asset = await storage.assets.get("skill", saved["resource_id"], "1.0.0")
    payload = asset["payload"]
    assert {"archive_path", "archive_size_bytes", "archive_sha256"} <= payload.keys()
    assert storage.skill_archives.read_archive(payload)
    async with storage.engine.connect() as conn:
        assert "ak_skill_archives" not in await conn.run_sync(
            lambda sync: inspect(sync).get_table_names()
        )
    cold = ResourceService(storage, tmp_path / "another-node-cache")
    skill = await cold.materialize_skill(alice, saved["resource_id"], "1.0.0")
    assert (Path(skill.dir) / "SKILL.md").is_file()
    (storage.skill_archives.root / payload["archive_path"]).write_bytes(b"corrupt")
    with pytest.raises(Conflict):
        await cold.download_skill(alice, saved["resource_id"], "1.0.0")


@pytest.mark.asyncio
async def test_nas_failure_rolls_back_published_version_and_owner_grant(
    storage, tmp_path, monkeypatch
):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)

    def fail(*args, **kwargs):
        raise OSError("NAS unavailable")

    monkeypatch.setattr(storage.skill_archives, "write_archive", fail)
    with pytest.raises(OSError):
        await resources.upload_skill(
            alice, archive(), "1.0.0", resource_id="failed-skill"
        )
    assert await storage.assets.get("skill", "failed-skill", "1.0.0") is None
    async with storage.engine.connect() as conn:
        assert (
            await conn.execute(
                select(t.resource_permission).where(
                    t.resource_permission.c.resource_id == "failed-skill"
                )
            )
        ).first() is None


def test_skill_storage_requires_an_explicit_absolute_nas_root():
    from pydantic import ValidationError

    from egis_agentkit.client.nacos._settings import SkillConfig, SkillStorageConfig
    from egis_agentkit.storage.skill._archive import SkillArchiveStorage

    with pytest.raises(ValueError, match="NAS"):
        SkillArchiveStorage.from_settings(SkillConfig())
    with pytest.raises(ValidationError):
        SkillStorageConfig(root="relative/skills")
