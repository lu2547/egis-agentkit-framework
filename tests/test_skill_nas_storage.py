"""发布、读取及下载受管的技能版本目录。"""

import asyncio
from pathlib import Path

import pytest
from test_domain import identities
from test_resources import archive

from egis_agentkit.core.skill._archive import read_archive_files
from egis_agentkit.errors import Conflict
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import Grant


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "owner_type, location",
    [
        ("system", "common/skills/report/1.0.0"),
        ("user", "user/alice/skills/report/1.0.0"),
    ],
)
async def test_published_files_are_canonical_for_download_and_runtime(
    storage, tmp_path, owner_type, location
):
    _, admin, alice, _ = await identities(storage)
    user = admin if owner_type == "system" else alice
    root = tmp_path / "workspace"
    resources = ResourceService(storage, root)
    uploaded = archive()
    await resources.upload_skill(
        user, uploaded, "1.0.0", resource_id="report", owner_type=owner_type
    )
    destination = root / location
    assert destination.is_dir()
    assert (
        destination / "references/checklist.md"
    ).read_text() == "Actual attached content"
    assert not list(root.rglob("*.zip"))
    asset = await storage.assets.get("skill", "report", "1.0.0")
    assert not any(key.startswith("archive_") for key in asset["payload"])
    if owner_type == "system":
        await GrantService(storage).grant(
            admin,
            Grant(
                principal_type="user",
                principal_id=alice.user_id,
                resource_type="skill",
                resource_id="report",
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="system",
            ),
        )
    user = alice
    other_worker = ResourceService(storage, root)
    result = await other_worker.materialize_skill(user, "report", "1.0.0")
    assert Path(result.dir) == destination
    assert read_archive_files(
        await other_worker.download_skill(user, "report", "1.0.0")
    ) == read_archive_files(uploaded)
    (destination / "references/checklist.md").write_text("corrupt")
    with pytest.raises(Conflict):
        await other_worker.download_skill(user, "report", "1.0.0")
    with pytest.raises(Conflict):
        await other_worker.materialize_skill(user, "report", "1.0.0")


@pytest.mark.asyncio
async def test_failed_filesystem_publish_leaves_no_version_or_owner_grant(
    storage, tmp_path, monkeypatch
):
    import os

    from sqlalchemy import select

    from egis_agentkit.storage import tables as t

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)

    def fail(*args, **kwargs):
        raise OSError("NAS unavailable")

    monkeypatch.setattr(os, "rename", fail)
    with pytest.raises(OSError, match="NAS unavailable"):
        await resources.upload_skill(
            alice, archive(), "1.0.0", resource_id="failed-skill"
        )
    assert not list((tmp_path / "user/alice/skills/failed-skill").iterdir())
    assert await storage.assets.get("skill", "failed-skill", "1.0.0") is None
    async with storage.engine.connect() as conn:
        assert (
            await conn.execute(
                select(t.resource_permission).where(
                    t.resource_permission.c.resource_id == "failed-skill"
                )
            )
        ).first() is None


@pytest.mark.asyncio
async def test_competing_publications_leave_one_complete_skill_version(storage, tmp_path):
    _, _, alice, _ = await identities(storage)

    async def publish(name):
        resources = ResourceService(storage, tmp_path)
        return await resources.upload_skill(
            alice, archive(name=name), "1.0.0", resource_id="report"
        )

    results = await asyncio.gather(
        publish("first"), publish("second"), return_exceptions=True
    )
    [saved] = [result for result in results if isinstance(result, dict)]
    [failure] = [result for result in results if isinstance(result, BaseException)]
    assert isinstance(failure, Conflict)
    version = tmp_path / "user/alice/skills/report/1.0.0"
    contents = await ResourceService(storage, tmp_path).download_skill(
        alice, "report", "1.0.0"
    )
    assert read_archive_files(contents) == read_archive_files(
        archive(name=saved["payload"]["display_name"])
    )
    assert list(version.parent.iterdir()) == [version]
