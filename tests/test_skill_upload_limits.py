import io
import zipfile
from types import SimpleNamespace

import pytest
from pydantic import ValidationError
from test_domain import identities

from egis_agentkit.client.nacos._settings import ConnectorConfig, SkillArchiveConfig
from egis_agentkit.core.skill._archive import read_archive_files
from egis_agentkit.service.resource._catalog import ResourceService

CAP = 50 * 1024 * 1024


def bundle(size):
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_STORED) as archive:
        archive.writestr(
            "SKILL.md", "---\nname: limit-check\ndescription: test\n---\n# Test"
        )
        archive.writestr("references/data.bin", b"a" * size)
    return output.getvalue()


@pytest.mark.asyncio
async def test_skill_21mb_passes_upload_archive(storage, tmp_path):
    archive = bundle(21 * 1024 * 1024)
    assert SkillArchiveConfig().max_bytes == CAP - 1
    assert len(read_archive_files(archive)["references/data.bin"]) == 21 * 1024 * 1024
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.upload_skill(alice, archive, "1.0.0", resource_id="large")
    attachment = tmp_path / "user/alice/skills/large/1.0.0/references/data.bin"
    assert attachment.stat().st_size == 21 * 1024 * 1024


def test_skill_limit_rejects_exactly_50m_and_expansion_over_limit():
    with pytest.raises(ValueError, match="upload limit"):
        read_archive_files(b"x" * CAP)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("SKILL.md", "# test")
        archive.writestr("large.bin", b"a" * CAP)
    with pytest.raises(ValueError, match="Expanded"):
        read_archive_files(output.getvalue())
    with pytest.raises(ValidationError):
        SkillArchiveConfig(max_bytes=CAP)


def test_skill_archive_accepts_many_small_entries_without_count_limit():
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr(
            "SKILL.md", "---\nname: many-files\ndescription: test\n---\n# Test"
        )
        for index in range(20001):
            archive.writestr(f"references/{index}.txt", "x")
    content = output.getvalue()
    assert len(read_archive_files(content)) == 20002


@pytest.mark.asyncio
async def test_live_skill_limits_keep_expanded_cap_separate(storage, tmp_path):
    snapshot = SimpleNamespace(
        connector=ConnectorConfig(),
        skill=SimpleNamespace(
            archive=SkillArchiveConfig(max_bytes=2048, max_expanded_bytes=64)
        ),
    )
    resources = ResourceService(
        storage, tmp_path, config=SimpleNamespace(snapshot=snapshot)
    )
    with pytest.raises(ValueError, match="Expanded"):
        await resources.upload_skill(SimpleNamespace(), bundle(128), "1.0.0")
