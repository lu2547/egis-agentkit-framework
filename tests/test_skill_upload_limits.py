import io
import zipfile
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from egis_agentkit.backend.nacos._settings import ConnectorConfig, SkillArchiveConfig
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill import _archive_files

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
    assert len(_archive_files(archive)["references/data.bin"]) == 21 * 1024 * 1024
    resources = ResourceService(storage, tmp_path)

    async def save(user, kind, data, *, files, archive_limits):
        return {"size": len(files["references/data.bin"])}

    resources._save = save
    assert (await resources.upload_skill(SimpleNamespace(), archive, "1.0.0"))[
        "size"
    ] == 21 * 1024 * 1024


def test_skill_limit_rejects_exactly_50m_and_expansion_over_limit():
    with pytest.raises(ValueError, match="upload limit"):
        _archive_files(b"x" * CAP)
    output = io.BytesIO()
    with zipfile.ZipFile(output, "w", compression=zipfile.ZIP_DEFLATED) as archive:
        archive.writestr("SKILL.md", "# test")
        archive.writestr("large.bin", b"a" * CAP)
    with pytest.raises(ValueError, match="Expanded"):
        _archive_files(output.getvalue())
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
    assert len(_archive_files(content)) == 20002


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
