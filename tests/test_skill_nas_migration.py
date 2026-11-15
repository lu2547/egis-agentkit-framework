"""旧技能 ZIP 只有完整迁移并校验后才能移除数据库二进制表。"""

import pytest
from sqlalchemy import text
from test_resources import archive

from egis_agentkit.client.nacos._settings import SkillArchiveConfig
from egis_agentkit.core.skill._archive import calculate_archive_digest
from egis_agentkit.errors import Conflict
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset


async def legacy_packages(storage, packages):
    async with storage.engine.begin() as conn:
        await conn.execute(
            text(
                "CREATE TABLE ak_skill_archives (checksum_sha256 varchar(64) PRIMARY KEY, archive bytea NOT NULL)"
            )
        )
        for version, content in packages:
            digest = calculate_archive_digest(content)
            await conn.execute(
                text(
                    "INSERT INTO ak_skill_archives VALUES (:digest,:content) ON CONFLICT DO NOTHING"
                ),
                {"digest": digest, "content": content},
            )
            asset = Asset(
                resource_type="skill",
                resource_id="legacy-report",
                resource_version=version,
                owner_type="user",
                owner_id="alice",
                checksum_sha256=digest,
                payload={"original_markdown": "legacy body", "description": "Legacy"},
            )
            await conn.execute(
                t.assets.insert().values(
                    **asset.model_dump(), created_by="test", updated_by="test"
                )
            )


@pytest.mark.asyncio
async def test_existing_versions_move_to_nas_before_binary_table_is_removed(storage):
    from egis_agentkit.storage.migration._skill_archives import migrate_skill_archives

    await legacy_packages(
        storage, [("1.0.0", archive()), ("1.1.0", archive(extra="references/new.md"))]
    )
    async with storage.engine.begin() as conn:
        schema = await conn.scalar(text("SELECT current_schema()"))
        assert await migrate_skill_archives(conn, schema) == "pending"
        assert not storage.skill_archives.root.exists()
        result = await migrate_skill_archives(
            conn,
            schema,
            archive_store=storage.skill_archives,
            limits=SkillArchiveConfig(),
            apply=True,
        )
        assert result == "upgraded"
        assert (
            await conn.scalar(text("SELECT to_regclass('ak_skill_archives')")) is None
        )
        assert (
            await migrate_skill_archives(
                conn,
                schema,
                archive_store=storage.skill_archives,
                limits=SkillArchiveConfig(),
                apply=True,
            )
            == "current"
        )
    for version in ("1.0.0", "1.1.0"):
        asset = await storage.assets.get("skill", "legacy-report", version)
        assert "original_markdown" not in asset["payload"]
        assert (
            calculate_archive_digest(
                storage.skill_archives.read_archive(asset["payload"])
            )
            == asset["checksum_sha256"]
        )


@pytest.mark.asyncio
async def test_file_failure_preserves_old_table_and_allows_retry(storage, monkeypatch):
    from egis_agentkit.storage.migration._skill_archives import migrate_skill_archives

    await legacy_packages(
        storage, [("1.0.0", archive()), ("1.1.0", archive(extra="references/new.md"))]
    )
    original = storage.skill_archives.write_archive
    attempts = []

    def fail_second(content):
        attempts.append(content)
        if len(attempts) == 2:
            raise OSError("NAS write failed")
        return original(content)

    monkeypatch.setattr(storage.skill_archives, "write_archive", fail_second)
    with pytest.raises(OSError, match="NAS write failed"):
        async with storage.engine.begin() as conn:
            schema = await conn.scalar(text("SELECT current_schema()"))
            await migrate_skill_archives(
                conn,
                schema,
                archive_store=storage.skill_archives,
                limits=SkillArchiveConfig(),
                apply=True,
            )
    async with storage.engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM ak_skill_archives")) == 2
        assert (
            await conn.scalar(
                text(
                    "SELECT count(*) FROM ak_resource_assets WHERE payload ? 'archive_path'"
                )
            )
            == 0
        )
    monkeypatch.setattr(storage.skill_archives, "write_archive", original)
    async with storage.engine.begin() as conn:
        schema = await conn.scalar(text("SELECT current_schema()"))
        assert (
            await migrate_skill_archives(
                conn,
                schema,
                archive_store=storage.skill_archives,
                limits=SkillArchiveConfig(),
                apply=True,
            )
            == "upgraded"
        )


@pytest.mark.asyncio
async def test_corrupt_legacy_package_stops_migration_without_dropping_source(storage):
    from egis_agentkit.storage.migration._skill_archives import migrate_skill_archives

    await legacy_packages(storage, [("1.0.0", archive())])
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("UPDATE ak_skill_archives SET checksum_sha256=:bad"), {"bad": "0" * 64}
        )
    with pytest.raises((Conflict, ValueError)):
        async with storage.engine.begin() as conn:
            schema = await conn.scalar(text("SELECT current_schema()"))
            await migrate_skill_archives(
                conn,
                schema,
                archive_store=storage.skill_archives,
                limits=SkillArchiveConfig(),
                apply=True,
            )
    async with storage.engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM ak_skill_archives")) == 1
