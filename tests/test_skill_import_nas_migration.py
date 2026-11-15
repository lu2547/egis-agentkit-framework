"""导入队列也只保存 NAS 文件定位，迁移失败时保留上传字节和任务状态。"""

import io

import pytest
from sqlalchemy import inspect, select, text
from test_domain import identities
from test_skill_imports import bundle

from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill_import import SkillImportService
from egis_agentkit.storage import tables as t


async def legacy_upload(storage):
    content = bundle()
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("ALTER TABLE ak_skill_imports DROP COLUMN archive_location")
        )
        await conn.execute(
            text("ALTER TABLE ak_skill_imports ADD COLUMN archive bytea")
        )
        await conn.execute(
            text("""
            INSERT INTO ak_skill_imports
                (job_id, owner_id, filename, category, avatar, resource_id, archive,
                 status, created_by, updated_by)
            VALUES ('queued', 'alice', 'test.zip', 'other', 'book', 'test', :archive,
                    'queued', 'alice', 'alice'),
                   ('done', 'alice', 'done.zip', 'other', 'book', 'done', NULL,
                    'succeeded', 'alice', 'alice')
        """),
            {"archive": content},
        )
    return content


@pytest.mark.asyncio
async def test_import_bytes_move_to_nas_without_changing_job_state(storage):
    from egis_agentkit.storage.migration._skill_imports import (
        migrate_skill_import_files,
    )

    content = await legacy_upload(storage)
    async with storage.engine.begin() as conn:
        schema = await conn.scalar(text("SELECT current_schema()"))
        assert await migrate_skill_import_files(conn, schema) == "pending"
        assert not storage.skill_archives.root.exists()
        assert (
            await migrate_skill_import_files(
                conn, schema, archive_store=storage.skill_archives, apply=True
            )
            == "upgraded"
        )
        columns = await conn.run_sync(
            lambda sync: inspect(sync).get_columns("ak_skill_imports")
        )
        assert "archive" not in {col["name"] for col in columns}
        assert await migrate_skill_import_files(conn, schema) == "current"
        jobs = (await conn.execute(select(t.skill_imports))).mappings().all()
    jobs = {job["job_id"]: job for job in jobs}
    assert jobs["queued"]["status"] == "queued"
    assert (
        storage.skill_archives.read_archive(jobs["queued"]["archive_location"])
        == content
    )
    assert jobs["done"]["status"] == "succeeded"
    assert jobs["done"]["archive_location"] is None


@pytest.mark.asyncio
async def test_import_nas_failure_preserves_database_bytes(storage, monkeypatch):
    from egis_agentkit.storage.migration._skill_imports import (
        migrate_skill_import_files,
    )

    content = await legacy_upload(storage)

    def fail(*args):
        raise OSError("NAS unavailable")

    monkeypatch.setattr(storage.skill_archives, "write_archive", fail)
    with pytest.raises(OSError, match="NAS unavailable"):
        async with storage.engine.begin() as conn:
            schema = await conn.scalar(text("SELECT current_schema()"))
            await migrate_skill_import_files(
                conn, schema, archive_store=storage.skill_archives, apply=True
            )
    async with storage.engine.connect() as conn:
        assert (
            bytes(
                await conn.scalar(
                    text("SELECT archive FROM ak_skill_imports WHERE job_id='queued'")
                )
            )
            == content
        )
        columns = await conn.run_sync(
            lambda sync: inspect(sync).get_columns("ak_skill_imports")
        )
        assert "archive_location" not in {col["name"] for col in columns}


@pytest.mark.asyncio
async def test_failed_nas_upload_does_not_enqueue_job(storage, tmp_path, monkeypatch):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    queue = SkillImportService(storage, resources)

    def fail(*args):
        raise OSError("NAS unavailable")

    monkeypatch.setattr(storage.skill_archives, "write_archive", fail)
    with pytest.raises(OSError, match="NAS unavailable"):
        await queue.enqueue(alice, io.BytesIO(bundle()), filename="test.zip")
    assert await queue.list(alice) == []


@pytest.mark.asyncio
async def test_import_failure_rolls_back_the_whole_schema_upgrade(storage, monkeypatch):
    from test_resources import archive
    from test_skill_nas_migration import legacy_packages

    from egis_agentkit.storage._schema import upgrade_schema

    await legacy_packages(storage, [("1.0.0", archive())])
    uploaded = await legacy_upload(storage)
    async with storage.engine.connect() as conn:
        database = await conn.scalar(text("SELECT current_database()"))
    original = storage.skill_archives.write_archive

    def fail_import(content):
        if content == uploaded:
            raise OSError("import NAS write failed")
        return original(content)

    monkeypatch.setattr(storage.skill_archives, "write_archive", fail_import)
    with pytest.raises(OSError, match="import NAS write failed"):
        await upgrade_schema(
            storage.engine,
            database=database,
            apply=True,
            skill_archives=storage.skill_archives,
        )
    async with storage.engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM ak_skill_archives")) == 1
        assert not await conn.scalar(
            text(
                "SELECT payload ? 'archive_path' FROM ak_resource_assets WHERE resource_type='skill'"
            )
        )
        assert (
            bytes(
                await conn.scalar(
                    text("SELECT archive FROM ak_skill_imports WHERE job_id='queued'")
                )
            )
            == uploaded
        )
    monkeypatch.setattr(storage.skill_archives, "write_archive", original)
    report = await upgrade_schema(
        storage.engine,
        database=database,
        apply=True,
        skill_archives=storage.skill_archives,
    )
    assert report.tables["技能文件存储"] == "upgraded"
    assert report.tables["技能导入文件"] == "upgraded"
    report = await upgrade_schema(storage.engine)
    assert report.tables["技能文件存储"] == "current"
    assert report.tables["技能导入文件"] == "current"
