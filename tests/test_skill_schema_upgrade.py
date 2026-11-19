"""Exercise the Skill schema upgrade against disposable PostgreSQL schemas."""

import pytest
from sqlalchemy import text

from egis_agentkit.storage._schema import upgrade_schema


async def test_upgrade_preserves_skill_revisions_shared_pointer_and_metadata(storage):
    async with storage.engine.begin() as conn:
        await conn.execute(text("DROP TABLE ak_skill_summary, ak_skill_detail"))
        await conn.execute(
            text("""
            INSERT INTO ak_resource_assets (
                resource_type, resource_id, resource_version, publication_revision,
                owner_type, owner_id, checksum_sha256, entrypoint, is_enabled,
                is_shared_current, payload, created_by, updated_by, created_at, updated_at
            ) VALUES
                ('skill', 'ledger', '8.0.0', 3, 'system', 'system', 'first', 'SKILL.md',
                 true, true, '{"display_name":"Ledger","description":"Exports entries",
                 "category":"finance","avatar":"book","avatar_checksum":"avatar-digest",
                 "files":[{"path":"SKILL.md","size": 20}],"import_job_id":"chat-job",
                 "uploader_type":"virtual","custom":{"key": 1}}',
                 'virtual-owner', 'reviewer', '2026-01-01 UTC', '2026-03-01 UTC'),
                ('skill', 'ledger', '2.0.0', 5, 'system', 'system', 'middle', NULL,
                 false, false, '{"display_name": null,"files":{"unusual": true},"avatar": false}',
                 'author', 'author', '2026-02-01 UTC', '2026-02-01 UTC'),
                ('skill', 'ledger', '1.0.0', 7, 'system', 'system', 'latest', NULL,
                 true, false, '{}', 'author', 'author', '2026-02-02 UTC', '2026-02-02 UTC'),
                ('tool', 'ledger', '8.0.0', NULL, 'system', 'system', 'tool', NULL,
                 true, false, '{}', 'author', 'author', '2026-01-02 UTC', '2026-01-02 UTC'),
                ('mcp', 'ledger', '1.0.0', 1, 'system', 'system', 'connector', NULL,
                 true, false, '{"managed_connector": true}',
                 'author', 'author', '2026-01-02 UTC', '2026-01-02 UTC')
        """)
        )
    database = storage.engine.url.database

    preview = await upgrade_schema(storage.engine)
    assert preview.tables["技能版本指针"] == "pending"
    async with storage.engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM ak_resource_assets")) == 5

    report = await upgrade_schema(storage.engine, database=database, apply=True)
    assert report.tables["技能版本指针"] == "upgraded"
    async with storage.engine.connect() as conn:
        summary = (
            (
                await conn.execute(
                    text("SELECT * FROM ak_skill_summary WHERE skill_id = 'ledger'")
                )
            )
            .mappings()
            .one()
        )
        assert (summary["owner_version"], summary["shared_version"]) == (7, 3)
        assert summary["is_enabled"] is True
        assert (summary["owner_type"], summary["owner_id"]) == ("system", "system")
        assert summary["source_user_id"] == summary["created_by"] == "virtual-owner"
        assert summary["updated_by"] == "reviewer"
        editions = (
            (await conn.execute(text("SELECT * FROM ak_skill_detail ORDER BY version")))
            .mappings()
            .all()
        )
        assert [
            (edition["version"], edition["resource_version"], edition["is_enabled"])
            for edition in editions
        ] == [(3, "8.0.0", True), (5, "2.0.0", False), (7, "1.0.0", True)]
        first = editions[0]
        assert (first["checksum_sha256"], first["entrypoint"]) == ("first", "SKILL.md")
        assert (first["display_name"], first["description"]) == (
            "Ledger",
            "Exports entries",
        )
        assert (first["category"], first["avatar"]) == ("finance", "book")
        assert first["avatar_checksum"] == "avatar-digest"
        assert first["files"] == [{"path": "SKILL.md", "size": 20}]
        assert first["import_job_id"] == "chat-job"
        assert first["payload"] == {"custom": {"key": 1}, "uploader_type": "virtual"}
        assert editions[1]["display_name"] is None
        assert editions[1]["avatar"] is None
        assert editions[1]["files"] is None
        assert editions[1]["payload"] == {
            "display_name": None,
            "avatar": False,
            "files": {"unusual": True},
        }
        assert summary["created_at"] == first["created_at"]
        assert summary["updated_at"] == first["updated_at"]
        remaining = await conn.execute(
            text(
                "SELECT resource_type, checksum_sha256, is_enabled FROM ak_resource_assets"
            )
        )
        assert set(remaining) == {("tool", "tool", True), ("mcp", "connector", True)}

    report = await upgrade_schema(storage.engine, database=database, apply=True)
    assert report.tables["技能版本指针"] == "current"


async def test_initialize_moves_legacy_skill_and_retains_unshared_state(storage):
    async with storage.engine.begin() as conn:
        await conn.execute(text("DROP TABLE ak_skill_summary, ak_skill_detail"))
        await conn.execute(
            text("""
            INSERT INTO ak_resource_assets (
                resource_type, resource_id, resource_version, publication_revision,
                owner_type, owner_id, checksum_sha256, is_enabled, payload, created_by, updated_by
            ) VALUES ('skill', 'my-skill', '3.1.0', 1, 'user', 'alice', 'digest',
                      true, '{}', 'alice', 'alice')
        """)
        )

    await storage.init_schema()

    async with storage.engine.connect() as conn:
        assert (
            await conn.execute(
                text("SELECT owner_version, shared_version FROM ak_skill_summary")
            )
        ).one() == (1, None)
        assert (
            await conn.scalar(text("SELECT resource_version FROM ak_skill_detail"))
            == "3.1.0"
        )
        assert await conn.scalar(text("SELECT count(*) FROM ak_resource_assets")) == 0


@pytest.mark.parametrize("drop_asset_pointers", [True, False])
async def test_skill_upgrade_runs_after_legacy_access_upgrade(
    storage, drop_asset_pointers
):
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("ALTER TABLE ak_resource_permission ADD COLUMN resource_version text")
        )
        if drop_asset_pointers:
            await conn.execute(
                text(
                    "ALTER TABLE ak_resource_assets DROP COLUMN publication_revision, DROP COLUMN is_shared_current"
                )
            )
        await conn.execute(
            text("""
            INSERT INTO ak_resource_assets (
                resource_type, resource_id, resource_version, owner_type, owner_id,
                checksum_sha256, is_enabled, payload, created_by, updated_by, created_at
            ) VALUES
                ('skill', 'my-skill', '9.0.0', 'user', 'alice', 'first', true,
                 '{}', 'alice', 'alice', '2026-01-01 UTC'),
                ('skill', 'my-skill', '1.0.0', 'user', 'alice', 'latest', true,
                 '{}', 'alice', 'alice', '2026-02-01 UTC')
        """)
        )
        if not drop_asset_pointers:
            await conn.execute(
                text("""
                UPDATE ak_resource_assets
                SET publication_revision = CASE resource_version WHEN '9.0.0' THEN 1 ELSE 2 END
            """)
            )
        await conn.execute(
            text("""
            INSERT INTO ak_resource_permission (
                id, principal_type, principal_id, resource_type, resource_id,
                resource_version, resource_view_type, resource_owner_type,
                resource_owner_id, grant_source_type, origin_key, granted_by, granted_at
            ) VALUES ('skill-grant', 'user', 'bob', 'skill', 'my-skill',
                '9.0.0', 'shared', 'user', 'alice', 'manual', 'manual', 'alice', now())
        """)
        )

    with pytest.raises(ValueError, match="upgrade-db"):
        await storage.init_schema()
    preview = await upgrade_schema(storage.engine)
    assert preview.tables["授权和共享版本"] == "pending"
    assert preview.tables["技能版本指针"] == "pending"
    await upgrade_schema(
        storage.engine, database=storage.engine.url.database, apply=True
    )
    async with storage.engine.connect() as conn:
        assert (
            await conn.execute(
                text("SELECT owner_version, shared_version FROM ak_skill_summary")
            )
        ).one() == (2, 1)
        assert (
            await conn.scalar(text("SELECT id FROM ak_resource_permission"))
            == "skill-grant"
        )


async def test_upgrade_preserves_legacy_rows_when_publication_order_is_missing(storage):
    async with storage.engine.begin() as conn:
        await conn.execute(
            text("""
            INSERT INTO ak_resource_assets (
                resource_type, resource_id, resource_version, owner_type, owner_id,
                checksum_sha256, is_enabled, payload, created_by, updated_by
            ) VALUES ('skill', 'unordered', '1.0.0', 'user', 'alice', 'digest',
                      true, '{}', 'alice', 'alice')
        """)
        )

    preview = await upgrade_schema(storage.engine)
    assert preview.tables["技能版本指针"] == "inconsistent"
    with pytest.raises(ValueError, match="技能版本指针"):
        await upgrade_schema(
            storage.engine, database=storage.engine.url.database, apply=True
        )
    async with storage.engine.connect() as conn:
        assert (
            await conn.scalar(text("SELECT resource_id FROM ak_resource_assets"))
            == "unordered"
        )
        assert await conn.scalar(text("SELECT count(*) FROM ak_skill_detail")) == 0
