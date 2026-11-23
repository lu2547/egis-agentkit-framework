"""项目文件改名和用户 Workspace 身份升级必须保留数据并允许重复执行。"""

import pytest
from sqlalchemy import text
from test_domain import expert, identities
from test_workspace_service import make_workspace

from egis_agentkit.client.nacos import load_agentkit_config
from egis_agentkit.core.workspace._identity import workspace_id_for
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.storage._schema import upgrade_schema
from egis_agentkit.storage.model import _tables as tables


def test_project_file_tables_and_config_belong_to_project():
    from pathlib import Path

    import yaml

    assert tables.file_changes.name == "ak_project_file_changes"
    assert tables.file_fingerprints.name == "ak_project_file_fingerprints"
    assert tables.reply_artifacts.name == "ak_project_reply_artifacts"
    source = Path("examples/nacos/agentkit.yaml").read_text()
    settings = load_agentkit_config(source)
    assert settings.project.upload.max_bytes > 0
    assert settings.project.context.max_files > 0
    assert settings.project.preview.max_concurrent > 0
    assert settings.workspace.files.win is False
    assert set(yaml.safe_load(source)["agentkit"]["workspace"]) == {
        "runtime",
        "readGuard",
        "probe",
        "lock",
        "files",
    }


@pytest.mark.asyncio
async def test_project_upgrade_retains_records_and_backfills_user_workspace(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    svc = make_workspace(storage, tmp_path)
    session = await svc.tasks.add_session(alice, source["id"], "private")
    pid, sid = session["project_id"], session["session_id"]
    async with svc.workspace.track_changes(alice.user_id, pid, sid, "reply"):
        svc.files.write(alice.user_id, pid, "report.md", b"report", area="artifacts")
    await svc.workspace.finalize_reply(alice.user_id, pid, sid, "reply")
    renames = {
        "ak_project_file_changes": "ak_workspace_file_changes",
        "ak_project_file_fingerprints": "ak_workspace_file_fingerprints",
        "ak_project_reply_artifacts": "ak_reply_artifacts",
    }
    async with storage.engine.begin() as conn:
        counts = {
            name: await conn.scalar(text(f'SELECT count(*) FROM "{name}"'))
            for name in renames
        }
        for current, old in renames.items():
            await conn.execute(text(f'ALTER TABLE "{current}" RENAME TO "{old}"'))
        await conn.execute(
            text(
                "UPDATE sessions SET payload=jsonb_set(payload::jsonb, '{config,workspace_id}', to_jsonb(CAST(:pid AS text)))::json WHERE id=:sid"
            ),
            {"pid": pid, "sid": sid},
        )
    with pytest.raises(ValueError, match="部署 SQL 脚本"):
        await storage.init_schema()
    preview = await upgrade_schema(storage.engine)
    assert preview.tables["Session Workspace 身份"] == "pending"
    applied = await upgrade_schema(
        storage.engine, database=preview.database, apply=True
    )
    assert applied.tables["Session Workspace 身份"] == "upgraded"
    await storage.init_schema()
    async with storage.engine.connect() as conn:
        assert {
            name: await conn.scalar(text(f'SELECT count(*) FROM "{name}"'))
            for name in renames
        } == counts
        for old in renames.values():
            assert (
                await conn.scalar(text("SELECT to_regclass(:name)"), {"name": old})
                is None
            )
    restored = await svc.tasks.get_session_record(alice, sid)
    assert restored.config.workspace_id == workspace_id_for(alice.user_id)
    assert restored.state.middle_context["egis_agentkit"]["project_id"] == pid
    assert [
        r["path"] for r in await svc.workspace.reply_artifacts(alice.user_id, pid, sid)
    ] == ["report.md"]
    repeated = await upgrade_schema(
        storage.engine, database=preview.database, apply=True
    )
    assert repeated.tables["Session Workspace 身份"] == "current"


@pytest.mark.asyncio
async def test_upgrade_consolidates_equivalent_indexes_from_both_previous_names(
    storage,
):
    """两次历史改名留下的重复索引，不能在最终统一命名时相互冲突。"""
    async with storage.engine.begin() as conn:
        # Current provisioning replaces this prefix index with the page index;
        # reproduce the historical schema explicitly for this migration test.
        await conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ak_project_file_changes_project_id ON ak_project_file_changes(project_id)"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE ak_project_file_changes RENAME TO ak_workspace_file_changes"
            )
        )
        await conn.execute(
            text(
                "ALTER INDEX ix_ak_project_file_changes_project_id RENAME TO ix_ak_file_changes_project_id"
            )
        )
        await conn.execute(
            text(
                "CREATE INDEX ix_ak_workspace_file_changes_project_id ON ak_workspace_file_changes(project_id)"
            )
        )
    preview = await upgrade_schema(storage.engine)
    await upgrade_schema(storage.engine, database=preview.database, apply=True)
    async with storage.engine.connect() as conn:
        indexes = (
            (
                await conn.execute(
                    text(
                        "SELECT indexname FROM pg_indexes WHERE schemaname=current_schema() AND tablename='ak_project_file_changes'"
                    )
                )
            )
            .scalars()
            .all()
        )
    assert "ix_ak_project_file_changes_project_id" in indexes
    assert "ix_ak_file_changes_project_id" not in indexes
    assert "ix_ak_workspace_file_changes_project_id" not in indexes
    assert (await upgrade_schema(storage.engine)).tables[
        "Session Workspace 身份"
    ] == "current"


@pytest.mark.asyncio
async def test_upgrade_rejects_same_named_index_with_different_definition(storage):
    async with storage.engine.begin() as conn:
        await conn.execute(
            text(
                "CREATE INDEX IF NOT EXISTS ix_ak_project_file_changes_project_id ON ak_project_file_changes(project_id)"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE ak_project_file_changes RENAME TO ak_workspace_file_changes"
            )
        )
        await conn.execute(
            text(
                "CREATE INDEX ix_ak_workspace_file_changes_project_id ON ak_workspace_file_changes(project_id,path)"
            )
        )
    preview = await upgrade_schema(storage.engine)
    with pytest.raises(ValueError, match="索引.*不一致"):
        await upgrade_schema(storage.engine, database=preview.database, apply=True)
    async with storage.engine.connect() as conn:
        assert await conn.scalar(
            text("SELECT to_regclass('ak_workspace_file_changes')")
        )
        assert await conn.scalar(
            text("SELECT to_regclass('ix_ak_workspace_file_changes_project_id')")
        )
        assert (
            await conn.scalar(text("SELECT to_regclass('ak_project_file_changes')"))
            is None
        )
