"""The maintenance CLI needs PG/NAS configuration, not a sandbox application."""

import importlib.util
import json
from pathlib import Path
from types import SimpleNamespace

import pytest
import yaml
from test_domain import identities

from egis_agentkit.client.nacos import load_agentkit_config
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import Asset


@pytest.fixture
def cli_config(storage, tmp_path, monkeypatch):
    script = Path(__file__).resolve().parents[1] / "scripts/publish_shared_skills.py"
    spec = importlib.util.spec_from_file_location("publish_shared_skills_cli", script)
    cli = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(cli)
    example = Path(__file__).resolve().parents[1] / "examples/nacos/agentkit.yaml"
    content = yaml.safe_load(example.read_text())
    content["agentkit"]["workspace"]["runtime"]["root"] = str(tmp_path)
    content["agentkit"]["workspace"]["runtime"]["sandbox"]["domain"] = None
    snapshot = load_agentkit_config(yaml.safe_dump(content))
    monkeypatch.delenv(snapshot.workspace.runtime.sandbox.api_key_env, raising=False)

    class NacosClient:
        def __init__(self, bootstrap):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    class ConfigProvider:
        def __init__(self, client):
            self.snapshot = snapshot

        async def refresh(self):
            pass

    monkeypatch.setattr(cli, "NacosClient", NacosClient)
    monkeypatch.setattr(cli, "NacosConfigProvider", ConfigProvider)
    monkeypatch.setattr(cli, "nacos_bootstrap", lambda: object())
    # Keep the real engine's disposable schema; never connect to configured deployment rows.
    monkeypatch.setattr(
        cli,
        "create_postgres_engine_from_config",
        lambda settings: storage.engine,
    )
    return cli


async def test_backfill_cli_previews_real_approved_rows_without_sandbox_credentials(
    storage, tmp_path, cli_config, capsys
):
    _, _, alice, _ = await identities(storage)
    async with storage.definition_write() as write:
        await write.assets.insert_asset(
            Asset(
                resource_type="skill",
                resource_id="report",
                resource_version="1.0.0",
                publication_revision=1,
                owner_type="user",
                owner_id=alice.user_id,
                checksum_sha256="a" * 64,
            ),
            actor=alice.user_id,
        )
    async with storage.transaction() as conn:
        await conn.execute(
            t.skill_summary.update()
            .where(t.skill_summary.c.skill_id == "report")
            .values(shared_version=1)
        )
    await cli_config.main(
        SimpleNamespace(app_dir=tmp_path, apply=False, database=None, skill_ids=None)
    )
    result = json.loads(capsys.readouterr().out)
    assert result == {
        "applied": False,
        "changes": [
            {
                "resource_id": "report",
                "resource_version": "1.0.0",
                "owner_id": "alice",
                "checksum_sha256": "a" * 64,
            }
        ],
    }
    assert not (tmp_path / "common").exists()
    assert (await storage.assets.get("skill", "report", "1.0.0"))["shared_version"] == 1


async def test_backfill_cli_rejects_wrong_database_before_publication(
    storage, tmp_path, cli_config
):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_skill(alice, {"resource_id": "report", "markdown": "# Report"})
    async with storage.transaction() as conn:
        await conn.execute(
            t.skill_summary.update()
            .where(t.skill_summary.c.skill_id == "report")
            .values(shared_version=1)
        )
    with pytest.raises(ValueError, match="--database"):
        await cli_config.main(
            SimpleNamespace(
                app_dir=tmp_path, apply=True, database="wrong-database", skill_ids=None
            )
        )
    assert not (tmp_path / "common").exists()
