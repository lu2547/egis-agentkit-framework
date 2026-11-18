"""Skill 表独立重置，其它数据库升级可在未配置 Skill 时执行。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from egis_agentkit_app.bootstrap import _provision
from test_app_http import app_snapshot

from egis_agentkit.client.nacos._settings import SkillConfig
from egis_agentkit.storage import _schema


@pytest.mark.asyncio
@pytest.mark.parametrize("apply", [False, True])
async def test_upgrade_preview_is_available_before_nas_configuration(
    tmp_path, monkeypatch, apply
):
    settings = app_snapshot(tmp_path).model_copy(update={"skill": SkillConfig()})
    provider = SimpleNamespace(snapshot=settings, refresh=AsyncMock())

    class Client:
        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            pass

    engine = SimpleNamespace(dispose=AsyncMock())
    operation = AsyncMock(return_value=_schema.SchemaReport("test", "public", {}))
    monkeypatch.setattr(_provision, "NacosClient", lambda *args: Client())
    monkeypatch.setattr(_provision, "NacosConfigProvider", lambda *args: provider)
    monkeypatch.setattr(
        _provision, "create_postgres_engine_from_config", lambda *args, **kwargs: engine
    )
    monkeypatch.setattr(_schema, "upgrade_schema", operation)
    await _provision.provision(
        "upgrade-db", database="test" if apply else None, apply=apply
    )
    operation.assert_awaited_once_with(
        engine, database="test" if apply else None, apply=apply
    )
    engine.dispose.assert_awaited_once()
