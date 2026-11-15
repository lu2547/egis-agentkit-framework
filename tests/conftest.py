"""Every integration test owns a disposable PG schema, never production rows."""

import os
from pathlib import Path
from uuid import uuid4

import pytest
import pytest_asyncio
from dotenv import dotenv_values
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from egis_agentkit.storage import AgentKitStorage


@pytest_asyncio.fixture
async def storage(tmp_path):
    env = dotenv_values(
        Path(
            os.environ.get(
                "AGENTKIT_TEST_PG_ENV",
                str(
                    Path(__file__).resolve().parents[3] / "deploy/agentkit-infra/pg.env"
                ),
            )
        )
    )
    url = URL.create(
        "postgresql+asyncpg",
        username=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
        host="127.0.0.1",
        port=5433,
        database=env["POSTGRES_DB"],
    )
    schema = "ak_test_" + uuid4().hex
    admin = create_async_engine(url, hide_parameters=True)
    async with admin.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    engine = create_async_engine(
        url,
        hide_parameters=True,
        connect_args={"server_settings": {"search_path": schema}},
    )
    from egis_agentkit.client.nacos._settings import SkillArchiveConfig
    from egis_agentkit.storage.skill._archive import SkillArchiveStorage

    archives = SkillArchiveStorage(
        tmp_path / "nas", max_bytes=SkillArchiveConfig().max_bytes
    )
    result = AgentKitStorage(str(url), engine, skill_archives=archives)
    await result.init_schema()
    async with result:
        yield result
    await engine.dispose()
    async with admin.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await admin.dispose()


@pytest.fixture(autouse=True)
def sandbox_test_credentials(monkeypatch):
    """只供显式 TEST_SANDBOX_KEY 配置使用，绝不读取真实部署密钥。"""
    monkeypatch.setenv("TEST_SANDBOX_KEY", "test-only")
