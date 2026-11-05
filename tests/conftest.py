"""Every integration test owns a disposable PG schema, never production rows."""

from pathlib import Path
from uuid import uuid4

import pytest_asyncio
from dotenv import dotenv_values
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import create_async_engine

from egis_agentkit.storage import AgentKitStorage


@pytest_asyncio.fixture
async def storage():
    env = dotenv_values(
        Path(__file__).resolve().parents[3] / "deploy/agentkit-infra/pg.env"
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
    result = AgentKitStorage(str(url), engine)
    await result.migrate_enterprise()
    # Explicit native provisioning in this isolated test schema.
    from agentscope.app.storage import AsyncSQLAlchemyStorage

    async with AsyncSQLAlchemyStorage(str(url), engine=engine, create_tables=True):
        pass
    async with result:
        yield result
    await engine.dispose()
    async with admin.begin() as conn:
        await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
    await admin.dispose()
