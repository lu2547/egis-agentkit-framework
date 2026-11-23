"""Run replay-only cluster integrations against scoped keys on local Redis."""

import os
from contextlib import AsyncExitStack
from uuid import uuid4

import pytest
import pytest_asyncio

from egis_agentkit.client.nacos._settings import RedisConfig
from egis_agentkit.core.message_bus import EgisMessageBus


@pytest.fixture
def redis_config():
    if os.environ.get("EGIS_BUS_INTEGRATION") != "1":
        pytest.skip("set EGIS_BUS_INTEGRATION=1 for local Redis")
    return RedisConfig(host="127.0.0.1", port=6380, db=14)


@pytest_asyncio.fixture
async def redis_buses(storage, redis_config):
    async with AsyncExitStack() as stack:
        buses = []
        for _ in range(2):
            bus = EgisMessageBus(
                deployment_mode="cluster", engine=storage.engine, redis=redis_config
            )
            stack.push_async_callback(bus.aclose)
            await bus.init_schema()
            await bus._start_replay()
            buses.append(bus)
        yield tuple(buses)


@pytest_asyncio.fixture
async def redis_bus(redis_buses):
    return redis_buses[0]


@pytest_asyncio.fixture
async def redis_key(redis_buses):
    """Use a unique key prefix, then remove only keys carrying that prefix."""
    prefix = "test-" + uuid4().hex
    try:
        yield prefix
    finally:
        client = redis_buses[1]._replay.get_client()
        async for key in client.scan_iter(match=f"*{prefix}*"):
            await client.delete(key)
