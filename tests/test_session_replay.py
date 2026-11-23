"""会话事件保留原生重放上限与实时订阅。"""

import asyncio
import json
from contextlib import aclosing

from agentscope.app._bus_ops import publish_session_event
from agentscope.app._router._session import stream_session_events
from agentscope.app.message_bus import MessageBusKeys
from redis_helpers import redis_buses as redis_buses
from redis_helpers import redis_config as redis_config
from redis_helpers import redis_key as redis_key

from egis_agentkit.core.message_bus import EgisMessageBus


class SessionStorage:
    async def get_session(self, *_):
        return object()


async def assert_bounded_session_events(bus):
    sid = "bounded-replay"
    key = MessageBusKeys.session_events(sid)
    for index in range(1002):
        await bus.log_append(
            key, {"sequence": index}, max_len=MessageBusKeys.SESSION_REPLAY_MAX_LEN
        )
    retained = await bus.log_read(key, max_count=1002)
    assert len(retained) == 1000
    assert [event["sequence"] for _, event in retained] == list(range(2, 1002))
    assert await bus.log_read(key, since=retained[-1][0]) == []


async def test_standalone_session_events_keep_latest_1000():
    async with EgisMessageBus(deployment_mode="standalone") as bus:
        await assert_bounded_session_events(bus)


async def test_redis_session_events_keep_the_latest_page(redis_buses, redis_key):
    sender, reader = redis_buses
    key = MessageBusKeys.session_events(redis_key)
    cap = MessageBusKeys.SESSION_REPLAY_MAX_LEN
    count = cap + 300
    for index in range(count):
        await sender.log_append(key, {"sequence": index}, max_len=cap)
    retained = await reader.log_read(key, max_count=count)
    # AgentScope's Redis Streams MAXLEN trim is approximate.
    assert cap <= len(retained) < count
    assert [event["sequence"] for _, event in retained[-cap:]] == list(
        range(count - cap, count)
    )
    assert await reader.log_read(key, since=retained[-1][0]) == []


async def test_native_sse_replays_retained_events_then_receives_new_live_events():
    async with EgisMessageBus(deployment_mode="standalone") as bus:
        sid = "native-replay"
        for index in range(1002):
            await publish_session_event(bus, sid, {"sequence": index})
        response = await stream_session_events(
            session_id=sid,
            agent_id="agent",
            user_id="user",
            storage=SessionStorage(),
            message_bus=bus,
            after=None,
        )
        async with aclosing(response.body_iterator) as stream:
            async with asyncio.timeout(2):
                frames = [
                    json.loads((await anext(stream)).removeprefix("data: ").strip())
                    for _ in range(1000)
                ]
                assert [frame["sequence"] for frame in frames] == list(range(2, 1002))
                await publish_session_event(bus, sid, {"sequence": 1002})
                live = json.loads((await anext(stream)).removeprefix("data: ").strip())
                assert live["sequence"] == 1002
