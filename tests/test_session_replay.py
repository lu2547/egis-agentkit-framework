"""长回复切换会话后，完整重放尚未落库的事件，不能裁掉回复开头。"""

import asyncio
import json
from contextlib import aclosing

from agentscope.app._bus_ops import publish_session_event
from agentscope.app._router._session import stream_session_events
from agentscope.app.message_bus import MessageBusKeys

from egis_agentkit.extension._message_bus_impl import EgisMessageBus


class SessionStorage:
    async def get_session(self, *_):
        return object()


async def test_active_reply_is_retained_until_its_message_checkpoint_is_persisted():
    async with EgisMessageBus(deployment_mode="standalone") as bus:
        sid = "long-reply"
        key = MessageBusKeys.session_events(sid)
        await publish_session_event(
            bus, sid, {"type": "REPLY_START", "reply_id": "first"}
        )
        cursor = None
        for index in range(MessageBusKeys.SESSION_REPLAY_MAX_LEN + 1):
            cursor = await publish_session_event(
                bus, sid, {"type": "TEXT_BLOCK_DELTA", "delta": str(index)}
            )
        page = await bus.log_read(key, max_count=MessageBusKeys.SESSION_REPLAY_MAX_LEN)
        assert page[0][1]["type"] == "REPLY_START"
        assert len(await bus.log_read(key, since=page[-1][0])) == 2
        await bus.registry_set(
            MessageBusKeys.session_event_checkpoint(sid),
            MessageBusKeys.SESSION_EVENT_CURSOR_FIELD,
            cursor,
        )
        await publish_session_event(
            bus, sid, {"type": "REPLY_START", "reply_id": "second"}
        )
        remaining = await bus.log_read(key)
        assert len(remaining) <= 2
        assert remaining[-1][1]["reply_id"] == "second"


async def test_sse_replays_all_pages_before_switching_to_live_events():
    async with EgisMessageBus(deployment_mode="standalone") as bus:
        count = MessageBusKeys.SESSION_REPLAY_MAX_LEN + 2
        key = MessageBusKeys.session_events("paged-replay")
        for index in range(count):
            await bus.delegate.log_append(key, {"sequence": index})
        response = await stream_session_events(
            session_id="paged-replay",
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
                    for _ in range(count)
                ]
            assert [frame["sequence"] for frame in frames] == list(range(count))


async def test_postgres_replay_keeps_the_start_of_a_long_active_reply(storage):
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus.init_schema()
    try:
        key = MessageBusKeys.session_events("postgres-long-reply")
        await bus.log_append(
            key, {"type": "REPLY_START"}, max_len=MessageBusKeys.SESSION_REPLAY_MAX_LEN
        )
        for index in range(MessageBusKeys.SESSION_REPLAY_MAX_LEN + 1):
            await bus.log_append(
                key,
                {"type": "TEXT_BLOCK_DELTA", "delta": str(index)},
                max_len=MessageBusKeys.SESSION_REPLAY_MAX_LEN,
            )
        page = await bus.log_read(key, max_count=MessageBusKeys.SESSION_REPLAY_MAX_LEN)
        assert page[0][1]["type"] == "REPLY_START"
        assert len(await bus.log_read(key, since=page[-1][0])) == 2
    finally:
        await bus.aclose()
