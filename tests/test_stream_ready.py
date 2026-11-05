import asyncio
from contextlib import aclosing

import pytest
from agentscope.app.message_bus import MessageBusKeys

from egis_agentkit.services.assembly import ScopedMessageBus


@pytest.mark.asyncio
async def test_idle_sse_flushes_only_after_real_subscription_ready():
    from egis_agentkit_app.streaming import StreamReady

    callback_ran = asyncio.Event()
    response = asyncio.Queue()
    callbacks = []
    bus = ScopedMessageBus(deployment_mode="standalone")

    async def native_app(scope, receive, send):
        await send(
            {
                "type": "http.response.start",
                "status": 200,
                "headers": [(b"content-type", b"text/event-stream")],
            }
        )

        def ready():
            callbacks.append(True)
            callback_ran.set()

        async with aclosing(
            bus.subscribe(MessageBusKeys.session_events("s1"), on_ready=ready)
        ) as stream:
            async for value in stream:
                await send(
                    {"type": "http.response.body", "body": value, "more_body": True}
                )

    async def send(message):
        if message["type"] == "http.response.body":
            assert callback_ran.is_set(), (
                "must not advertise readiness before bus subscription"
            )
        await response.put(message)

    async def receive():
        await asyncio.Event().wait()

    async with bus:
        task = asyncio.create_task(
            StreamReady(native_app)(
                {"type": "http", "path": "/sessions/s1/stream"}, receive, send
            )
        )
        assert (await asyncio.wait_for(response.get(), 0.5))[
            "type"
        ] == "http.response.start"
        try:
            frame = await asyncio.wait_for(response.get(), 0.5)
            assert frame["body"] == b": ready\n\n"
            assert frame["more_body"] is True
            assert callbacks == [True]
            await bus.publish(
                MessageBusKeys.session_events("s1"), b"data: unchanged\n\n"
            )
            assert (await asyncio.wait_for(response.get(), 0.5))[
                "body"
            ] == b"data: unchanged\n\n"
        finally:
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
