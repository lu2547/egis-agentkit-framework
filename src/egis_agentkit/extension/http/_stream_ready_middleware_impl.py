"""Flush idle SSE headers after the native bus subscription is ready.

Some proxies hold response headers until the first body chunk. A comment
unblocks the browser without inventing an AgentEvent or replacing the
native replay, heartbeat, and disconnect implementation.
"""

import asyncio
from contextlib import suppress

from egis_agentkit.extension.runtime._session_lock_override import (
    stream_subscription_ready,
)


class StreamReady:
    def __init__(self, app):
        self.app = app

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or not (
            scope["path"].startswith("/sessions/") and scope["path"].endswith("/stream")
        ):
            return await self.app(scope, receive, send)
        ready, lock = asyncio.Event(), asyncio.Lock()
        token = stream_subscription_ready.set(ready)
        flush_task = None
        ended = False

        async def flush():
            await ready.wait()
            async with lock:
                if not ended:
                    await send(
                        {
                            "type": "http.response.body",
                            "body": b": ready\n\n",
                            "more_body": True,
                        }
                    )

        async def forward(message):
            nonlocal ended, flush_task
            async with lock:
                await send(message)
                if message["type"] == "http.response.start":
                    content_type = dict(message.get("headers", [])).get(
                        b"content-type", b""
                    )
                    if content_type.startswith(b"text/event-stream"):
                        flush_task = asyncio.create_task(flush(), name="egis-sse-ready")
                elif message["type"] == "http.response.body" and not message.get(
                    "more_body", False
                ):
                    ended = True

        try:
            await self.app(scope, receive, forward)
        finally:
            if flush_task is not None:
                flush_task.cancel()
                with suppress(asyncio.CancelledError, OSError):
                    await flush_task
            stream_subscription_ready.reset(token)
