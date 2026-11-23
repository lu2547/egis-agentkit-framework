import time
from dataclasses import replace
from uuid import uuid4

from agentscope.app.message_bus import InMemoryMessageBus
from pydantic import SecretStr
from redis_helpers import redis_config as redis_config

from egis_agentkit.core.session._context import VerifiedChatAuthorization
from egis_agentkit.service.session._authorization import ChatAuthorizationStore
from egis_agentkit.service.session._message_bus_override import SessionMessageBus


def authorization(**changes):
    return VerifiedChatAuthorization(
        user_id="alice", agent_id="agent", session_id="session",
        access_token=SecretStr("verified-test-token"),
        **{"expires_at": time.time() + 60, **changes},
    )


async def test_authorization_is_scoped_to_the_exact_owner_agent_and_session():
    bus = SessionMessageBus(deployment_mode="standalone")
    store = ChatAuthorizationStore(bus)
    await store.remember(authorization())
    loaded = await store.load("alice", "agent", "session")
    assert loaded.access_token.get_secret_value() == "verified-test-token"
    assert "verified-test-token" not in repr(loaded)
    assert await store.load("bob", "agent", "session") is None
    assert await store.load("alice", "other-agent", "session") is None
    assert await store.load("alice", "agent", "other-session") is None
    await store.discard("session")
    assert await store.load("alice", "agent", "session") is None
    await store.remember(authorization())
    await bus.purge_sessions(["session"])
    assert await store.load("alice", "agent", "session") is None


async def test_expired_authorization_cannot_be_loaded_or_extended(monkeypatch):
    store = ChatAuthorizationStore(InMemoryMessageBus())
    expires_at = time.time() + 60
    await store.remember(authorization(expires_at=expires_at))
    monkeypatch.setattr(time, "time", lambda: expires_at + 1)
    assert await store.load("alice", "agent", "session") is None
    await store.remember(authorization(expires_at=expires_at))
    assert await store.load("alice", "agent", "session") is None


async def test_cluster_worker_can_read_the_credentials_from_another_process_bus(
    storage, redis_config
):
    kwargs = {
        "deployment_mode": "cluster",
        "engine": storage.engine,
        "redis": redis_config,
    }
    sender, worker = SessionMessageBus(**kwargs), SessionMessageBus(**kwargs)
    await sender.init_schema()
    await worker._start_replay()
    sid = "test-chat-auth-" + uuid4().hex
    try:
        writer, reader = ChatAuthorizationStore(sender), ChatAuthorizationStore(worker)
        await writer.remember(replace(authorization(), session_id=sid))
        loaded = await reader.load("alice", "agent", sid)
        assert loaded.access_token.get_secret_value() == "verified-test-token"
        await worker.purge_sessions([sid])
        assert await writer.load("alice", "agent", sid) is None
    finally:
        await worker.aclose()
        await sender.aclose()
