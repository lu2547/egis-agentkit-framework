import time

from agentscope.app.message_bus import InMemoryMessageBus
from pydantic import SecretStr

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


async def test_cluster_worker_can_read_the_credentials_from_another_process_bus(storage):
    sender = SessionMessageBus(deployment_mode="cluster", engine=storage.engine)
    worker = SessionMessageBus(deployment_mode="cluster", engine=storage.engine)
    await sender.init_schema()
    await ChatAuthorizationStore(sender).remember(authorization())
    loaded = await ChatAuthorizationStore(worker).load("alice", "agent", "session")
    assert loaded.access_token.get_secret_value() == "verified-test-token"
    await worker.purge_sessions(["session"])
    assert await ChatAuthorizationStore(sender).load("alice", "agent", "session") is None
