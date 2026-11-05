import asyncio
import importlib.util
from types import SimpleNamespace

import pytest
from agentscope.middleware import ReMeMiddleware

from egis_agentkit.config.snapshot import RuntimeConfig
from egis_agentkit.services.assembly import execution_scope
from egis_agentkit.services.factories import AgentFactories

original = importlib.util.find_spec


def runtime(**extra):
    return RuntimeConfig.model_validate(
        {
            "config_version": 1,
            "workspace_root": "/tmp/work",
            "release_roots": [],
            "auth": {
                "mode": "demo",
                "issuer": "test",
                "audience": "test",
                "algorithm": "HS256",
            },
            **extra,
        }
    )


def test_memory_defaults_to_disabled():
    assert runtime().memory.is_enabled is False


def test_enabled_memory_rejects_missing_dependency(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "importlib.util.find_spec",
        lambda name: None if name == "reme" else original(name),
    )
    config = SimpleNamespace(
        snapshot=SimpleNamespace(runtime=runtime(memory={"is_enabled": True}))
    )
    with pytest.raises(RuntimeError, match="reme-ai"):
        AgentFactories(None, None, tmp_path, config_provider=config)


@pytest.mark.asyncio
async def test_native_middleware_is_cached_per_user_and_closed(monkeypatch, tmp_path):
    from egis_agentkit.services import memory
    from egis_agentkit.services.memory import MemoryService

    monkeypatch.setattr(memory.importlib.util, "find_spec", lambda name: object())
    calls = []

    async def resolve(self, user, model_key=None, **kwargs):
        calls.append(user.user_id)
        return "config"

    async def get_model(user_id, config, access):
        assert config == "config"
        return None

    monkeypatch.setattr(memory.ModelService, "resolve", resolve)
    monkeypatch.setattr(memory, "get_model", get_model)
    service = MemoryService(None, tmp_path, object())
    alice = SimpleNamespace(user_id="alice")
    bob = SimpleNamespace(user_id="bob")
    one, same = await asyncio.gather(service.for_user(alice), service.for_user(alice))
    other = await service.for_user(bob)
    assert isinstance(one, ReMeMiddleware) and one is same and one is not other
    assert one._workspace_dir == str(tmp_path / "user" / "alice" / "memory")
    assert other._workspace_dir == str(tmp_path / "user" / "bob" / "memory")
    assert calls == ["alice", "bob"]
    closed = []

    async def close():
        closed.append(True)

    monkeypatch.setattr(one, "close", close)
    monkeypatch.setattr(other, "close", close)
    await service.close()
    assert len(closed) == 2


@pytest.mark.asyncio
async def test_factory_adds_memory_only_for_personal(monkeypatch, tmp_path):
    config = SimpleNamespace(snapshot=SimpleNamespace(runtime=runtime(), policies=None))
    factory = AgentFactories(None, None, tmp_path, config_provider=config)

    class Users:
        def __init__(self, storage):
            pass

        async def get(self, user_id):
            return SimpleNamespace(user_id=user_id)

    class Memory:
        async def for_user(self, user):
            return marker

    async def preferences(user):
        return {}

    monkeypatch.setattr("egis_agentkit.services.factories.UserService", Users)
    monkeypatch.setattr(factory.preferences, "read", preferences)
    factory.memory = Memory()
    marker = object()
    for kind, expected in [("personal", True), ("private", False)]:
        scope = SimpleNamespace(
            user_id="alice",
            agent_id="a",
            session_id="s",
            assembly=None,
            definition={"agent_type": kind, "payload": {}},
        )
        token = execution_scope.set(scope)
        try:
            result = await factory.middlewares("alice", "a", "s", None)
            assert (marker in result) is expected
        finally:
            execution_scope.reset(token)


@pytest.mark.asyncio
async def test_enabled_service_uses_native_model_and_starts_reme(
    storage, tmp_path, monkeypatch
):
    pytest.importorskip("reme")
    from egis_agentkit.config.snapshot import ConfigSnapshot
    from egis_agentkit.services.memory import MemoryService

    monkeypatch.setenv("MEMORY_TEST_MODEL_KEY", "unused-offline-key")
    snapshot = ConfigSnapshot.model_validate(
        {
            "config_version": 1,
            "runtime": runtime().model_dump(),
            "models": {
                "config_version": 1,
                "default_key": "model",
                "models": [
                    {
                        "key": "model",
                        "display_name": "Offline",
                        "model": "offline-test",
                        "api_key_env": "MEMORY_TEST_MODEL_KEY",
                    }
                ],
            },
            "policies": {"config_version": 1},
        }
    )
    service = MemoryService(storage, tmp_path, SimpleNamespace(snapshot=snapshot))
    try:
        middleware = await service.for_user(SimpleNamespace(user_id="alice"))
        assert middleware._parameters.chat_model.model == "offline-test"
        await middleware._ensure_started()
        assert middleware._started
    finally:
        await service.close()
