from types import SimpleNamespace

import pytest
from agentscope.app.message_bus import MessageBusKeys
from agentscope.permission import PermissionMode
from config_helpers import sandbox_runtime
from test_domain import expert, identities

from egis_agentkit.client.nacos._settings import (
    AccessConfig,
    ProjectConfig,
    SessionConfig,
    SessionPermissionsConfig,
)
from egis_agentkit.core.message_bus import EgisMessageBus
from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.session._session import SessionService


@pytest.mark.asyncio
async def test_modes_are_persisted_policy_checked_and_locked(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    config = SimpleNamespace(
        snapshot=SimpleNamespace(
            access=AccessConfig(),
            project=ProjectConfig(upload={"max_bytes": 20971520}),
            session=SessionConfig(
                permissions=SessionPermissionsConfig(
                    default_mode="default",
                    allowed_modes=["default", "accept_edits", "explore"],
                )
            ),
        )
    )
    service = SessionService(storage, tmp_path, config_provider=config)
    default_task = await service.add_session(alice, source["id"], "private")
    assert default_task["permission_mode"] == "default"
    task = await service.add_session(
        alice, source["id"], "private", permission_mode=PermissionMode.EXPLORE
    )
    assert task["permission_mode"] == "explore" and task["model_key"] is None
    with pytest.raises(Forbidden):
        await service.add_session(
            alice, source["id"], "private", permission_mode=PermissionMode.BYPASS
        )
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    other = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus.init_schema()
    key = MessageBusKeys.session_lock(task["session_id"])
    assert await other.try_lock(key)
    with pytest.raises(Conflict):
        await service.update_session_settings(
            alice, task["session_id"], bus, permission_mode=PermissionMode.ACCEPT_EDITS
        )
    await other.unlock(key)
    with pytest.raises(Forbidden):
        await service.update_session_settings(
            bob, task["session_id"], bus, permission_mode=PermissionMode.DEFAULT
        )
    with pytest.raises(Forbidden):
        await service.update_session_settings(
            alice, task["session_id"], bus, permission_mode=PermissionMode.BYPASS
        )
    changed = await service.update_session_settings(
        alice, task["session_id"], bus, permission_mode=PermissionMode.ACCEPT_EDITS
    )
    assert changed["permission_mode"] == "accept_edits"
    assert (
        await service.get_session_record(alice, task["session_id"])
    ).state.permission_context.mode == PermissionMode.ACCEPT_EDITS
    config.snapshot.session = SessionConfig(
        permissions=SessionPermissionsConfig(
            default_mode="default",
            allowed_modes=["default", "bypass"],
        )
    )
    enabled = await service.update_session_settings(
        alice, task["session_id"], bus, permission_mode=PermissionMode.BYPASS
    )
    assert enabled["permission_mode"] == "bypass"
    await bus.aclose()
    await other.aclose()


@pytest.mark.asyncio
async def test_model_update_resolves_and_persists_native_config(
    storage, tmp_path, monkeypatch
):
    from egis_agentkit.client.nacos._settings import AgentKitConfig
    from egis_agentkit.service._model import ModelService

    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    monkeypatch.setenv("TEST_SETTINGS_MODEL_KEY", "test-only-credential")

    def settings(allow_override=True):
        return AgentKitConfig.model_validate(
            {
                "revision": 1,
                "workspace": {"runtime": sandbox_runtime(tmp_path)}, "project": {"upload": {"max_bytes": 20_971_520}},
                "user": {
                    "identity": {
                        "mode": "demo",
                        "issuer": "test",
                        "audience": "test",
                        "algorithm": "HS256",
                    }
                },
                "model": {
                    "catalog": {
                        "models": [
                            {
                                "key": key,
                                "display_name": key.upper(),
                                "model": f"model-{key}",
                                "api_key_env": "TEST_SETTINGS_MODEL_KEY",
                            }
                            for key in ("a", "b")
                        ]
                    },
                    "selection": {"default_key": "a", "allow_override": allow_override},
                },
            }
        )

    config = SimpleNamespace(snapshot=settings())
    service = SessionService(
        storage, tmp_path, models=ModelService(storage, config), config_provider=config
    )
    task = await service.add_session(
        alice, source["id"], "private", permission_mode=PermissionMode.DEFAULT
    )
    bus = EgisMessageBus()
    updated = await service.update_session_settings(
        alice, task["session_id"], bus, model_key="b"
    )
    session = await service.get_session_record(alice, task["session_id"])
    assert updated["model_key"] == "b"
    assert session.config.chat_model_config.model == "model-b"
    assert session.state.permission_context.mode == PermissionMode.DEFAULT
    config.snapshot = settings(allow_override=False)
    with pytest.raises(Forbidden):
        await service.update_session_settings(alice, task["session_id"], bus, model_key="a")


@pytest.mark.asyncio
async def test_runtime_options_and_settings_http(storage, tmp_path):
    from types import SimpleNamespace

    import httpx
    from egis_agentkit_app.bootstrap._router import manager_router as router
    from egis_agentkit_app.controller.base import current_user
    from fastapi import FastAPI

    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    service = SessionService(storage, tmp_path)
    task = await service.add_session(alice, source["id"], "private")
    app = FastAPI()
    app.include_router(router)
    app.state.egis_services = SimpleNamespace(sessions=service)
    app.state.egis_bus = EgisMessageBus()
    app.dependency_overrides[current_user] = lambda: alice
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        options = (await client.get("/manager/runtime-options")).json()
        assert options["default_permission_mode"] == "bypass"
        assert task["permission_mode"] == "bypass"
        assert {m["value"]: m["is_enabled"] for m in options["permission_modes"]}[
            "bypass"
        ] is True
        assert "dont_ask" not in {m["value"] for m in options["permission_modes"]}
        created = await client.post(
            "/manager/tasks",
            json={
                "definition_id": source["id"],
                "resource_view_type": "private",
                "resource_version": "1",
            },
        )
        assert created.status_code == 201
        assert created.json()["resource_version"] == "1"
        assert created.json()["permission_mode"] == "bypass"
        result = await client.patch(
            f"/manager/tasks/{task['session_id']}/settings",
            json={"permission_mode": "explore"},
        )
        assert (
            result.status_code == 200 and result.json()["permission_mode"] == "explore"
        )
        assert (
            await client.patch(
                f"/manager/tasks/{task['session_id']}/settings",
                json={"permission_mode": "invalid"},
            )
        ).status_code == 422


def test_full_access_default_and_unsupported_dont_ask_is_rejected(tmp_path):
    defaults = SessionPermissionsConfig()
    assert PermissionMode.BYPASS in defaults.allowed_modes
    assert PermissionMode.DONT_ASK not in defaults.allowed_modes
    service = SessionService(None, tmp_path)
    assert service.validate_permission_mode("bypass") == PermissionMode.BYPASS
    with pytest.raises(Forbidden):
        service.validate_permission_mode("dont_ask")
    service.config = SimpleNamespace(
        snapshot=SimpleNamespace(
            session=SimpleNamespace(
                permissions=SimpleNamespace(
                    allowed_modes=[PermissionMode.DEFAULT, PermissionMode.DONT_ASK]
                )
            )
        )
    )
    with pytest.raises(Forbidden):
        service.validate_permission_mode("dont_ask")


@pytest.mark.asyncio
async def test_configured_session_naming_and_listing_limits(storage, tmp_path):
    from test_app_http import app_snapshot

    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    config = SimpleNamespace(
        snapshot=app_snapshot(
            tmp_path,
            session={
                "naming": {
                    "default_name": "讨论",
                    "max_length": 3,
                    "create_max_length": 4,
                },
                "listing": {"max_sessions": 1},
            },
        )
    )
    service = SessionService(storage, tmp_path, config_provider=config)
    assert (await service.add_session(alice, source["id"], "private"))["name"] == "讨论"
    second = await service.add_session(alice, source["id"], "private", name="标题")
    assert [item["session_id"] for item in await service.list_sessions(alice)] == [
        second["session_id"]
    ]
    with pytest.raises(ValueError, match="4"):
        await service.add_session(alice, source["id"], "private", name="12345")
    async with EgisMessageBus() as bus:
        with pytest.raises(ValueError, match="3"):
            await service.update_session_name(alice, second["session_id"], bus, "1234")
