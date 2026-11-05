from types import SimpleNamespace

import pytest
from agentscope.app.message_bus import MessageBusKeys
from agentscope.permission import PermissionMode
from test_domain import expert, identities

from egis_agentkit.config.snapshot import PoliciesConfig
from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.message_bus import EgisMessageBus
from egis_agentkit.services import AgentService
from egis_agentkit.services.tasks import TaskService


@pytest.mark.asyncio
async def test_modes_are_persisted_policy_checked_and_locked(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    config = SimpleNamespace(snapshot=SimpleNamespace(policies=PoliciesConfig(config_version=1)))
    service = TaskService(storage, tmp_path, config_provider=config)
    task = await service.create(alice, source['id'], 'private', permission_mode=PermissionMode.EXPLORE)
    assert task['permission_mode'] == 'explore' and task['model_key'] is None
    with pytest.raises(Forbidden):
        await service.create(alice, source['id'], 'private', permission_mode=PermissionMode.BYPASS)
    bus = EgisMessageBus(deployment_mode='cluster', engine=storage.engine)
    other = EgisMessageBus(deployment_mode='cluster', engine=storage.engine)
    await bus.migrate()
    key = MessageBusKeys.session_lock(task['session_id'])
    assert await other.try_lock(key)
    with pytest.raises(Conflict):
        await service.update_settings(alice, task['session_id'], bus, permission_mode=PermissionMode.ACCEPT_EDITS)
    await other.unlock(key)
    with pytest.raises(Forbidden):
        await service.update_settings(bob, task['session_id'], bus, permission_mode=PermissionMode.DEFAULT)
    with pytest.raises(Forbidden):
        await service.update_settings(alice, task['session_id'], bus, permission_mode=PermissionMode.BYPASS)
    changed = await service.update_settings(alice, task['session_id'], bus, permission_mode=PermissionMode.ACCEPT_EDITS)
    assert changed['permission_mode'] == 'accept_edits'
    assert (await service.session(alice, task['session_id'])).state.permission_context.mode == PermissionMode.ACCEPT_EDITS
    config.snapshot.policies = PoliciesConfig(config_version=1, allowed_permission_modes=['default', 'bypass'])
    enabled = await service.update_settings(alice, task['session_id'], bus, permission_mode=PermissionMode.BYPASS)
    assert enabled['permission_mode'] == 'bypass'
    await bus.aclose()
    await other.aclose()

@pytest.mark.asyncio
async def test_model_update_resolves_and_persists_native_config(storage, tmp_path, monkeypatch):
    from egis_agentkit.config.snapshot import ModelsConfig
    from egis_agentkit.services.models import ModelService
    _, _, alice, _ = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    monkeypatch.setenv('TEST_SETTINGS_MODEL_KEY', 'test-only-credential')
    models = ModelsConfig.model_validate({'config_version': 1, 'default_key': 'a', 'models': [
        {'key': 'a', 'display_name': 'A', 'model': 'model-a', 'api_key_env': 'TEST_SETTINGS_MODEL_KEY'},
        {'key': 'b', 'display_name': 'B', 'model': 'model-b', 'api_key_env': 'TEST_SETTINGS_MODEL_KEY'},
    ]})
    config = SimpleNamespace(snapshot=SimpleNamespace(config_version=1, models=models, policies=PoliciesConfig(config_version=1)))
    service = TaskService(storage, tmp_path, models=ModelService(storage, config), config_provider=config)
    task = await service.create(alice, source['id'], 'private')
    bus = EgisMessageBus()
    updated = await service.update_settings(alice, task['session_id'], bus, model_key='b')
    session = await service.session(alice, task['session_id'])
    assert updated['model_key'] == 'b'
    assert session.config.chat_model_config.model == 'model-b'
    assert session.state.permission_context.mode == PermissionMode.DEFAULT
    config.snapshot.policies = PoliciesConfig(config_version=1, allow_model_override=False)
    with pytest.raises(Forbidden):
        await service.update_settings(alice, task['session_id'], bus, model_key='a')


@pytest.mark.asyncio
async def test_runtime_options_and_settings_http(storage, tmp_path):
    from types import SimpleNamespace
    import httpx
    from fastapi import FastAPI
    from egis_agentkit_app.manager_router import router
    from egis_agentkit_app.security import current_user
    _, _, alice, _ = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    service = TaskService(storage, tmp_path)
    task = await service.create(alice, source['id'], 'private')
    app = FastAPI()
    app.include_router(router)
    app.state.egis_services = {'tasks': service}
    app.state.egis_bus = EgisMessageBus()
    app.dependency_overrides[current_user] = lambda: alice
    async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url='http://test') as client:
        options = (await client.get('/manager/runtime-options')).json()
        assert options['default_permission_mode'] == 'default'
        assert {m['value']: m['is_enabled'] for m in options['permission_modes']}['bypass'] is False
        result = await client.patch(f"/manager/tasks/{task['session_id']}/settings", json={'permission_mode': 'explore'})
        assert result.status_code == 200 and result.json()['permission_mode'] == 'explore'
        assert (await client.patch(f"/manager/tasks/{task['session_id']}/settings", json={'permission_mode': 'invalid'})).status_code == 422
