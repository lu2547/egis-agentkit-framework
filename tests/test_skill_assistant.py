"""验证系统专家授权、工具绑定、附件安装和个人技能创建。"""

from types import SimpleNamespace

import pytest
from egis_agentkit_app.bootstrap._skill_assistant import (
    IDENTIFIER,
    provision_skill_assistant,
)
from test_domain import identities
from test_resources import archive

from egis_agentkit.bootstrap._skill_tools import deploy_skill_tools
from egis_agentkit.errors import Forbidden
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill_import import SkillImportService
from egis_agentkit.storage.model.agent._update import AgentUpdate
from egis_agentkit.storage.project._files import ProjectFileStorage


@pytest.mark.asyncio
async def test_skill_assistant_installs_and_creates_under_current_user(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    imports = SkillImportService(storage, resources)
    registry = resources.tool_registry
    deploy_skill_tools(registry, imports)
    await resources.seed_builtin_tools()
    await provision_skill_assistant(storage, None, None)
    await provision_skill_assistant(storage, None, None)

    definition = await storage.definitions.get_definition_view(IDENTIFIER, "system")
    assert definition["agent_display_name"] == "Skills 助手"
    assert definition["version"] == 1
    scope = SimpleNamespace(
        user_id=alice.user_id,
        session_id="session",
        project_id="project",
        definition=definition,
        root_definition=definition,
        binding={"agent_definition_id": IDENTIFIER},
    )
    install, create = await registry.resolve(alice, scope)
    assert await registry.authorize(alice, scope, None, install)
    assert await registry.authorize(alice, scope, None, create)

    files = ProjectFileStorage(tmp_path)
    files.write(
        alice.user_id, scope.project_id, "report.zip", archive(), area="uploads"
    )
    with pytest.raises(Forbidden):
        await install.call(path="../../another-user.zip")
    await install.call(path="report.zip")
    await install.call(path="report.zip")
    (uploaded,) = await resources.list(alice, "skill")
    assert uploaded["is_installed"] and uploaded["is_enabled"]
    assert uploaded["resource_version"] == "1.0.0"
    assert (
        tmp_path
        / "user"
        / alice.user_id
        / "skills"
        / uploaded["resource_id"]
        / "1.0.0"
        / "SKILL.md"
    ).is_file()

    await create.call(
        skill_id="format-report",
        markdown="---\nname: format-report\ndescription: Format reports\nversion: 1.0.0\n---\n# Instructions",
    )
    assert await resources.list(bob, "skill") == []
    assert (
        tmp_path
        / "user"
        / alice.user_id
        / "skills"
        / "format-report"
        / "1.0.0"
        / "SKILL.md"
    ).is_file()

    scope.definition = {**definition, "agent_type": "personal"}
    assert await registry.resolve(alice, scope) == []


@pytest.mark.asyncio
async def test_skill_assistant_repairs_tool_bindings_without_replacing_its_prompt(storage, tmp_path):
    await identities(storage)
    resources = ResourceService(storage, tmp_path)
    deploy_skill_tools(resources.tool_registry, SkillImportService(storage, resources))
    await resources.seed_builtin_tools()
    await provision_skill_assistant(storage, None, None)
    definition = await storage.get_definition(IDENTIFIER)
    from egis_agentkit.storage.model import User
    actor = User(user_id="system", username="系统", role_type="admin")
    payload = {**definition["payload"], "bindings": [], "system_prompt": "保留现有配置"}
    await AgentManager(storage).update_definition(actor, IDENTIFIER, AgentUpdate(
        agent_name=definition["agent_name"], agent_display_name=definition["agent_display_name"],
        payload=payload, owner_version=definition["owner_version"],
    ))
    await provision_skill_assistant(storage, None, None)
    saved = await storage.get_definition(IDENTIFIER)
    assert {item["resource_id"] for item in saved["payload"]["bindings"]} == {
        "CreateSkill", "InstallSkillPackage"
    }
    assert saved["payload"]["system_prompt"] == "保留现有配置"
    await provision_skill_assistant(storage, None, None)
    assert (await storage.get_definition(IDENTIFIER))["owner_version"] == saved["owner_version"]


@pytest.mark.asyncio
async def test_app_startup_provisions_usable_skills_assistant(storage, tmp_path, monkeypatch):
    import httpx
    from egis_agentkit_app.bootstrap import build_app
    from identity_helpers import configure_test_issuer, signed_token
    from test_app_http import Config, app_snapshot

    from egis_agentkit.client.nacos import NacosBootstrap, NacosClient

    configure_test_issuer(monkeypatch)
    monkeypatch.setenv("TEST_DOWNLOAD", "download-secret-with-more-than-thirty-two-bytes")
    monkeypatch.setenv("TEST_MODEL_KEY", "unused-test-only")
    async with NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos:
        app = await build_app(Config(app_snapshot(tmp_path)), nacos, engine=storage.engine, manage_logging=False)
        async with app.router.lifespan_context(app):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test", headers={
                "Authorization": "Bearer " + signed_token("first-user")
            }) as client:
                response = await client.get(f"/manager/agents/{IDENTIFIER}", params={"view":"system"})
                assert response.status_code == 200, response.text
                item = response.json()
                assert item["is_usable"]
                assert {ref["resource_id"] for ref in item["payload"]["bindings"]} == {
                    "CreateSkill", "InstallSkillPackage"
                }
