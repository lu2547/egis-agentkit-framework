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
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill_import import SkillImportService
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
