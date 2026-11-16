from types import SimpleNamespace

import pytest
from egis_agentkit_app.bootstrap._demo import seed_demo
from test_app_http import app_snapshot
from test_domain import expert, identities

from egis_agentkit.bootstrap._demo_tools import (
    CreateDemoReport,
    deploy_demo_report_tools,
)
from egis_agentkit.errors import Forbidden
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.project._files import ProjectFileStorage


def test_example_tool_is_outside_framework_business_layers():
    assert CreateDemoReport.__module__.startswith("egis_agentkit_examples.")


async def test_deployed_report_tool_writes_project_artifact(tmp_path):
    files = ProjectFileStorage(tmp_path)
    scope = SimpleNamespace(user_id="alice", project_id="finance")
    tool = CreateDemoReport(scope, files=files, version="1.1.0")
    result = await tool.call(title="财务报告", findings=["差旅支出 250 元"])
    content, _ = files.read("alice", "finance", "artifacts", "report.md")
    assert "差旅支出 250 元" in content.decode()
    assert "1.1.0" in content.decode()
    assert result.metadata["artifacts"] == [{"path": "report.md", "area": "artifacts"}]


async def test_deployed_tool_requires_current_expert_and_exact_binding(
    storage, tmp_path
):
    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    registry = resources.tool_registry
    deploy_demo_report_tools(registry, ProjectFileStorage(tmp_path))
    await resources.seed_builtin_tools()
    identity = "CreateDemoReport", "1.1.0"
    binding = {
        "resource_type": "tool",
        "resource_id": identity[0],
        "resource_version": identity[1],
        "resource_view_type": "system",
    }
    value = expert()
    value.payload["bindings"] = [binding]
    definition = await AgentManager(storage).add_definition(alice, value)
    graph = await storage.definitions.get_definition_view(
        definition["id"], "private", version=definition["resource_version"]
    )
    scope = SimpleNamespace(
        user_id=alice.user_id,
        session_id="session",
        project_id="project",
        definition=definition,
        root_definition=graph,
        binding={"agent_definition_id": graph["id"]},
    )
    (tool,) = await registry.resolve(alice, scope)
    assert tool.name == identity[0]
    assert await registry.authorize(alice, scope, None, tool)

    scope.definition = {**definition, "agent_type": "personal"}
    assert await registry.resolve(alice, scope) == []
    with pytest.raises(Forbidden):
        await registry.authorize(alice, scope, None, tool)

    scope.definition = {
        **definition,
        "payload": {"bindings": [{**binding, "resource_version": "1.0.0"}]},
    }
    assert await registry.resolve(alice, scope) == []
    scope.definition = definition
    await resources.set_asset_enabled(admin, "tool", *identity, False, admin=True)
    assert await registry.resolve(alice, scope) == []


async def test_demo_seed_uses_local_catalog_and_deployed_tool(storage, tmp_path):
    resources = ResourceService(storage, tmp_path)
    deploy_demo_report_tools(resources.tool_registry, ProjectFileStorage(tmp_path))
    await resources.seed_builtin_tools()
    provider = SimpleNamespace(snapshot=app_snapshot(tmp_path))
    await seed_demo(storage, provider, resources)
    await seed_demo(storage, provider, resources)

    skill = await storage.assets.get("skill", "egis-demo-report", "1.0.0")
    assert "original_markdown" not in skill["payload"]
    assert await resources.read_skill_markdown(skill)
    connector = await storage.assets.get("mcp", "egis-demo-tools", "1.0.0")
    assert connector["payload"]["config"]["transport"] == "streamable_http"
    rows, _ = await storage.definitions.list_definitions_and_reviews("agent")
    experts = [row for row in rows if row["agent_type"] in {"expert", "remote"}]
    assert len(experts) == 6
    assert all(
        any(
            ref["resource_id"] == "CreateDemoReport"
            for ref in row["payload"]["bindings"]
        )
        for row in experts
    )


async def test_configured_default_tools_are_filtered_in_catalog_and_execution(
    storage, tmp_path
):
    from agentscope.tool import Read, Write

    snapshot = app_snapshot(
        tmp_path, tool={"defaults": {"registered_ids": ["Read"]}}
    )
    config = SimpleNamespace(snapshot=snapshot)
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path, config=config)
    await resources.seed_builtin_tools()
    assert [tool["resource_id"] for tool in await resources.list(alice, "tool")] == [
        "Read"
    ]
    scope = SimpleNamespace(user_id=alice.user_id, configuration=snapshot)
    assert await resources.tool_registry.authorize(alice, scope, None, Read())
    with pytest.raises(Forbidden):
        await resources.tool_registry.authorize(alice, scope, None, Write())
