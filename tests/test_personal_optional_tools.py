from types import SimpleNamespace
from pathlib import Path

import pytest
from sqlalchemy import text
from sqlalchemy.exc import IntegrityError
from test_app_http import app_snapshot
from test_domain import expert, identities

from egis_agentkit.bootstrap._init import init_agentkit
from egis_agentkit.errors import Forbidden
from egis_agentkit.service.session._preparation import SessionPreparationService


@pytest.fixture
async def optional_tools(storage, tmp_path, monkeypatch):
    monkeypatch.setenv("TEST_MODEL_KEY", "test-only-unused-model-key")
    _, _, alice, bob = await identities(storage)
    config = SimpleNamespace(snapshot=app_snapshot(
        tmp_path,
        tool={"knowledge_base": {"query_url": "https://knowledge.example.com/query"}},
    ))
    kit = init_agentkit(config, None, engine=storage.engine, manage_logging=False)
    async with kit.services.sessions.storage:
        await kit.services.resources.seed_builtin_tools()
        yield kit, config, alice, bob


async def test_optional_tools_are_personal_assistant_selections(
    optional_tools, storage, tmp_path,
):
    kit, config, alice, bob = optional_tools
    resources, sessions = kit.services.resources, kit.services.sessions
    personal = await kit.services.personal.get_or_add_definition(alice)
    task = await sessions.add_session(alice, personal["id"], "private")
    other_personal = await kit.services.personal.get_or_add_definition(bob)
    other_task = await sessions.add_session(bob, other_personal["id"], "private")
    prepare = SessionPreparationService(storage, tmp_path, config)
    async with prepare.bind(task["session_id"]) as scope:
        assert await kit.tools(scope.user_id, scope.agent_id, scope.session_id) == []

    page = await resources.catalog_page(alice, "tool", category="可选工具")
    assert {item["resource_id"] for item in page["items"]} == {
        "CreateScheduledTask", "DeleteScheduledTask", "InternetSearch",
        "KnowledgeBaseSearch", "RunScheduledTask",
    }
    card = next(item for item in page["items"] if item["resource_id"] == "KnowledgeBaseSearch")
    assert card["is_optional_tool"] and not card["is_system_default"]
    assert card["is_usable"] and not card["is_installed"] and not card["is_enabled"]
    await resources.install(alice, "tool", "KnowledgeBaseSearch")
    selected = await kit.services.personal.list_resources(alice, "tool")
    assert selected["KnowledgeBaseSearch"]["agent_id"] == personal["id"]
    assert selected["KnowledgeBaseSearch"]["is_enabled"]
    card = next(
        item for item in (await resources.catalog_page(alice, "tool", category="可选工具"))["items"]
        if item["resource_id"] == "KnowledgeBaseSearch"
    )
    assert card["is_installed"] and card["is_enabled"]
    assert not any(item["is_enabled"] for item in (await resources.catalog_page(bob, "tool", category="可选工具"))["items"])

    second_task = await sessions.add_session(alice, personal["id"], "private")
    for selected_task in (task, second_task):
        async with prepare.bind(selected_task["session_id"]) as scope:
            tools = await kit.tools(scope.user_id, scope.agent_id, scope.session_id)
            assert [tool.name for tool in tools] == ["KnowledgeBaseSearch"]
    async with prepare.bind(other_task["session_id"]) as scope:
        assert await kit.tools(scope.user_id, scope.agent_id, scope.session_id) == []

    definition = await kit.services.agents.add_definition(alice, expert())
    expert_task = await sessions.add_session(alice, definition["id"], "private")
    async with prepare.bind(expert_task["session_id"]) as scope:
        assert await kit.tools(scope.user_id, scope.agent_id, scope.session_id) == []

    async with prepare.bind(task["session_id"]) as scope:
        (tool,) = await kit.tools(scope.user_id, scope.agent_id, scope.session_id)
        await resources.update_installation(alice, "tool", "KnowledgeBaseSearch", False)
        with pytest.raises(Forbidden):
            await resources.tool_registry.authorize(alice, scope, None, tool)
        assert await kit.tools(scope.user_id, scope.agent_id, scope.session_id) == []
    await resources.remove_installation(alice, "tool", "KnowledgeBaseSearch")
    assert await kit.services.personal.list_resources(alice, "tool") == {}


async def test_personal_selection_does_not_enable_unavailable_or_expert_tools(
    optional_tools, storage, tmp_path,
):
    kit, config, alice, _ = optional_tools
    resources = kit.services.resources
    await resources.install(alice, "tool", "KnowledgeBaseSearch")
    config.snapshot = app_snapshot(tmp_path)
    card = next(
        item for item in (await resources.catalog_page(alice, "tool", category="可选工具"))["items"]
        if item["resource_id"] == "KnowledgeBaseSearch"
    )
    assert not card["is_usable"] and not card["is_available"]
    with pytest.raises(Forbidden):
        await resources.install(alice, "tool", "KnowledgeBaseSearch")
    await resources.update_installation(alice, "tool", "KnowledgeBaseSearch", False)
    await resources.remove_installation(alice, "tool", "KnowledgeBaseSearch")
    for name in ("Read", "CreateDemoReport", "AgentInvite"):
        with pytest.raises(Forbidden):
            await resources.install(alice, "tool", name)


async def test_deployment_sql_extends_personal_resource_selection_without_losing_rows(
    optional_tools, storage,
):
    kit, _, alice, _ = optional_tools
    personal = await kit.services.personal.get_or_add_definition(alice)
    await storage.personal_resources.save_resource(
        alice.user_id, personal["id"], "skill", "retained-skill", True, actor=alice.user_id,
    )
    async with storage.engine.connect() as connection:
        await connection.execute(text("""
            ALTER TABLE ak_agent_personal_resources
            DROP CONSTRAINT ak_agent_personal_resources_resource_type_check,
            ADD CONSTRAINT ak_agent_personal_resources_resource_type_check
                CHECK (resource_type IN ('skill','mcp'))
        """))
        await connection.commit()
        raw = await connection.get_raw_connection()
        sql = (Path(__file__).parents[1] / "scripts/releases/1.0.3/02_personal_optional_tools.sql").read_text()
        await raw.driver_connection.execute(sql)
        await raw.driver_connection.execute(sql)
    await kit.services.resources.install(alice, "tool", "KnowledgeBaseSearch")
    assert (await storage.personal_resources.list_resources(alice.user_id, "skill"))["retained-skill"]["is_enabled"]
    with pytest.raises(IntegrityError):
        await storage.personal_resources.save_resource(
            alice.user_id, personal["id"], "agent", "not-a-tool", True, actor=alice.user_id,
        )


async def test_disabling_optional_tool_does_not_expose_an_obsolete_registration(
    optional_tools, storage,
):
    import hashlib
    import json
    from egis_agentkit.tool._knowledgebase_tool import KnowledgeBaseSearch

    kit, _, alice, _ = optional_tools
    resources = kit.services.resources
    current = resources.tool_registry.catalog[resources.tool_registry.native[KnowledgeBaseSearch]]
    payload = {**current.payload, "source": "toolkit"}
    checksum = hashlib.sha256(json.dumps([payload, current.entrypoint], sort_keys=True).encode()).hexdigest()
    previous = current.model_copy(update={"resource_version": "1.0.0", "payload": payload, "checksum_sha256": checksum})
    async with storage.definition_write() as write:
        await write.assets.insert_asset(previous, actor="system")
    admin = await kit.services.users.get("admin")
    await resources.set_asset_enabled(admin, "tool", "KnowledgeBaseSearch", "1.1.0", False, admin=True)
    assert "KnowledgeBaseSearch" not in {
        item["resource_id"] for item in (await resources.catalog_page(alice, "tool"))["items"]
    }


async def test_internet_search_placeholder_requires_personal_selection(
    optional_tools, storage, tmp_path,
):
    kit, config, alice, _ = optional_tools
    config.snapshot = app_snapshot(tmp_path)
    resources = kit.services.resources
    cards = {
        card["resource_id"]: card
        for card in (await resources.catalog_page(alice, "tool", category="可选工具"))["items"]
    }
    assert "InternetSearch" in cards
    assert cards["InternetSearch"]["is_usable"]
    assert not cards["InternetSearch"]["is_installed"]
    personal = await kit.services.personal.get_or_add_definition(alice)
    task = await kit.services.sessions.add_session(alice, personal["id"], "private")
    prepare = SessionPreparationService(storage, tmp_path, config)
    async with prepare.bind(task["session_id"]) as scope:
        assert await kit.tools(scope.user_id, scope.agent_id, scope.session_id) == []
        await resources.install(alice, "tool", "InternetSearch")
        (tool,) = await kit.tools(scope.user_id, scope.agent_id, scope.session_id)
        assert tool.name == "InternetSearch"
        assert await resources.tool_registry.authorize(
            alice, scope, None, tool, {"query": "测试互联网搜索"}
        )
        result = await tool.call(query="测试互联网搜索")
        assert result.content[0].text == "待实现"
        await resources.update_installation(alice, "tool", "InternetSearch", False)
        assert await kit.tools(scope.user_id, scope.agent_id, scope.session_id) == []
