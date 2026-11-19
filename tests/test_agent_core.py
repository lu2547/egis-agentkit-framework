"""核心定义可脱离数据库构建配置，管理写入只能经 Service。"""

import pytest
from test_domain import expert

from egis_agentkit.core.agent._expert_definition import ExpertDefinition
from egis_agentkit.core.agent._orchestrator_definition import OrchestratorDefinition
from egis_agentkit.core.agent._personal_definition import PersonalDefinition
from egis_agentkit.errors import Forbidden
from egis_agentkit.storage.model import User


def test_expert_core_builds_independent_configuration_without_storage():
    original = expert()
    configured = ExpertDefinition().build_definition(original)
    assert configured.agent_type == "expert"
    assert configured.agent_name == "writer"
    assert configured.payload["system_prompt"] == "旧版"
    configured.payload["system_prompt"] = "新内容"
    assert original.payload["system_prompt"] == "旧版"


def test_expert_core_rejects_non_deployed_agent_tools():
    data = expert()
    data.payload["bindings"] = [
        {
            "resource_type": "tool",
            "resource_id": "OtherTool",
            "resource_version": "1.0.0",
            "resource_view_type": "system",
        }
    ]
    with pytest.raises(Forbidden):
        ExpertDefinition().build_definition(
            data,
            assets=[
                {
                    "resource_type": "tool",
                    "payload": {"source": "workspace", "exposure": "model"},
                }
            ],
        )


def test_orchestrator_core_preserves_team_leader_identity_without_writing():
    core = OrchestratorDefinition()
    first = core.build_from_team(
        owner_type="user",
        owner_id="alice",
        orchestrator_config={"agent_name": "leader", "agent_display_name": "主理人"},
    )
    previous = {**first.model_dump(), "owner_version": 3}
    revised = core.build_from_team(
        owner_type="user",
        owner_id="alice",
        orchestrator_config={"agent_name": "leader", "agent_display_name": "新主理人"},
        previous_definition=previous,
    )
    assert revised.id == first.id
    assert revised.owner_version == 3
    assert revised.agent_type == "orchestrator"
    assert revised.agent_display_name == "新主理人"


def test_personal_core_builds_same_identity_and_distinguishes_empty_documents():
    core = PersonalDefinition()
    user = User(user_id="alice", username="Alice")
    first, repeated = core.build_definition(user), core.build_definition(user)
    assert first.id == repeated.id
    assert first.owner_id == "alice" and first.agent_type == "personal"


def test_core_assembles_complete_native_agent_and_session_binding():
    core = ExpertDefinition()
    source = {
        **expert().model_dump(),
        "resource_version": "1",
        "resource_view_type": "private",
    }
    record = core.build_agent("alice", source)
    assert record.data.system_prompt == "旧版"
    config, state = core.build_session(
        source,
        project_id="project",
        workspace_id="user-workspace",
        session_id="session",
        cwd="/workspace/.work",
        name="工作会话",
        auto_name=True,
    )
    assert config.workspace_id == "user-workspace" and config.cwd == "/workspace/.work"
    assert config.naming.auto
    binding = state.middle_context["egis_agentkit"]
    assert binding["definition_id"] == source["id"]
    assert binding["resource_version"] == "1"
    assert binding["agent_definition_id"] == source["id"]
    assert "payload" not in binding


def test_personal_agent_assembly_uses_current_preferences_without_changing_definition():
    core = PersonalDefinition()
    definition = core.build_definition(
        User(user_id="alice", username="Alice")
    ).model_dump()
    previous_definition = {
        **definition,
        "payload": {**definition["payload"], "system_prompt": "已存储的定义提示"},
    }
    record = core.build_agent(
        "alice",
        previous_definition,
        preferences={"agent_md": "规范", "soul_md": "个人助理", "user_md": ""},
    )
    assert record.data.system_prompt == "规范\n\n个人助理"
    assert previous_definition["payload"]["system_prompt"] == "已存储的定义提示"
    assert definition["payload"]["system_prompt"] == ""
