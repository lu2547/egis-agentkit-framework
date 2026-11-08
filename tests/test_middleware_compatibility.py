from types import SimpleNamespace

import pytest
from agentscope.permission import PermissionBehavior
from agentscope.state import AgentState
from agentscope.tool import Read, Toolkit

from egis_agentkit.errors import Forbidden
from egis_agentkit.extensions.middleware import (
    SkillBindingMiddleware,
    TeamPromptMiddleware,
    ToolAccessMiddleware,
)
from egis_agentkit.extensions.skill import LiveSkillLoader
from egis_agentkit.models import User


@pytest.mark.asyncio
@pytest.mark.parametrize("keyword_arguments", [False, True])
async def test_tool_access_accepts_legacy_permissions(tmp_path, keyword_arguments):
    root = {"id": "personal"}
    collaborator = {"id": "borrowed"}
    user = User(user_id="alice", username="Alice")
    scope = SimpleNamespace(
        workdir=tmp_path / ".work",
        artifacts_dir=tmp_path / "artifacts",
        session_record=SimpleNamespace(team_id=None),
        snapshot=root,
        binding={
            "is_team": False,
            "collaboration": {"members": [{"snapshot": collaborator}]},
        },
    )

    class Permissions:
        async def validate_snapshot(self, subject, snapshot):
            assert subject is user
            if snapshot is collaborator:
                raise Forbidden("collaborator revoked")
            assert snapshot is root

    permissions = Permissions()
    if keyword_arguments:
        middleware = ToolAccessMiddleware(
            user=user, scope=scope, permissions=permissions
        )
    else:
        middleware = ToolAccessMiddleware(user, scope, permissions=permissions)

    async def native(**kwargs):
        pytest.fail("Revoked access must stop before native permission")

    result = await middleware.on_check_permission(
        None, {"tool": Read(model_input_types=[]), "tool_input": {}}, native
    )
    assert result.behavior == PermissionBehavior.DENY
    assert result.message == "collaborator revoked"


@pytest.mark.asyncio
async def test_team_prompt_accepts_legacy_scope_and_reads_updated_roster():
    member = {
        "status": "pending",
        "snapshot": {"root": {"agent_name": "writer"}},
        "native_agent_id": "123456789",
    }
    scope = SimpleNamespace(
        binding={"is_team": False, "collaboration": {"members": [member]}}
    )
    middleware = TeamPromptMiddleware(scope)

    assert "writer@12345678" not in await middleware.on_system_prompt(None, "system")
    member["status"] = "joined"
    assert "writer@12345678" in await middleware.on_system_prompt(None, "system")


@pytest.mark.asyncio
async def test_skill_binding_accepts_a_stateful_loader_without_callback():
    scope = SimpleNamespace(binding={})
    loader = LiveSkillLoader(User(user_id="alice", username="Alice"), scope, object())
    middleware = SkillBindingMiddleware(loader)
    state = AgentState()
    toolkit = Toolkit()
    agent = SimpleNamespace(state=state, toolkit=toolkit)

    async def native(**kwargs):
        pins = state.middle_context["egis_agentkit"]["activated_skills"]
        assert scope.binding["activated_skills"] is pins
        assert toolkit.tool_groups[0].skills_or_loaders == [loader]
        yield "native result"

    assert [item async for item in middleware.on_reply(agent, {}, native)] == [
        "native result"
    ]
