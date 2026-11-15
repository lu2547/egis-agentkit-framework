from types import SimpleNamespace

import pytest
from agentscope.permission import PermissionBehavior
from agentscope.state import AgentState
from agentscope.tool import Read, Toolkit

from egis_agentkit.errors import Forbidden
from egis_agentkit.extension import (
    SkillBindingMiddleware,
)
from egis_agentkit.extension.resource._skill_loader_impl import LiveSkillLoader
from egis_agentkit.extension.resource._tool_access_callback import (
    team_prompt_middleware,
    tool_access_middleware,
)
from egis_agentkit.storage.model import User


@pytest.mark.asyncio
async def test_tool_access_checks_current_collaborator_grants(tmp_path):
    root = {"id": "personal"}
    collaborator = {"id": "borrowed"}
    user = User(user_id="alice", username="Alice")
    scope = SimpleNamespace(
        workdir=tmp_path / ".work",
        artifacts_dir=tmp_path / "artifacts",
        session_record=SimpleNamespace(team_id=None),
        root_definition=root,
        binding={
            "is_team": False,
            "collaboration": {"members": [{"native_agent_id": "borrowed"}]},
        },
        collaborator_definitions={"borrowed": collaborator},
    )

    class Permissions:
        async def require_execution(self, subject, snapshot, *, allow_pinned=False):
            assert subject is user
            if snapshot is collaborator:
                raise Forbidden("collaborator revoked")
            assert snapshot is root

    middleware = tool_access_middleware(user, scope, Permissions())

    async def native(**kwargs):
        pytest.fail("Revoked access must stop before native permission")

    result = await middleware.on_check_permission(
        None, {"tool": Read(model_input_types=[]), "tool_input": {}}, native
    )
    assert result.behavior == PermissionBehavior.DENY
    assert result.message == "collaborator revoked"


@pytest.mark.asyncio
async def test_team_prompt_reads_updated_roster():
    member = {
        "status": "pending",
        "native_agent_id": "123456789",
    }
    scope = SimpleNamespace(
        binding={"is_team": False, "collaboration": {"members": [member]}},
        collaborator_definitions={"123456789": {"agent_name": "writer"}},
    )
    middleware = team_prompt_middleware(scope)

    assert "writer@12345678" not in await middleware.on_system_prompt(None, "system")
    member["status"] = "joined"
    assert "writer@12345678" in await middleware.on_system_prompt(None, "system")


@pytest.mark.asyncio
async def test_skill_binding_uses_the_injected_state_callback():
    scope = SimpleNamespace(binding={})
    loader = LiveSkillLoader(User(user_id="alice", username="Alice"), scope, object())
    middleware = SkillBindingMiddleware(loader, bind_state=loader.bind_state)
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
