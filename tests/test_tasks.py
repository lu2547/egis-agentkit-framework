import pytest
from test_domain import expert, identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.services import AgentService
from egis_agentkit.services.tasks import TaskService
from egis_agentkit.services.teams import TeamService


@pytest.mark.asyncio
async def test_new_tasks_have_distinct_native_sessions_and_owner_binding(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    service = TaskService(storage, tmp_path)
    first = await service.create(alice, source["id"], "private")
    second = await service.create(alice, source["id"], "private")
    assert first["session_id"] != second["session_id"]
    assert len(await service.list(alice)) == 2
    with pytest.raises(Forbidden):
        await service.get(bob, first["session_id"])


@pytest.mark.asyncio
async def test_static_team_has_owned_orchestrator_and_new_member_states(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    member = await AgentService(storage).save(alice, expert())
    team = await TeamService(storage).save(
        alice,
        {
            "team_name": "writers",
            "team_display_name": "写作团",
            "orchestrator_config": {
                "agent_name": "coordinator",
                "agent_display_name": "主理人",
                "payload": {},
            },
            "members": [
                {"definition_id": member["id"], "resource_view_type": "private"}
            ],
        },
    )
    task = await TaskService(storage, tmp_path).create(alice, team["id"], "private")
    session = await storage.get_session("alice", task["agent_id"], task["session_id"])
    native_team = await storage.get_team("alice", session.team_id)
    assert native_team.leader_agent_id == task["agent_id"]
    assert native_team.data.members[0].agent_id != member["id"]
    member_session = await storage.get_session(
        "alice",
        native_team.data.members[0].agent_id,
        native_team.data.members[0].session_id,
    )
    assert member_session.state.context == []
    assert member_session.state.middle_context["egis_agentkit"]["is_initialized"]
    assert (await storage.get_agent("alice", task["agent_id"])).user_id == "alice"


@pytest.mark.asyncio
async def test_task_exposes_display_names_without_changing_runtime_identity(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    source = await AgentService(storage).save(alice, expert())
    task = await TaskService(storage, tmp_path).create(alice, source["id"], "private")
    assert (
        task["agent_display_names"][source["agent_name"]]
        == source["agent_display_name"]
    )
    native = await storage.get_agent(alice.user_id, task["agent_id"])
    assert native.data.name == source["agent_name"]
