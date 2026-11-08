from agentscope.app.storage import AgentRecord, TeamMember, TeamRecord


def test_agent_factory_prepares_invitable_team_record_and_projects_pinned_data():
    from egis_agentkit.service.agent.assembly import AgentRecordFactory

    agent = AgentRecordFactory.create(
        "alice",
        name="expert",
        system_prompt="latest",
        source="team",
        record_id="agent-a",
        invite_description="Expert analyst",
    )
    assert isinstance(agent, AgentRecord)
    assert (agent.id, agent.user_id, agent.source) == ("agent-a", "alice", "team")
    assert agent.data.chat_config.invite_config.invitable
    assert agent.data.chat_config.invite_config.invite_description == "Expert analyst"

    projected = AgentRecordFactory.project(agent, name="pinned", system_prompt="old")
    assert (projected.id, projected.data.id) == (agent.id, agent.data.id)
    assert (projected.data.name, projected.data.system_prompt) == ("pinned", "old")
    assert not projected.data.chat_config.invite_config.invitable
    assert (agent.data.name, agent.data.system_prompt) == ("expert", "latest")
    assert agent.data.chat_config.invite_config.invitable


def test_team_factory_keeps_leader_and_worker_session_relationships():
    from egis_agentkit.service.team.assembly import TeamRecordFactory

    workers = [
        TeamMember(
            owner_id="alice", agent_id="agent-b", session_id="worker-a", role="created"
        ),
        TeamMember(
            owner_id="bob", agent_id="agent-c", session_id="worker-b", role="invited"
        ),
    ]
    team = TeamRecordFactory.create(
        "alice",
        leader_agent_id="agent-a",
        leader_session_id="leader",
        name="Review team",
        description="Review the report",
        members=workers,
    )
    assert isinstance(team, TeamRecord)
    assert (team.user_id, team.leader_agent_id, team.session_id) == (
        "alice",
        "agent-a",
        "leader",
    )
    assert (team.data.name, team.data.description) == (
        "Review team",
        "Review the report",
    )
    assert [
        (worker.agent_id, worker.session_id, worker.role)
        for worker in team.data.members
    ] == [
        ("agent-b", "worker-a", "created"),
        ("agent-c", "worker-b", "invited"),
    ]
