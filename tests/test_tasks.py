import pytest
from test_domain import agent_changes, expert, identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.session._preparation import SessionPreparationService
from egis_agentkit.service.session._session import SessionService
from egis_agentkit.service.team._definition import TeamDefinition


@pytest.mark.asyncio
async def test_new_tasks_have_distinct_native_sessions_and_owner_binding(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    service = SessionService(storage, tmp_path)
    first = await service.add_session(alice, source["id"], "private")
    second = await service.add_session(alice, source["id"], "private")
    assert first["session_id"] != second["session_id"]
    assert len(await service.list_sessions(alice)) == 2
    with pytest.raises(Forbidden):
        await service.get_session(bob, first["session_id"])


@pytest.mark.asyncio
async def test_static_team_has_owned_orchestrator_and_new_member_states(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    agents = AgentManager(storage)
    member = await agents.add_definition(alice, expert())
    team = await TeamDefinition(storage).save(
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
    changed = expert()
    changed.id = member["id"]
    changed.owner_version = member["owner_version"]
    changed.payload["system_prompt"] = "新版"
    await agents.update_definition(alice, (changed).id, agent_changes(changed))
    task = await SessionService(storage, tmp_path).add_session(alice, team["id"], "private")
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
    binding = member_session.state.middle_context["egis_agentkit"]
    assert binding["is_initialized"]
    assert binding["definition_id"] == team["id"]
    assert binding["resource_version"] == "1"
    assert "snapshot" not in binding
    async with SessionPreparationService(storage, tmp_path).bind(member_session.id) as scope:
        assert scope.definition["payload"]["system_prompt"] == "旧版"
    assert (await storage.get_agent("alice", task["agent_id"])).user_id == "alice"


@pytest.mark.asyncio
async def test_task_exposes_display_names_without_changing_runtime_identity(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    task = await SessionService(storage, tmp_path).add_session(
        alice, source["id"], "private"
    )
    assert (
        task["agent_display_names"][source["agent_name"]]
        == source["agent_display_name"]
    )
    native = await storage.get_agent(alice.user_id, task["agent_id"])
    assert native.data.name == source["agent_name"]


@pytest.mark.asyncio
@pytest.mark.parametrize("team_task", [False, True])
async def test_task_delete_removes_only_owned_reply_associations_and_keeps_project_files(
    storage, tmp_path, team_task
):
    from agentscope.app.message_bus import InMemoryMessageBus
    from sqlalchemy import insert, select

    from egis_agentkit.storage import tables as t

    _, _, alice, _ = await identities(storage)
    definition = await AgentManager(storage).add_definition(alice, expert())
    service = SessionService(storage, tmp_path)
    if team_task:
        team = await TeamDefinition(storage).save(
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
                    {"definition_id": definition["id"], "resource_view_type": "private"}
                ],
            },
        )
        deleted = await service.add_session(alice, team["id"], "private")
    else:
        deleted = await service.add_session(alice, definition["id"], "private")
    surviving = await service.add_session(alice, definition["id"], "private")
    assert deleted["project_id"] == surviving["project_id"]
    session_ids = [deleted["session_id"]]
    if team_task:
        leader = await storage.get_session(
            "alice", deleted["agent_id"], deleted["session_id"]
        )
        native_team = await storage.get_team("alice", leader.team_id)
        session_ids.extend(member.session_id for member in native_team.data.members)
    async with storage.transaction() as conn:
        await conn.execute(
            insert(t.reply_artifacts),
            [
                dict(
                    project_id=deleted["project_id"],
                    session_id=sid,
                    reply_id="reply-" + sid,
                    path="shared.pptx",
                    digest="0" * 64,
                )
                for sid in [*session_ids, surviving["session_id"]]
            ],
        )
    service.files.write(
        "alice",
        deleted["project_id"],
        "shared.pptx",
        b"keep project file",
        area="artifacts",
    )
    async with InMemoryMessageBus() as bus:
        await service.delete_session(alice, deleted["session_id"], bus)
    async with storage.engine.connect() as conn:
        associations = (
            (await conn.execute(select(t.reply_artifacts.c.session_id))).scalars().all()
        )
    members = {
        row.id
        for row in await storage.sessions.in_project("alice", deleted["project_id"])
    }
    assert associations == [surviving["session_id"]]
    assert not set(session_ids).intersection(members)
    assert surviving["session_id"] in members
    assert (
        service.files.read("alice", deleted["project_id"], "artifacts", "shared.pptx")[
            0
        ]
        == b"keep project file"
    )


async def test_session_delete_retries_without_losing_session_or_reply_rows(
    storage, tmp_path
):
    from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
    from sqlalchemy import insert, select

    from egis_agentkit.storage import tables as t

    class FailingCleanupBus(InMemoryMessageBus):
        fail = True

        async def log_trim(self, key, before_id=None):
            if self.fail:
                raise RuntimeError("message bus temporarily unavailable")
            await super().log_trim(key, before_id)

    _, _, alice, _ = await identities(storage)
    definition = await AgentManager(storage).add_definition(alice, expert())
    service = SessionService(storage, tmp_path)
    task = await service.add_session(alice, definition["id"], "private")
    sid = task["session_id"]
    async with storage.transaction() as conn:
        await conn.execute(
            insert(t.reply_artifacts).values(
                project_id=task["project_id"],
                session_id=sid,
                reply_id="reply",
                path="report.md",
                digest="0" * 64,
            )
        )

    async with FailingCleanupBus() as bus:
        with pytest.raises(RuntimeError, match="temporarily unavailable"):
            await service.delete_session(alice, sid, bus)
        assert await storage.get_session(alice.user_id, task["agent_id"], sid)
        assert not await bus.is_locked(MessageBusKeys.session_lock(sid))
        async with storage.engine.connect() as conn:
            assert await conn.scalar(select(t.reply_artifacts.c.path)) == "report.md"
        bus.fail = False
        await service.delete_session(alice, sid, bus)
    assert await storage.get_session(alice.user_id, task["agent_id"], sid) is None
    async with storage.engine.connect() as conn:
        assert await conn.scalar(select(t.reply_artifacts.c.path)) is None
