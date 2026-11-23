"""Session SQL stays bounded while authority and native cleanup stay current."""

from contextlib import contextmanager

import pytest
from agentscope.app.message_bus import InMemoryMessageBus
from agentscope.message import Msg
from sqlalchemy import event
from test_domain import expert, identities
from test_session_listing_queries import select_queries

from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.session._session import SessionService
from egis_agentkit.service.team._definition import TeamDefinition


@contextmanager
def sql_queries(storage):
    statements = []

    def capture(_conn, _cursor, statement, _params, _context, _many):
        statements.append(statement)

    event.listen(storage.engine.sync_engine, "before_cursor_execute", capture)
    try:
        yield statements
    finally:
        event.remove(storage.engine.sync_engine, "before_cursor_execute", capture)


async def test_detail_reuses_owned_session_project_and_pinned_root(storage, tmp_path):
    from test_domain import agent_changes

    users, admin, alice, bob = await identities(storage)
    agents = AgentManager(storage)
    source = await agents.add_definition(alice, expert())
    tasks = SessionService(storage, tmp_path)
    task = await tasks.add_session(alice, source["id"], "private")
    changed = expert()
    changed.id, changed.owner_version = source["id"], source["owner_version"]
    changed.agent_display_name = "第二版"
    await agents.update_definition(alice, source["id"], agent_changes(changed))
    with select_queries(storage) as queries:
        detail = await tasks.get_session(alice, task["session_id"])
    assert detail["resource_version"] == "1"
    assert detail["definition_display_name"] == "写作专家"
    assert len(queries) <= 4, f"detail executed {len(queries)} SELECTs"
    with pytest.raises((Forbidden, NotFound)):
        await tasks.get_session(bob, task["session_id"])
    await users.update_user(admin, alice.model_copy(update={"is_enabled": False}))
    with pytest.raises(Forbidden):
        await tasks.get_session(alice, task["session_id"])


@pytest.mark.parametrize("count", [1, 5])
async def test_stage_collaborators_batches_selected_versions_and_keeps_idempotency(
    storage, tmp_path, count
):
    from test_collaboration import ref, setup_task

    from egis_agentkit.service.team._collaboration import TeamCollaborationService

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    writers = [writer]
    for index in range(count - 1):
        writers.append(
            await AgentManager(storage).add_definition(
                alice, expert(name=f"writer_{index}")
            )
        )
    async with InMemoryMessageBus() as bus:
        with select_queries(storage) as queries:
            result = await TeamCollaborationService.stage_collaborators(
                tasks,
                alice,
                task["session_id"],
                bus,
                "batch-select",
                [ref(w) for w in writers],
            )
        assert {m["definition_id"] for m in result["collaborators"]} == {
            w["id"] for w in writers
        }
        assert result["pending_collaboration_request_id"] == "batch-select"
        assert len(queries) <= 12, f"{count} experts executed {len(queries)} SELECTs"
        again = await TeamCollaborationService.stage_collaborators(
            tasks,
            alice,
            task["session_id"],
            bus,
            "batch-select",
            [ref(w) for w in writers],
        )
        assert again["collaborators"] == result["collaborators"]
        await AgentManager(storage).delete_definition(alice, writers[-1]["id"])
        with pytest.raises((Forbidden, NotFound)):
            await tasks.get_session_record(alice, task["session_id"])
        result = await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "clear", []
        )
        assert result["collaborators"] == []


@pytest.mark.parametrize("count", [1, 8])
async def test_delete_static_team_batches_native_graph_without_deleting_other_sessions(
    storage, tmp_path, count
):
    _, _, alice, _ = await identities(storage)
    agents, tasks = AgentManager(storage), SessionService(storage, tmp_path)
    sources = [
        await agents.add_definition(alice, expert(name=f"writer_{i}"))
        for i in range(count)
    ]
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
                {"definition_id": source["id"], "resource_view_type": "private"}
                for source in sources
            ],
        },
    )
    task = await tasks.add_session(alice, team["id"], "private")
    unrelated = await tasks.add_session(alice, sources[0]["id"], "private")
    leader = await storage.get_session(
        alice.user_id, task["agent_id"], task["session_id"]
    )
    native_team = await storage.get_team(alice.user_id, leader.team_id)
    member_ids = [member.session_id for member in native_team.data.members]
    agent_ids = [member.agent_id for member in native_team.data.members]
    for sid in member_ids:
        await storage.upsert_message(
            alice.user_id,
            sid,
            Msg(name="user", role="user", content=[{"type": "text", "text": "task"}]),
        )
    async with InMemoryMessageBus() as bus:
        with sql_queries(storage) as queries:
            await tasks.delete_session(alice, task["session_id"], bus)
    assert (
        await storage.get_session(alice.user_id, task["agent_id"], task["session_id"])
        is None
    )
    assert await storage.get_team(alice.user_id, leader.team_id) is None
    for aid, sid in zip(agent_ids, member_ids):
        assert await storage.get_agent(alice.user_id, aid) is None
        assert await storage.get_session(alice.user_id, aid, sid) is None
        assert await storage.list_messages(alice.user_id, sid) == ([], False)
    assert (await tasks.get_session(alice, unrelated["session_id"]))[
        "session_id"
    ] == unrelated["session_id"]
    assert len(queries) <= 18, (
        f"{count} team members executed {len(queries)} SQL statements"
    )


async def test_native_history_reuses_only_request_verified_record(
    storage, tmp_path, monkeypatch
):
    import httpx
    from egis_agentkit_app.bootstrap import build_app
    from identity_helpers import configure_test_issuer, signed_token
    from test_app_http import Config, app_snapshot

    from egis_agentkit.client.nacos import NacosBootstrap, NacosClient

    monkeypatch.setenv("TEST_DOWNLOAD", "test-download-secret-with-thirty-two-bytes")
    monkeypatch.setenv("TEST_MODEL_KEY", "test-only-unused-model-key")
    configure_test_issuer(monkeypatch)
    users, admin, alice, _ = await identities(storage)
    async with NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos:
        app = await build_app(
            Config(app_snapshot(tmp_path)), nacos, engine=storage.engine
        )
        async with app.router.lifespan_context(app):
            tasks = app.state.egis_services.sessions
            source = await app.state.egis_services.agents.add_definition(
                alice, expert()
            )
            task = await tasks.add_session(alice, source["id"], "private")
            await storage.upsert_message(
                alice.user_id,
                task["session_id"],
                Msg(
                    name="user",
                    role="user",
                    content=[{"type": "text", "text": "original"}],
                ),
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                headers={"Authorization": "Bearer " + signed_token("alice")},
            ) as client:
                with select_queries(storage) as queries:
                    response = await client.get(
                        f"/sessions/{task['session_id']}/messages",
                        params={"agent_id": task["agent_id"]},
                    )
                assert response.status_code == 200, response.text
                assert "original" in response.text
                session_reads = [q for q in queries if "FROM sessions" in q]
                assert len(session_reads) == 1, (
                    f"native history read session {len(session_reads)} times"
                )
                await users.update_user(
                    admin, alice.model_copy(update={"is_enabled": False})
                )
                response = await client.get(
                    f"/sessions/{task['session_id']}/messages",
                    params={"agent_id": task["agent_id"]},
                )
                assert response.status_code == 403


@pytest.mark.parametrize("count", [1, 8])
async def test_create_static_team_batches_native_publish_and_initialization(
    storage, tmp_path, count
):
    _, _, alice, _ = await identities(storage)
    sources = [
        await AgentManager(storage).add_definition(alice, expert(name=f"writer_{i}"))
        for i in range(count)
    ]
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
                {"definition_id": source["id"], "resource_view_type": "private"}
                for source in sources
            ],
        },
    )
    tasks = SessionService(storage, tmp_path)
    await tasks.projects.default(alice)
    with sql_queries(storage) as queries:
        task = await tasks.add_session(alice, team["id"], "private")
    assert task["definition_display_name"] == "写作团"
    assert len(task["agent_display_names"]) == count + 1
    record = await tasks.get_session_record(alice, task["session_id"])
    native_team = await storage.get_team(alice.user_id, record.team_id)
    assert len(native_team.data.members) == count
    for member in native_team.data.members:
        worker = await storage.get_session(
            alice.user_id, member.agent_id, member.session_id
        )
        assert worker.team_id == native_team.id
        assert worker.state.middle_context["egis_agentkit"]["is_initialized"] is True
    assert len(queries) <= 13, f"creating {count} members executed {len(queries)} SQLs"


@pytest.mark.parametrize("count", [1, 5])
async def test_execution_preparation_batches_collaborators_and_rechecks_current_authority(
    storage, tmp_path, count
):
    from test_collaboration import ref, setup_task

    from egis_agentkit.service.session._preparation import SessionPreparationService
    from egis_agentkit.service.team._collaboration import TeamCollaborationService

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    writers = [
        writer,
        *[
            await AgentManager(storage).add_definition(
                alice, expert(name=f"writer_{i}")
            )
            for i in range(count - 1)
        ],
    ]
    async with InMemoryMessageBus() as bus:
        await TeamCollaborationService.stage_collaborators(
            tasks,
            alice,
            task["session_id"],
            bus,
            "selection",
            [ref(w) for w in writers],
        )
    prepare = SessionPreparationService(storage, tmp_path)
    with select_queries(storage) as queries:
        async with prepare.bind(task["session_id"]) as scope:
            assert len(scope.collaborator_definitions) == count
            assert scope.project_id == task["project_id"]
    assert len(queries) <= 8, (
        f"preparing {count} collaborators executed {len(queries)} SELECTs"
    )
    await AgentManager(storage).delete_definition(alice, writer["id"])
    with pytest.raises((Forbidden, NotFound)):
        async with prepare.bind(task["session_id"]):
            pytest.fail("revoked collaborator reached execution")


async def test_cleanup_preserves_borrowed_agent_and_scrubs_surviving_team_roster(
    storage, tmp_path
):
    from agentscope.app.storage import TeamMember

    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    tasks = SessionService(storage, tmp_path)
    leader, created, borrowed, survivor = [
        await tasks.add_session(alice, source["id"], "private") for _ in range(4)
    ]
    roster = [
        TeamMember(
            owner_id=alice.user_id,
            agent_id=item["agent_id"],
            session_id=item["session_id"],
            role=role,
        )
        for item, role in [(created, "created"), (borrowed, "invited")]
    ]
    native = TeamDefinition.make_record(
        alice.user_id,
        leader_agent_id=leader["agent_id"],
        leader_session_id=leader["session_id"],
        name="native",
        description="",
        members=roster,
    )
    await storage.upsert_team(alice.user_id, native)
    for item in [leader, created, borrowed]:
        await storage.set_session_team_id(alice.user_id, item["session_id"], native.id)
    other = TeamDefinition.make_record(
        alice.user_id,
        leader_agent_id=survivor["agent_id"],
        leader_session_id=survivor["session_id"],
        name="survivor",
        description="",
        members=[roster[0]],
    )
    await storage.upsert_team(alice.user_id, other)
    await storage.set_session_team_id(alice.user_id, survivor["session_id"], other.id)
    async with InMemoryMessageBus() as bus:
        await tasks.delete_session(alice, leader["session_id"], bus)
    assert await storage.get_agent(alice.user_id, created["agent_id"]) is None
    assert await storage.get_agent(alice.user_id, borrowed["agent_id"]) is not None
    assert (
        await storage.get_session(
            alice.user_id, borrowed["agent_id"], borrowed["session_id"]
        )
        is None
    )
    assert (await storage.get_team(alice.user_id, other.id)).data.members == []
    assert (
        await storage.get_session(
            alice.user_id, survivor["agent_id"], survivor["session_id"]
        )
        is not None
    )


@pytest.mark.parametrize("count", [1, 5])
async def test_native_invitation_reuses_lock_parent_and_team_reads(
    storage, tmp_path, count
):
    from types import SimpleNamespace

    from agentscope.app._tool._team_create import TeamCreate
    from test_collaboration import ref, setup_task

    from egis_agentkit.service.access._permission import PermissionService
    from egis_agentkit.service.session._preparation import SessionPreparationService
    from egis_agentkit.service.team._collaboration import TeamCollaborationService
    from egis_agentkit.service.team._invitation import apply_invitations

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    writers = [
        writer,
        *[
            await AgentManager(storage).add_definition(
                alice, expert(name=f"writer_{i}")
            )
            for i in range(count - 1)
        ],
    ]
    async with InMemoryMessageBus() as bus:
        await TeamCollaborationService.stage_collaborators(
            tasks,
            alice,
            task["session_id"],
            bus,
            "selection",
            [ref(w) for w in writers],
        )
        async with SessionPreparationService(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                alice.user_id,
                task["session_id"],
                task["agent_id"],
            )
            with select_queries(storage) as queries:
                await apply_invitations(
                    alice,
                    scope,
                    scope.session_record.state,
                    native,
                    Msg(
                        name="user",
                        role="user",
                        content=[{"type": "text", "text": "work"}],
                    ),
                    PermissionService(storage),
                )
            parent_reads = [q for q in queries if "FROM sessions" in q]
            team_reads = [q for q in queries if "FROM teams" in q]
            assert len(parent_reads) <= count, (
                f"invitation repeated session reads: {len(parent_reads)}"
            )
            assert len(team_reads) <= 2, (
                f"invitation repeated team reads: {len(team_reads)}"
            )
            assert len(queries) <= 7, (
                f"inviting {count} experts executed {len(queries)} SELECTs"
            )
        result = await tasks.get_session(alice, task["session_id"])
        assert len(result["collaborators"]) == count
        assert all(m["status"] == "joined" for m in result["collaborators"])
        assert len({m["worker_session_id"] for m in result["collaborators"]}) == count


async def test_native_verified_record_is_not_reused_by_inherited_background_task(
    storage, tmp_path
):
    import asyncio

    from pydantic import SecretStr

    from egis_agentkit.core.session._context import (
        VerifiedChatAuthorization,
        verified_chat_authorization,
    )
    from egis_agentkit.storage._session_execution import SessionExecutionStorage

    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    task = await SessionService(storage, tmp_path).add_session(
        alice, source["id"], "private", name="before"
    )
    record = await storage.get_session(
        alice.user_id, task["agent_id"], task["session_id"]
    )
    native = SessionExecutionStorage(
        "postgresql+asyncpg://unused", engine=storage.engine
    )
    await native.__aenter__()
    marker = verified_chat_authorization.set(
        VerifiedChatAuthorization(
            alice.user_id,
            record.agent_id,
            record.id,
            SecretStr("test"),
            record,
            asyncio.current_task(),
        )
    )
    try:
        current = record.model_copy(deep=True)
        current.config.name = "after"
        await storage.upsert_session(
            alice.user_id,
            current.agent_id,
            current.config,
            session_id=current.id,
            state=current.state,
        )
        worker = await asyncio.create_task(
            native.get_session(alice.user_id, record.agent_id, record.id)
        )
        assert worker.config.name == "after"
        local = await native.get_session(alice.user_id, record.agent_id, record.id)
        assert local.config.name == "before"
        local.config.name = "mutated"
        assert record.config.name == "before"
    finally:
        verified_chat_authorization.reset(marker)
        await native.__aexit__(None, None, None)


async def test_legacy_roster_deletes_only_agents_with_native_worker_sessions(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    tasks = SessionService(storage, tmp_path)
    source = await AgentManager(storage).add_definition(alice, expert())
    leader, worker, orphan = [
        await tasks.add_session(alice, source["id"], "private") for _ in range(3)
    ]
    await storage.delete_session(
        alice.user_id, orphan["agent_id"], orphan["session_id"]
    )
    team = TeamDefinition.make_record(
        alice.user_id,
        leader_agent_id=leader["agent_id"],
        leader_session_id=leader["session_id"],
        name="legacy",
        description="",
        members=[],
    )
    team.data.member_ids = [worker["agent_id"], orphan["agent_id"]]
    await storage.upsert_team(alice.user_id, team)
    await storage.set_session_team_id(alice.user_id, leader["session_id"], team.id)
    await storage.set_session_team_id(alice.user_id, worker["session_id"], team.id)
    async with InMemoryMessageBus() as bus:
        await tasks.delete_session(alice, leader["session_id"], bus)
    assert await storage.get_agent(alice.user_id, worker["agent_id"]) is None
    assert await storage.get_agent(alice.user_id, orphan["agent_id"]) is not None


async def test_rename_reuses_lock_record_and_rechecks_disable_after_lock(
    storage, tmp_path, monkeypatch
):
    users, admin, alice, _ = await identities(storage)
    tasks = SessionService(storage, tmp_path)
    source = await AgentManager(storage).add_definition(alice, expert())
    task = await tasks.add_session(alice, source["id"], "private")
    async with InMemoryMessageBus() as bus:
        with sql_queries(storage) as queries:
            result = await tasks.update_session_name(
                alice, task["session_id"], bus, "renamed"
            )
        assert result["name"] == "renamed"
        assert len(queries) <= 8, f"rename executed {len(queries)} SQLs"
        original = bus.try_lock

        async def disable_then_lock(key, **kwargs):
            await users.update_user(
                admin, alice.model_copy(update={"is_enabled": False})
            )
            return await original(key, **kwargs)

        monkeypatch.setattr(bus, "try_lock", disable_then_lock)
        with pytest.raises(Forbidden):
            await tasks.update_session_name(
                alice, task["session_id"], bus, "should not be saved"
            )
        record = await storage.get_session(
            alice.user_id, task["agent_id"], task["session_id"]
        )
        assert record.config.name == "renamed"


async def test_project_session_rows_batch_bindings_with_owner_scope(storage, tmp_path):
    from test_collaboration import ref, setup_task

    from egis_agentkit.service.team._collaboration import TeamCollaborationService

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    _, _, _, bob = await identities(storage)
    foreign = await AgentManager(storage).add_definition(
        bob, expert(owner="bob", name="foreign")
    )
    await tasks.add_session(bob, foreign["id"], "private")
    ordinary = await tasks.add_session(
        alice, writer["id"], "private", project_id=task["project_id"]
    )
    async with InMemoryMessageBus() as bus:
        await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "project-cleanup", [ref(writer)]
        )
    with select_queries(storage) as queries:
        rows = await storage.sessions.in_project(alice.user_id, task["project_id"])
    assert {row.id for row in rows} == {task["session_id"], ordinary["session_id"]}
    selected = next(row for row in rows if row.id == task["session_id"])
    assert (
        selected.binding["collaboration"]["members"][0]["definition_id"] == writer["id"]
    )
    assert selected.binding["collaboration"]["members"][0]["native_agent_id"]
    assert len(queries) == 1
    assert await storage.sessions.in_project(bob.user_id, task["project_id"]) == []


async def test_native_invitation_preserves_preexisting_projection_and_primary_session(
    storage, tmp_path
):
    from datetime import timedelta
    from types import SimpleNamespace

    from agentscope.app._tool._team_create import TeamCreate
    from test_collaboration import ref, setup_task

    from egis_agentkit.core.agent._definition_base import AgentDefinitionBase
    from egis_agentkit.service.access._permission import PermissionService
    from egis_agentkit.service.session._preparation import SessionPreparationService
    from egis_agentkit.service.team._collaboration import TeamCollaborationService
    from egis_agentkit.service.team._invitation import apply_invitations

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    async with InMemoryMessageBus() as bus:
        await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "recover", [ref(writer)]
        )
        parent = await tasks.get_session_record(alice, task["session_id"])
        member = parent.state.middle_context["egis_agentkit"]["collaboration"][
            "members"
        ][0]
        definition = await storage.definitions.get_definition_by_reference(member)
        projected = AgentDefinitionBase.build_agent(
            alice.user_id,
            definition,
            source="team",
            record_id=member["native_agent_id"],
            invite_description=member["display_name"],
        )
        projected.created_at -= timedelta(days=2)
        await storage.upsert_agent(alice.user_id, projected)
        primary = await storage.upsert_session(
            alice.user_id,
            projected.id,
            parent.config,
            state=parent.state.model_copy(deep=True),
        )
        async with SessionPreparationService(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                alice.user_id,
                task["session_id"],
                task["agent_id"],
            )
            await apply_invitations(
                alice,
                scope,
                scope.session_record.state,
                native,
                Msg(
                    name="user",
                    role="user",
                    content=[{"type": "text", "text": "retry"}],
                ),
                PermissionService(storage),
            )
        actual = await tasks.get_session(alice, task["session_id"])
        assert actual["collaborators"][0]["status"] == "joined"
        worker_id = actual["collaborators"][0]["worker_session_id"]
        assert worker_id != primary.id
        assert (
            await storage.get_session(alice.user_id, projected.id, primary.id)
            is not None
        )
        assert (
            await storage.get_agent(alice.user_id, projected.id)
        ).created_at == projected.created_at


@pytest.mark.parametrize("is_team", [False, True])
@pytest.mark.parametrize("fail_copy", [False, True])
async def test_branch_is_inaccessible_until_history_and_artifacts_publish(
    storage, tmp_path, monkeypatch, is_team, fail_copy
):
    import asyncio

    import httpx
    from agentscope.message import AssistantMsg, UserMsg
    from egis_agentkit_app.bootstrap import build_app
    from identity_helpers import configure_test_issuer, signed_token
    from sqlalchemy import insert, select
    from test_app_http import Config, app_snapshot

    from egis_agentkit.client.nacos import NacosBootstrap, NacosClient
    from egis_agentkit.storage import tables as t
    from egis_agentkit.storage.model import utcnow

    monkeypatch.setenv("TEST_DOWNLOAD", "test-download-secret-with-thirty-two-bytes")
    monkeypatch.setenv("TEST_MODEL_KEY", "test-only-unused-model-key")
    configure_test_issuer(monkeypatch)
    _, _, alice, _ = await identities(storage)
    async with NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos:
        app = await build_app(
            Config(app_snapshot(tmp_path)), nacos, engine=storage.engine
        )
        async with app.router.lifespan_context(app):
            tasks = app.state.egis_services.sessions
            source = await app.state.egis_services.agents.add_definition(
                alice, expert()
            )
            if is_team:
                source = await TeamDefinition(storage).save(
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
                            {
                                "definition_id": source["id"],
                                "resource_view_type": "private",
                            }
                        ],
                    },
                )
            task = await tasks.add_session(alice, source["id"], "private")
            question = UserMsg("user", "branch question")
            reply = AssistantMsg(
                "coordinator" if is_team else "writer",
                "branch reply",
                finished_at=utcnow().isoformat(),
            )
            for msg in (question, reply):
                await storage.upsert_message(alice.user_id, task["session_id"], msg)
            async with storage.engine.begin() as conn:
                await conn.execute(
                    insert(t.reply_artifacts).values(
                        project_id=task["project_id"],
                        session_id=task["session_id"],
                        reply_id=reply.id,
                        path="result.md",
                        digest="a" * 64,
                    )
                )
            entered, resume = asyncio.Event(), asyncio.Event()
            original = tasks.storage.sessions.save_branch_history
            pending = {}

            async def paused_copy(user_id, record, source_id, message_ids):
                pending["record"] = record
                entered.set()
                await resume.wait()
                if fail_copy:
                    raise RuntimeError("copy failed before publish")
                return await original(user_id, record, source_id, message_ids)

            monkeypatch.setattr(
                tasks.storage.sessions, "save_branch_history", paused_copy
            )
            async with InMemoryMessageBus() as bus:
                operation = asyncio.create_task(
                    tasks.add_session_branch(alice, task["session_id"], reply.id, bus)
                )
                try:
                    await asyncio.wait_for(entered.wait(), 5)
                    branch = pending["record"]
                    all_pending = [
                        row
                        for row in await storage.sessions.in_project(
                            alice.user_id, task["project_id"]
                        )
                        if row.id not in {task["session_id"]}
                    ]
                    # Every newly allocated branch member remains uninitialized.
                    source_record = await storage.get_session(
                        alice.user_id, task["agent_id"], task["session_id"]
                    )
                    old_team = (
                        await storage.get_team(alice.user_id, source_record.team_id)
                        if source_record.team_id
                        else None
                    )
                    old_ids = {
                        task["session_id"],
                        *(
                            [m.session_id for m in old_team.data.members]
                            if old_team
                            else []
                        ),
                    }
                    new_rows = [row for row in all_pending if row.id not in old_ids]
                    assert branch.id in {row.id for row in new_rows}
                    for row in new_rows:
                        assert row.binding["is_initialized"] is False
                        with pytest.raises(Forbidden):
                            await tasks.get_session_record(alice, row.id)
                    visible = await tasks.list_sessions(alice)
                    assert {item["session_id"] for item in visible} == {
                        task["session_id"]
                    }
                    async with httpx.AsyncClient(
                        transport=httpx.ASGITransport(app=app),
                        base_url="http://test",
                        headers={"Authorization": "Bearer " + signed_token("alice")},
                    ) as client:
                        denied = await client.post(
                            "/chat/",
                            json={
                                "agent_id": branch.agent_id,
                                "session_id": branch.id,
                                "inputs": [],
                            },
                        )
                    assert denied.status_code == 403, denied.text
                finally:
                    resume.set()
                    if fail_copy:
                        with pytest.raises(
                            RuntimeError, match="copy failed before publish"
                        ):
                            await operation
                    else:
                        await operation
                if fail_copy:
                    assert (
                        await storage.get_session(
                            alice.user_id, branch.agent_id, branch.id
                        )
                        is None
                    )
                    assert {
                        row.id
                        for row in await storage.sessions.in_project(
                            alice.user_id, task["project_id"]
                        )
                    } == old_ids
                else:
                    loaded = await tasks.get_session_record(alice, branch.id)
                    assert (
                        loaded.state.middle_context["egis_agentkit"]["is_initialized"]
                        is True
                    )
                    copied, more = await storage.list_messages(alice.user_id, branch.id)
                    assert [msg.id for msg in copied] == [question.id, reply.id]
                    assert not more
                    for row in new_rows:
                        assert (
                            await tasks.get_session_record(alice, row.id)
                        ).state.middle_context["egis_agentkit"][
                            "is_initialized"
                        ] is True
                    async with storage.engine.connect() as conn:
                        assert (
                            await conn.scalars(
                                select(t.reply_artifacts.c.path).where(
                                    t.reply_artifacts.c.session_id == branch.id
                                )
                            )
                        ).all() == ["result.md"]
