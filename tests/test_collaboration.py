"""Explicit same-conversation invitations use native membership and inbox delivery."""

from types import SimpleNamespace

import pytest
from agentscope.app._tool._team_create import TeamCreate
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from agentscope.message import Msg
from agentscope.permission import PermissionMode
from agentscope.tool import Toolkit
from test_domain import expert, identities

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.extensions.compatibility.execution import ExecutionCoordinator
from egis_agentkit.extensions.middleware.tool_access import ToolAccessMiddleware
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.session.service import TaskService
from egis_agentkit.services import AgentService


async def setup_task(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    agents = AgentService(storage)
    personal = await agents.personal(alice)
    writer = await agents.save(alice, expert())
    tasks = TaskService(storage, tmp_path)
    task = await tasks.create(
        alice, personal["id"], "private", permission_mode=PermissionMode.BYPASS
    )
    return alice, writer, tasks, task


def ref(writer):
    return {"definition_id": writer["id"], "resource_view_type": "private"}


@pytest.mark.asyncio
async def test_stage_explicit_invitation_keeps_parent_and_is_idempotent(
    storage, tmp_path
):
    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    async with InMemoryMessageBus() as bus:
        result = await tasks.collaboration.stage(
            alice, task["session_id"], bus, "request-1", [ref(writer)]
        )
        again = await tasks.collaboration.stage(
            alice, task["session_id"], bus, "request-1", [ref(writer)]
        )
        assert result == again
        assert result["session_id"] == task["session_id"]
        assert result["collaboration_allowed"] is True
        assert result["collaborators"] == [
            dict(
                ref(writer), display_name=writer["agent_display_name"], status="pending"
            )
        ]
        assert len(await tasks.list(alice)) == 1
        assert (
            await storage.get_session("alice", task["agent_id"], task["session_id"])
        ).team_id is None
        with pytest.raises(Conflict):
            await tasks.collaboration.stage(
                alice, task["session_id"], bus, "request-1", []
            )
        await bus.try_lock(MessageBusKeys.session_lock(task["session_id"]))
        with pytest.raises(Conflict):
            await tasks.collaboration.stage(
                alice, task["session_id"], bus, "request-2", [ref(writer)]
            )


@pytest.mark.asyncio
async def test_expert_cannot_lead_collaboration(storage, tmp_path):
    alice, writer, tasks, _ = await setup_task(storage, tmp_path)
    task = await tasks.create(alice, writer["id"], "private")
    assert task["collaboration_allowed"] is False
    async with InMemoryMessageBus() as bus:
        with pytest.raises(Forbidden, match="个人"):
            await tasks.collaboration.stage(
                alice, task["session_id"], bus, "request-1", [ref(writer)]
            )


@pytest.mark.asyncio
async def test_pending_invitation_runs_on_user_input_with_bound_native_worker(
    storage, tmp_path
):
    from egis_agentkit.extensions.compatibility.collaboration import apply_invitations
    from egis_agentkit.service.team.collaboration import validate_collaborators

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    async with InMemoryMessageBus() as bus:
        await tasks.collaboration.stage(
            alice, task["session_id"], bus, "request-1", [ref(writer)]
        )
        async with ExecutionCoordinator(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            state = scope.session_record.state
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                "alice",
                task["session_id"],
                task["agent_id"],
            )
            await apply_invitations(
                alice, scope, state, native, None, PermissionService(storage)
            )
            assert scope.session_record.team_id is None
            await apply_invitations(
                alice,
                scope,
                state,
                native,
                Msg(
                    name="user",
                    content=[{"type": "text", "text": "请共同完成报告"}],
                    role="user",
                ),
                PermissionService(storage),
            )
            await storage.update_session_state(
                "alice", task["agent_id"], task["session_id"], state
            )
            team = await storage.get_team("alice", scope.session_record.team_id)
            assert len(team.data.members) == 1
            member = team.data.members[0]
            borrowed = await storage.get_session(
                "alice", member.agent_id, member.session_id
            )
            assert borrowed.origin.type == "team"
            assert borrowed.config.workspace_id == task["project_id"]
            assert borrowed.config.cwd == scope.session_record.config.cwd
            assert borrowed.state.permission_context.mode == PermissionMode.DEFAULT
            binding = borrowed.state.middle_context["egis_agentkit"]
            assert binding["is_initialized"]
            assert binding["snapshot"]["root"]["agent_name"] == writer["agent_name"]
            assert binding["collaboration_parent_session_id"] == task["session_id"]
            async with ExecutionCoordinator(storage, tmp_path).bind(
                member.session_id
            ) as worker:
                assert worker.definition["agent_type"] == "expert"
                assert worker.workdir == scope.workdir
            await apply_invitations(
                alice,
                scope,
                state,
                native,
                Msg(
                    name="user",
                    content=[{"type": "text", "text": "再来一次"}],
                    role="user",
                ),
                PermissionService(storage),
            )
            assert len((await storage.get_team("alice", team.id)).data.members) == 1
            await validate_collaborators(
                PermissionService(storage), alice, state.middle_context["egis_agentkit"]
            )
            await bus.queue_drain(MessageBusKeys.inbox(member.session_id))
        await tasks.collaboration.stage(
            alice, task["session_id"], bus, "request-2", [ref(writer)]
        )
        async with ExecutionCoordinator(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                "alice",
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
                    content=[{"type": "text", "text": "新任务"}],
                    role="user",
                ),
                PermissionService(storage),
            )
            inbox = await bus.queue_drain(MessageBusKeys.inbox(member.session_id))
            assert len(inbox) == 1
            assert "新任务" in str(inbox)
            assert len((await storage.get_team("alice", team.id)).data.members) == 1
        assert len(await tasks.list(alice)) == 1
        await tasks.delete(alice, task["session_id"], bus)
        assert (
            await storage.get_session("alice", member.agent_id, member.session_id)
            is None
        )
        assert await storage.get_agent("alice", member.agent_id) is None


@pytest.mark.asyncio
async def test_revoked_pending_or_joined_collaborator_blocks_parent_execution(
    storage, tmp_path
):
    from egis_agentkit.extensions.compatibility.collaboration import apply_invitations
    from egis_agentkit.models import Grant

    users, admin, alice, _ = await identities(storage)
    agents = AgentService(storage)
    personal = await agents.personal(alice)
    writer = await agents.save(admin, expert("egis-demo", "system"))
    permissions = PermissionService(storage)
    grant = await permissions.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id="alice",
            resource_type="agent",
            resource_id=writer["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="egis-demo",
        ),
    )
    tasks = TaskService(storage, tmp_path)
    task = await tasks.create(alice, personal["id"], "private")
    async with InMemoryMessageBus() as bus:
        await tasks.collaboration.stage(
            alice,
            task["session_id"],
            bus,
            "one",
            [dict(definition_id=writer["id"], resource_view_type="system")],
        )
        async with ExecutionCoordinator(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            await permissions.revoke(admin, grant["id"])
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                "alice",
                task["session_id"],
                task["agent_id"],
            )
            with pytest.raises(Forbidden):
                await apply_invitations(
                    alice,
                    scope,
                    scope.session_record.state,
                    native,
                    Msg(
                        name="user",
                        role="user",
                        content=[{"type": "text", "text": "execute"}],
                    ),
                    permissions,
                )
            assert scope.session_record.team_id is None
        with pytest.raises(Forbidden):
            async with ExecutionCoordinator(storage, tmp_path).bind(task["session_id"]):
                pytest.fail("revoked collaborator must stop execution")
        assert len(await permissions.list_grants(admin)) == 1
        metadata = await tasks.get(alice, task["session_id"])
        assert metadata["collaborators"][0]["status"] == "pending"
        cleared = await tasks.collaboration.stage(
            alice, task["session_id"], bus, "clear", []
        )
        assert cleared["collaborators"] == []


@pytest.mark.asyncio
async def test_native_worker_keeps_snapshot_and_permission_gate_blocks_roster_tools(
    storage, tmp_path
):
    from agentscope.app._tool._team_say import TeamSay
    from agentscope.permission import PermissionBehavior, PermissionDecision

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    async with InMemoryMessageBus() as bus:
        await tasks.collaboration.stage(
            alice, task["session_id"], bus, "one", [ref(writer)]
        )
        async with ExecutionCoordinator(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            toolkit = Toolkit(
                tools=[
                    TeamCreate(
                        storage,
                        bus,
                        SimpleNamespace(),
                        "alice",
                        task["session_id"],
                        task["agent_id"],
                    ),
                    TeamSay(
                        storage,
                        bus,
                        SimpleNamespace(),
                        "alice",
                        task["session_id"],
                        task["agent_id"],
                    ),
                ]
            )
            agent = SimpleNamespace(toolkit=toolkit, state=scope.session_record.state)
            middleware = ToolAccessMiddleware(
                alice, scope, permissions=PermissionService(storage)
            )

            async def downstream(**kwargs):
                yield "native reply"

            inputs = dict(
                inputs=Msg(
                    name="user",
                    role="user",
                    content=[{"type": "text", "text": "execute"}],
                )
            )
            assert [
                item async for item in middleware.on_reply(agent, inputs, downstream)
            ] == ["native reply"]
            assert await toolkit.get_tool("TeamCreate") is None
            assert await toolkit.get_tool("TeamSay") is not None

            async def native_allow(**kwargs):
                return PermissionDecision(PermissionBehavior.ALLOW, "native")

            for name in (
                "AgentInvite",
                "AgentCreate",
                "TeamCreate",
                "TeamDelete",
                "ScheduleCreate",
            ):
                decision = await middleware.on_check_permission(
                    agent,
                    {"tool": SimpleNamespace(name=name), "tool_input": {}},
                    native_allow,
                )
                assert decision.behavior == PermissionBehavior.DENY
            team = await storage.get_team("alice", scope.session_record.team_id)
            member = team.data.members[0]
        changed = expert()
        changed.id = writer["id"]
        changed.config_version = writer["config_version"]
        changed.payload["system_prompt"] = "新版"
        await AgentService(storage).save(alice, changed)
        async with ExecutionCoordinator(storage, tmp_path).bind(
            member.session_id
        ) as scope:
            assert scope.definition["payload"]["system_prompt"] == "旧版"
            assert scope.definition["agent_type"] == "expert"
        project_id = task["project_id"]
        await tasks.projects.delete(alice, project_id, bus)
        assert await storage.get_agent("alice", member.agent_id) is None


@pytest.mark.asyncio
async def test_collaboration_http_contract_stages_without_native_execution(
    storage, tmp_path
):
    import httpx
    from egis_agentkit_app.controllers.session import router
    from egis_agentkit_app.security import current_user
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    from egis_agentkit.errors import AgentKitError

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    app = FastAPI()
    app.include_router(router, prefix="/manager")
    app.state.egis_services = SimpleNamespace(tasks=tasks)
    app.dependency_overrides[current_user] = lambda: alice

    @app.exception_handler(AgentKitError)
    async def domain_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    async with InMemoryMessageBus() as bus:
        app.state.egis_bus = bus
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            response = await client.post(
                f"/manager/tasks/{task['session_id']}/collaborators",
                json={"request_id": "one", "experts": [ref(writer)]},
            )
            assert response.status_code == 200
            assert response.json()["session_id"] == task["session_id"]
            assert (
                response.json()["collaborators"][0]["display_name"]
                == writer["agent_display_name"]
            )
            assert (await tasks.session(alice, task["session_id"])).team_id is None
            invalid = await client.post(
                f"/manager/tasks/{task['session_id']}/collaborators",
                json={
                    "request_id": "two",
                    "experts": [dict(ref(writer), system_prompt="override")],
                },
            )
            assert invalid.status_code == 422


@pytest.mark.asyncio
async def test_invitation_delivery_failure_cleans_allocations_and_retries_same_request(
    storage, tmp_path
):
    from egis_agentkit.extensions.compatibility.collaboration import apply_invitations

    class FailedDeliveryBus(InMemoryMessageBus):
        fail = True
        failed_session_id = None

        async def queue_push(self, key, payload):
            if self.fail and key == MessageBusKeys.wakeup_queue():
                self.fail = False
                self.failed_session_id = payload["session_id"]
                raise RuntimeError("暂时无法唤醒专家")
            return await super().queue_push(key, payload)

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    async with FailedDeliveryBus() as bus:
        await tasks.collaboration.stage(
            alice, task["session_id"], bus, "one", [ref(writer)]
        )
        async with ExecutionCoordinator(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                "alice",
                task["session_id"],
                task["agent_id"],
            )
            message = Msg(
                name="user", role="user", content=[{"type": "text", "text": "execute"}]
            )
            with pytest.raises(Conflict, match="暂时无法唤醒专家"):
                await apply_invitations(
                    alice,
                    scope,
                    scope.session_record.state,
                    native,
                    message,
                    PermissionService(storage),
                )
            member = scope.binding["collaboration"]["members"][0]
            assert member["status"] == "pending"
            assert not (
                await storage.get_team("alice", scope.session_record.team_id)
            ).data.members
            assert await storage.get_agent("alice", member["native_agent_id"]) is None
            # Compensation must remove the initial inbox item as well.
            assert not await bus.queue_drain(
                MessageBusKeys.inbox(bus.failed_session_id)
            )
            from sqlalchemy import text

            async with storage.engine.connect() as conn:
                assert (
                    await conn.execute(text("SELECT count(*) FROM ak_project_sessions"))
                ).scalar_one() == 1
            await apply_invitations(
                alice,
                scope,
                scope.session_record.state,
                native,
                message,
                PermissionService(storage),
            )
            team = await storage.get_team("alice", scope.session_record.team_id)
            joined = team.data.members[0]
            assert len(team.data.members) == 1
            assert (
                len(await bus.queue_drain(MessageBusKeys.inbox(joined.session_id))) == 1
            )


@pytest.mark.asyncio
async def test_parent_cancel_finishes_native_invite_checkpoint_without_duplicate_delivery(
    storage, tmp_path
):
    import asyncio

    from egis_agentkit.extensions.compatibility.collaboration import apply_invitations

    class PausedWakeBus(InMemoryMessageBus):
        def __init__(self):
            super().__init__()
            self.started, self.release = asyncio.Event(), asyncio.Event()

        async def queue_push(self, key, payload):
            if key == MessageBusKeys.wakeup_queue():
                self.started.set()
                await self.release.wait()
            return await super().queue_push(key, payload)

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    async with PausedWakeBus() as bus:
        await tasks.collaboration.stage(
            alice, task["session_id"], bus, "one", [ref(writer)]
        )
        async with ExecutionCoordinator(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                "alice",
                task["session_id"],
                task["agent_id"],
            )
            message = Msg(
                name="user", role="user", content=[{"type": "text", "text": "execute"}]
            )
            running = asyncio.create_task(
                apply_invitations(
                    alice,
                    scope,
                    scope.session_record.state,
                    native,
                    message,
                    PermissionService(storage),
                )
            )
            await asyncio.wait_for(bus.started.wait(), timeout=3)
            running.cancel()
            bus.release.set()
            with pytest.raises(asyncio.CancelledError):
                await running
            member = scope.binding["collaboration"]["members"][0]
            assert member["status"] == "joined"
            assert scope.binding["collaboration"]["requests"][0]["completed"]
            team = await storage.get_team("alice", scope.session_record.team_id)
            assert len(team.data.members) == 1
            assert (
                len(
                    await bus.queue_drain(
                        MessageBusKeys.inbox(team.data.members[0].session_id)
                    )
                )
                == 1
            )


@pytest.mark.asyncio
async def test_replacing_or_clearing_prepared_request_never_invites_removed_experts(
    storage, tmp_path
):
    from egis_agentkit.extensions.compatibility.collaboration import apply_invitations

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    reviewer = await AgentService(storage).save(alice, expert(name="reviewer"))
    async with InMemoryMessageBus() as bus:
        await tasks.collaboration.stage(
            alice, task["session_id"], bus, "one", [ref(writer)]
        )
        replaced = await tasks.collaboration.stage(
            alice, task["session_id"], bus, "two", [ref(reviewer)]
        )
        assert [m["definition_id"] for m in replaced["collaborators"]] == [
            reviewer["id"]
        ]
        cleared = await tasks.collaboration.stage(
            alice, task["session_id"], bus, "three", []
        )
        assert cleared["collaborators"] == []
        # Retrying a superseded request must not reintroduce its roster.
        retried = await tasks.collaboration.stage(
            alice, task["session_id"], bus, "one", [ref(writer)]
        )
        assert retried["collaborators"] == []
        async with ExecutionCoordinator(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                "alice",
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
                    content=[{"type": "text", "text": "execute"}],
                ),
                PermissionService(storage),
            )
            assert scope.session_record.team_id is None


@pytest.mark.asyncio
async def test_failed_native_team_creation_removes_only_new_team_and_retries(
    storage, tmp_path, monkeypatch
):
    from egis_agentkit.extensions.compatibility.collaboration import apply_invitations

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    original = storage.set_session_team_id
    fail = True

    async def fail_first_binding(user_id, session_id, team_id):
        nonlocal fail
        if fail:
            fail = False
            raise RuntimeError("临时团队绑定失败")
        return await original(user_id, session_id, team_id)

    async with InMemoryMessageBus() as bus:
        await tasks.collaboration.stage(
            alice, task["session_id"], bus, "one", [ref(writer)]
        )
        monkeypatch.setattr(storage, "set_session_team_id", fail_first_binding)
        async with ExecutionCoordinator(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                "alice",
                task["session_id"],
                task["agent_id"],
            )
            message = Msg(
                name="user", role="user", content=[{"type": "text", "text": "execute"}]
            )
            with pytest.raises(Conflict, match="临时团队绑定失败"):
                await apply_invitations(
                    alice,
                    scope,
                    scope.session_record.state,
                    native,
                    message,
                    PermissionService(storage),
                )
            assert not await storage.list_teams("alice")
            await apply_invitations(
                alice,
                scope,
                scope.session_record.state,
                native,
                message,
                PermissionService(storage),
            )
            assert len(await storage.list_teams("alice")) == 1


@pytest.mark.asyncio
async def test_recovery_delivers_task_when_native_membership_committed_before_inbox(
    storage, tmp_path
):
    """Native membership alone cannot prove the initial task reached its inbox."""
    from egis_agentkit.extensions.compatibility.collaboration import apply_invitations

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    async with InMemoryMessageBus() as bus:
        await tasks.collaboration.stage(
            alice, task["session_id"], bus, "one", [ref(writer)]
        )
        async with ExecutionCoordinator(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                "alice",
                task["session_id"],
                task["agent_id"],
            )
            message = Msg(
                name="user", role="user", content=[{"type": "text", "text": "首条任务"}]
            )
            await apply_invitations(
                alice,
                scope,
                scope.session_record.state,
                native,
                message,
                PermissionService(storage),
            )
            team = await storage.get_team("alice", scope.session_record.team_id)
            worker = team.data.members[0]
            # Reproduce the durable state of a process death after native
            # upsert_team.members but before deliver_to_inbox/checkpoint.
            await bus.queue_drain(MessageBusKeys.inbox(worker.session_id))
            await bus.queue_drain(MessageBusKeys.wakeup_queue())
            journal = scope.session_record.state.middle_context["egis_agentkit"][
                "collaboration"
            ]
            assert journal["requests"][0]["prompt"] == "首条任务"
            journal["members"][0]["status"] = "pending"
            journal["members"][0].pop("worker_session_id")
            journal["requests"][0]["completed"] = False
            journal["requests"][0].pop("delivered")
            await storage.update_session_state(
                "alice",
                task["agent_id"],
                task["session_id"],
                scope.session_record.state,
            )
        async with ExecutionCoordinator(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            native = TeamCreate(
                storage,
                bus,
                SimpleNamespace(),
                "alice",
                task["session_id"],
                task["agent_id"],
            )
            retry = Msg(
                name="user", role="user", content=[{"type": "text", "text": "请重试"}]
            )
            await apply_invitations(
                alice,
                scope,
                scope.session_record.state,
                native,
                retry,
                PermissionService(storage),
            )
            inbox = await bus.queue_drain(MessageBusKeys.inbox(worker.session_id))
            assert len(inbox) == 1
            assert "首条任务" in str(inbox)
            assert len((await storage.get_team("alice", team.id)).data.members) == 1
            assert scope.binding["collaboration"]["requests"][0]["completed"]
            assert scope.binding["collaboration"]["members"][0]["status"] == "joined"
