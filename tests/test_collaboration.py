"""Explicit same-conversation invitations use native membership and inbox delivery."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from agentscope.app._service._projectors._subagent_hitl import SubagentHitlProjector
from agentscope.app._service._session_projection import SessionProjection
from agentscope.app._tool._team_create import TeamCreate
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from agentscope.message import Msg
from agentscope.permission import (
    PermissionBehavior,
    PermissionDecision,
    PermissionEngine,
    PermissionMode,
)
from agentscope.tool import Read, Toolkit
from agentscope.workspace import WorkspaceBase
from egis_agentkit_app.bootstrap._skill_assistant import provision_skill_assistant
from runtime_helpers import session_middlewares
from test_app_http import app_snapshot
from test_domain import expert, identities
from test_resources import archive

from egis_agentkit.bootstrap._middlewares import (
    build_team_prompt_middleware,
    build_tool_access_middleware,
)
from egis_agentkit.bootstrap._skill_tools import deploy_skill_tools
from egis_agentkit.client.nacos._settings import AccessConfig
from egis_agentkit.core.skill._assistant import SKILL_ASSISTANT_ID
from egis_agentkit.errors import CollaborationSessionBusy, Conflict, Forbidden
from egis_agentkit.middleware import ToolAccessMiddleware
from egis_agentkit.service._user import UserService
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.service.project._file_manager import ProjectFileManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.resource._skill_import import SkillImportService
from egis_agentkit.service.session._preparation import SessionPreparationService
from egis_agentkit.service.session._session import SessionService
from egis_agentkit.service.team._collaboration import TeamCollaborationService
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.service.team._tool_callbacks import tool_access_callbacks
from egis_agentkit.storage._session_execution import SessionExecutionStorage
from egis_agentkit.storage.project._files import ProjectFileStorage


async def setup_task(storage, tmp_path, *, project_name=None):
    _, _, alice, _ = await identities(storage)
    agents = AgentManager(storage)
    personal = await PersonalManager(storage).get_or_add_definition(alice)
    writer = await agents.add_definition(alice, expert())
    tasks = SessionService(storage, tmp_path)
    project = await tasks.projects.create(alice, project_name) if project_name else None
    task = await tasks.add_session(
        alice,
        personal["id"],
        "private",
        permission_mode=PermissionMode.BYPASS,
        project_id=project["id"] if project else None,
    )
    return alice, writer, tasks, task


def ref(writer):
    return {
        "definition_id": writer["id"],
        "resource_view_type": "private",
        "resource_version": writer["resource_version"],
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("mode", [PermissionMode.DEFAULT, PermissionMode.BYPASS])
async def test_candidate_checkbox_scopes_model_invitation_to_current_user(
    storage, tmp_path, mode
):
    import httpx
    from agentscope.app._tool import TeamSay
    from egis_agentkit_app.controller.agent._agent import router
    from egis_agentkit_app.controller.base import current_user
    from egis_agentkit_app.controller.base._exception import register_exception_handlers
    from fastapi import FastAPI

    from egis_agentkit.service.session._message_bus_override import SessionMessageBus

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    _, _, _, bob = await identities(storage)
    agents = AgentManager(storage)
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(router, prefix="/manager")
    app.state.egis_services = SimpleNamespace(agents=agents)
    app.dependency_overrides[current_user] = lambda: alice
    url = f"/manager/agents/{writer['id']}/invite-candidate?view=private"
    async with (
        SessionMessageBus(deployment_mode="standalone") as bus,
        httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client,
    ):
        app.dependency_overrides[current_user] = lambda: bob
        assert (await client.put(url)).status_code in (403, 404)
        app.dependency_overrides[current_user] = lambda: alice
        assert (await client.put(url)).status_code == 204
        assert not await storage.expert_candidates.list(bob.user_id)
        catalog = await client.get("/manager/agents", params={"catalog": True})
        assert catalog.json()["items"][0]["is_invite_candidate"] is True

        resources = ResourceService(storage, tmp_path)
        await resources.seed_builtin_tools()
        async with SessionPreparationService(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            toolkit = Toolkit(tools=[
                TeamCreate(storage, bus, SimpleNamespace(), alice.user_id, scope.session_id, scope.agent_id),
                TeamSay(storage, bus, SimpleNamespace(), alice.user_id, scope.session_id, scope.agent_id),
            ])
            agent = SimpleNamespace(toolkit=toolkit, state=scope.session_record.state)

            async def authorize(agent, tool, parameters=None):
                return await resources.tool_registry.authorize(
                    alice, scope, agent, tool, parameters
                )

            middleware = build_tool_access_middleware(
                alice, scope, agents.permissions, authorize_tool=authorize,
            )
            message = Msg(name="user", role="user", content=[{"type": "text", "text": "准备材料"}])

            async def downstream(**kwargs):
                yield "native reply"

            assert [
                item async for item in middleware.on_reply(
                    agent, {"inputs": message}, downstream
                )
            ] == ["native reply"]
            invite = await toolkit.get_tool("AgentInvite")
            assert invite is not None
            assert writer["payload"]["description"] in invite.description
            async def model_tool_names():
                call = await middleware.on_model_call(
                    agent,
                    {"tools": await toolkit.get_tool_schemas()},
                    AsyncMock(side_effect=lambda **params: params),
                )
                return {tool["function"]["name"] for tool in call["tools"]}

            assert await model_tool_names() == {"AgentInvite"}
            assert await build_team_prompt_middleware(scope).on_system_prompt(
                agent, "base"
            ) == "base"
            (target,) = invite.input_schema["properties"]["target"]["enum"]
            assert await authorize(agent, invite, {"target": target, "prompt": "请整理材料"})
            assert await toolkit.get_tool("TeamCreate") is None
            assert scope.session_record.team_id is None
            assert (await client.delete(url)).status_code == 204
            with pytest.raises(Forbidden):
                await invite(target=target, prompt="请整理材料")
            assert scope.session_record.team_id is None
            assert (await client.put(url)).status_code == 204
            agent.state.permission_context.mode = mode
            engine = PermissionEngine(agent.state.permission_context)

            async def native_permission(**kwargs):
                return await engine.check_permission(
                    kwargs["tool"], kwargs["tool_input"]
                )

            decision = await middleware.on_check_permission(
                agent,
                {
                    "tool": invite,
                    "tool_input": {"target": target, "prompt": "请整理材料"},
                },
                native_permission,
            )
            assert decision.behavior == PermissionBehavior.ALLOW
            result = await invite(target=target, prompt="请整理材料")
            assert result.state == "success"
            assert await model_tool_names() == {"AgentInvite", "TeamSay"}
            assert "可通过 AgentInvite 邀请已勾选的候选专家" in (
                await build_team_prompt_middleware(scope).on_system_prompt(
                    agent, "base"
                )
            )
            team = await storage.get_team(alice.user_id, scope.session_record.team_id)
            (member,) = team.data.members
            (projected,) = await bus.collaborator_progress.list(scope.session_id)
            assert projected["worker_session_id"] == member.session_id
            assert projected["status"] == "queued"
            assert "请整理材料" in str(await bus.queue_drain(MessageBusKeys.inbox(member.session_id)))
            assert (await invite(target=target, prompt="请整理材料")).state == "success"
            assert await bus.queue_drain(MessageBusKeys.inbox(member.session_id)) == []


@pytest.mark.asyncio
async def test_explicit_skill_install_dispatches_once_to_the_authorized_expert(
    storage, tmp_path
):
    alice, _, _, task = await setup_task(storage, tmp_path)
    resources = ResourceService(storage, tmp_path)
    files = ProjectFileStorage(tmp_path)
    deploy_skill_tools(resources.tool_registry, SkillImportService(storage, resources))
    await resources.seed_builtin_tools()
    await provision_skill_assistant(storage, None, None)

    def message(text, *paths):
        return Msg(
            name="user",
            role="user",
            content=[{
                "type": "text",
                "text": text + "\n\n已上传的项目资料（工作空间根目录）：\n"
                + "\n".join("- " + path for path in paths),
            }],
        )

    async with (
        InMemoryMessageBus() as bus,
        SessionPreparationService(storage, tmp_path).bind(task["session_id"]) as scope,
    ):
        for path in ("format-report.zip", "other.zip"):
            files.write(alice.user_id, scope.project_id, path, archive(), area="uploads")
        native = TeamCreate(
            storage, bus, SimpleNamespace(), "alice", task["session_id"], task["agent_id"]
        )
        agent = SimpleNamespace(
            state=scope.session_record.state,
            toolkit=SimpleNamespace(get_tool=AsyncMock(return_value=native)),
        )
        _, _, prepare = tool_access_callbacks(
            alice, scope, PermissionService(storage), files=files
        )
        for ambiguous in (
            message("", "format-report.zip"),
            message("不要安装这个技能", "format-report.zip"),
            message("帮我安装这个技能", "format-report.zip", "other.zip"),
            message("帮我安装这个技能", "absent.zip"),
            message("想做一个周报skills"),
            message("新建个日报skills"),
        ):
            await prepare(agent, ambiguous)
            assert scope.session_record.team_id is None
        assert await build_team_prompt_middleware(scope).on_system_prompt(
            agent, "个人助理提示词"
        ) == "个人助理提示词"

        explicit = message("帮我安装这个技能", "format-report.zip")
        await prepare(agent, explicit)
        team = await storage.get_team(alice.user_id, scope.session_record.team_id)
        (member,) = team.data.members
        definition = scope.collaborator_definitions[member.agent_id]
        assert definition["id"] == SKILL_ASSISTANT_ID
        key = MessageBusKeys.inbox(member.session_id)
        assert "format-report.zip" in str(await bus.queue_drain(key))
        async with SessionPreparationService(storage, tmp_path).bind(member.session_id) as worker:
            install, _ = await resources.tool_registry.resolve(alice, worker)
            assert await resources.tool_registry.authorize(alice, worker, None, install)
            await install.call(path="format-report.zip")
        (installed,) = await resources.list(alice, "skill")
        assert installed["is_installed"] and installed["is_enabled"]

        await prepare(agent, explicit)
        assert await bus.queue_drain(key) == []
        await prepare(agent, message("安装 other.zip", "format-report.zip", "other.zip"))
        assert "other.zip" in str(await bus.queue_drain(key))


@pytest.mark.asyncio
async def test_middleware_keeps_the_policy_of_the_locked_run(storage, tmp_path):
    alice, _, _, task = await setup_task(storage, tmp_path)
    config = SimpleNamespace(snapshot=app_snapshot(tmp_path))
    factory = session_middlewares(
        storage,
        users=UserService(storage),
        file_changes=ProjectFileManager(storage, tmp_path),
    )

    async def native_allow(**kwargs):
        return PermissionDecision(PermissionBehavior.ALLOW, "native")

    async with SessionPreparationService(storage, tmp_path, config).bind(
        task["session_id"]
    ):
        config.snapshot = app_snapshot(
            tmp_path, access=AccessConfig(allow_personal_self_use=False)
        )
        middlewares = await factory(
            alice.user_id, task["agent_id"], task["session_id"], WorkspaceBase()
        )
        access = next(mw for mw in middlewares if isinstance(mw, ToolAccessMiddleware))
        result = await access.on_check_permission(
            None, {"tool": Read(model_input_types=[]), "tool_input": {}}, native_allow
        )
        assert result.behavior == PermissionBehavior.ALLOW


@pytest.mark.asyncio
async def test_stage_explicit_invitation_keeps_parent_and_is_idempotent(
    storage, tmp_path
):
    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    async with InMemoryMessageBus() as bus:
        result = await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "request-1", [ref(writer)]
        )
        again = await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "request-1", [ref(writer)]
        )
        assert result == again
        assert result["session_id"] == task["session_id"]
        assert result["collaboration_allowed"] is True
        assert result["collaborators"] == [
            dict(
                ref(writer),
                display_name=writer["agent_display_name"],
                avatar=writer["payload"]["avatar"],
                status="pending",
            )
        ]
        assert len(await tasks.list_sessions(alice)) == 1
        assert (
            await storage.get_session("alice", task["agent_id"], task["session_id"])
        ).team_id is None
        with pytest.raises(Conflict):
            await TeamCollaborationService.stage_collaborators(
                tasks, alice, task["session_id"], bus, "request-1", []
            )
        await bus.try_lock(MessageBusKeys.session_lock(task["session_id"]))
        with pytest.raises(Conflict):
            await TeamCollaborationService.stage_collaborators(
                tasks, alice, task["session_id"], bus, "request-2", [ref(writer)]
            )


@pytest.mark.asyncio
async def test_expert_cannot_lead_collaboration(storage, tmp_path):
    alice, writer, tasks, _ = await setup_task(storage, tmp_path)
    task = await tasks.add_session(alice, writer["id"], "private")
    assert task["collaboration_allowed"] is False
    async with InMemoryMessageBus() as bus:
        with pytest.raises(Forbidden, match="个人"):
            await TeamCollaborationService.stage_collaborators(
                tasks, alice, task["session_id"], bus, "request-1", [ref(writer)]
            )


@pytest.mark.asyncio
async def test_pending_invitation_runs_on_user_input_with_bound_native_workers(
    storage, tmp_path
):
    from egis_agentkit.service.team._collaboration import (
        collaborators,
        validate_collaborators,
    )
    from egis_agentkit.service.team._invitation import apply_invitations

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    analyst = await AgentManager(storage).add_definition(alice, expert(name="analyst"))
    async with InMemoryMessageBus() as bus:
        await TeamCollaborationService.stage_collaborators(
            tasks,
            alice,
            task["session_id"],
            bus,
            "request-1",
            [ref(writer), ref(analyst)],
        )
        async with SessionPreparationService(storage, tmp_path).bind(
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
            assert len(team.data.members) == 2
            members_by_agent = {
                member.agent_id: member for member in team.data.members
            }
            members_by_definition = {
                entry["definition_id"]: members_by_agent[entry["native_agent_id"]]
                for entry in collaborators(state.middle_context["egis_agentkit"])
            }
            member = members_by_definition[writer["id"]]
            second_member = members_by_definition[analyst["id"]]
            assert member.session_id != second_member.session_id
            borrowed = await storage.get_session(
                "alice", member.agent_id, member.session_id
            )
            assert borrowed.origin.type == "team"
            assert (
                borrowed.config.workspace_id == scope.session_record.config.workspace_id
            )
            assert borrowed.config.cwd == scope.session_record.config.cwd
            assert borrowed.state.permission_context.mode == PermissionMode.BYPASS
            binding = borrowed.state.middle_context["egis_agentkit"]
            assert binding["is_initialized"]
            assert "snapshot" not in binding
            assert binding["definition_id"] == writer["id"]
            assert binding["resource_version"] == "1"
            assert binding["collaboration_parent_session_id"] == task["session_id"]
            async with SessionPreparationService(storage, tmp_path).bind(
                member.session_id
            ) as worker:
                assert worker.definition["agent_type"] == "expert"
                assert worker.workdir == scope.workdir
            async with SessionPreparationService(storage, tmp_path).bind(
                second_member.session_id
            ) as worker:
                assert worker.definition["id"] == analyst["id"]
                assert worker.project_id == scope.project_id
            second_inbox = await bus.queue_drain(
                MessageBusKeys.inbox(second_member.session_id)
            )
            assert "请共同完成报告" in str(second_inbox)
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
            assert len((await storage.get_team("alice", team.id)).data.members) == 2
            await validate_collaborators(
                PermissionService(storage), alice, state.middle_context["egis_agentkit"]
            )
            await bus.queue_drain(MessageBusKeys.inbox(member.session_id))
        await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "request-2", [ref(writer)]
        )
        await tasks.update_session_settings(
            alice, task["session_id"], bus, permission_mode=PermissionMode.EXPLORE
        )
        native_storage = SessionExecutionStorage(str(storage.engine.url), storage.engine, files=storage.message_history().files)
        async with SessionPreparationService(storage, tmp_path).bind(
            member.session_id
        ):
            runtime_session = await native_storage.get_session(
                alice.user_id, member.agent_id, member.session_id
            )
            assert (
                runtime_session.state.permission_context.mode == PermissionMode.EXPLORE
            )
        async with SessionPreparationService(storage, tmp_path).bind(
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
            assert len((await storage.get_team("alice", team.id)).data.members) == 2
        worker_sid = second_member.session_id
        parent_sid = task["session_id"]
        message = Msg(
            name="analyst",
            content=[{"type": "text", "text": "分析已完成"}],
            role="assistant",
        )
        await storage.upsert_message(alice.user_id, worker_sid, message)
        projection = SessionProjection(bus)
        kind = SubagentHitlProjector.KIND
        await projection.upsert(
            parent_sid,
            kind,
            SubagentHitlProjector.entry_id(worker_sid, "pending-reply"),
            {"worker_session_id": worker_sid, "reply_id": "pending-reply"},
        )
        publish = bus.publish
        bus.publish = AsyncMock(wraps=publish)
        async with bus.acquire_lock(MessageBusKeys.session_lock(worker_sid)):
            with pytest.raises(CollaborationSessionBusy) as busy:
                await TeamCollaborationService.remove_collaborator(
                    tasks, alice, parent_sid, worker_sid, bus
                )
        assert busy.value.session_id == worker_sid
        bus.publish.assert_awaited_once_with(
            MessageBusKeys.session_interrupt_channel(), {"session_id": worker_sid}
        )
        bus.publish = publish
        removed = await TeamCollaborationService.remove_collaborator(
            tasks, alice, parent_sid, worker_sid, bus
        )
        assert [m["worker_session_id"] for m in removed["collaborators"]] == [
            member.session_id,
        ]
        team = await storage.get_team(alice.user_id, team.id)
        assert [entry.session_id for entry in team.data.members] == [member.session_id]
        retired = await storage.get_session(alice.user_id, second_member.agent_id, worker_sid)
        assert retired.team_id is None
        assert await storage.get_message(alice.user_id, worker_sid, message.id) == message
        assert await projection.list(parent_sid, kind) == []
        with pytest.raises(Forbidden, match="专家协作关系已失效"):
            async with SessionPreparationService(storage, tmp_path).bind(worker_sid):
                pytest.fail("已移除的协作专家仍可执行")
        assert await TeamCollaborationService.remove_collaborator(
            tasks, alice, parent_sid, worker_sid, bus
        ) == removed
        assert len(await tasks.list_sessions(alice)) == 1
        await tasks.delete_session(alice, task["session_id"], bus)
        assert (
            await storage.get_session("alice", member.agent_id, member.session_id)
            is None
        )
        assert await storage.get_agent("alice", member.agent_id) is None
        assert await storage.get_agent("alice", second_member.agent_id) is None
        assert await storage.get_message(alice.user_id, worker_sid, message.id) is None


@pytest.mark.asyncio
async def test_revoked_pending_or_joined_collaborator_blocks_parent_execution(
    storage, tmp_path
):
    from egis_agentkit.service.team._invitation import apply_invitations
    from egis_agentkit.storage.model import Grant

    users, admin, alice, _ = await identities(storage)
    agents = AgentManager(storage)
    personal = await PersonalManager(storage).get_or_add_definition(alice)
    writer = await agents.add_definition(admin, expert("egis-demo", "system"))
    permissions = PermissionService(storage)
    grant = await GrantService(storage).grant(
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
    tasks = SessionService(storage, tmp_path)
    task = await tasks.add_session(alice, personal["id"], "private")
    async with InMemoryMessageBus() as bus:
        await TeamCollaborationService.stage_collaborators(
            tasks,
            alice,
            task["session_id"],
            bus,
            "one",
            [dict(definition_id=writer["id"], resource_view_type="system")],
        )
        async with SessionPreparationService(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            await GrantService(storage).revoke(admin, grant["id"])
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
            async with SessionPreparationService(storage, tmp_path).bind(
                task["session_id"]
            ):
                pytest.fail("revoked collaborator must stop execution")
        direct = [
            row
            for row in await GrantService(storage).list_grants(admin)
            if row["grant_source_type"] != "owner"
        ]
        assert [row["id"] for row in direct] == [grant["id"]]
        metadata = await tasks.get_session(alice, task["session_id"])
        assert metadata["collaborators"][0]["status"] == "pending"
        cleared = await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "clear", []
        )
        assert cleared["collaborators"] == []


@pytest.mark.asyncio
async def test_native_worker_keeps_its_version_and_permission_gate_blocks_roster_tools(
    storage, tmp_path
):
    from agentscope.app._tool._team_say import TeamSay
    from agentscope.permission import PermissionBehavior, PermissionDecision

    alice, writer, tasks, task = await setup_task(
        storage, tmp_path, project_name="协作清理测试"
    )
    async with InMemoryMessageBus() as bus:
        await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "one", [ref(writer)]
        )
        changed = expert()
        changed.id = writer["id"]
        changed.owner_version = writer["owner_version"]
        changed.payload["system_prompt"] = "新版"
        from test_domain import agent_changes

        await AgentManager(storage).update_definition(
            alice, changed.id, agent_changes(changed)
        )
        async with SessionPreparationService(storage, tmp_path).bind(
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
            middleware = build_tool_access_middleware(
                alice, scope, PermissionService(storage)
            )
            team_prompt = build_team_prompt_middleware(scope)

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
            assert (
                f"{writer['agent_name']}@{member.agent_id[:8]}"
                in await team_prompt.on_system_prompt(agent, "system")
            )
        async with SessionPreparationService(storage, tmp_path).bind(
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
    from egis_agentkit_app.controller.base import current_user
    from egis_agentkit_app.controller.base._exception import register_exception_handlers
    from egis_agentkit_app.controller.session import router
    from fastapi import FastAPI

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    app = FastAPI()
    register_exception_handlers(app)
    app.include_router(router, prefix="/manager")
    app.state.egis_services = SimpleNamespace(
        sessions=tasks, teams=TeamDefinition(tasks.storage)
    )
    app.dependency_overrides[current_user] = lambda: alice

    async with InMemoryMessageBus() as bus:
        app.state.egis_bus = bus
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            url = f"/manager/tasks/{task['session_id']}/collaborators"
            body = {"request_id": "one", "experts": [ref(writer)]}
            lock = MessageBusKeys.session_lock(task["session_id"])
            async with bus.acquire_lock(lock):
                busy = await client.post(url, json=body)
            assert busy.status_code == 409
            assert busy.json()["code"] == "collaboration_session_busy"
            assert busy.json()["session_id"] == task["session_id"]
            assert busy.json()["agent_id"] == task["agent_id"]
            response = await client.post(url, json=body)
            assert response.status_code == 200
            assert response.json()["session_id"] == task["session_id"]
            assert (
                response.json()["collaborators"][0]["display_name"]
                == writer["agent_display_name"]
            )
            assert (
                await tasks.get_session_record(alice, task["session_id"])
            ).team_id is None
            invalid = await client.post(
                f"/manager/tasks/{task['session_id']}/collaborators",
                json={
                    "request_id": "two",
                    "experts": [dict(ref(writer), system_prompt="override")],
                },
            )
            assert invalid.status_code == 422
            assert (await client.delete(f"{url}/absent-worker")).status_code == 404


@pytest.mark.asyncio
async def test_invitation_delivery_failure_cleans_allocations_and_retries_same_request(
    storage, tmp_path
):
    from egis_agentkit.service.team._invitation import apply_invitations

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
        await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "one", [ref(writer)]
        )
        async with SessionPreparationService(storage, tmp_path).bind(
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
            assert {
                row.id
                for row in await storage.sessions.in_project(
                    "alice", task["project_id"]
                )
            } == {task["session_id"]}
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
            assert {
                row.id
                for row in await storage.sessions.in_project(
                    "alice", task["project_id"]
                )
            } == {task["session_id"], joined.session_id}
            assert (
                len(await bus.queue_drain(MessageBusKeys.inbox(joined.session_id))) == 1
            )


@pytest.mark.asyncio
async def test_parent_cancel_finishes_native_invite_checkpoint_without_duplicate_delivery(
    storage, tmp_path
):
    import asyncio

    from egis_agentkit.service.team._invitation import apply_invitations

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
        await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "one", [ref(writer)]
        )
        async with SessionPreparationService(storage, tmp_path).bind(
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
    from egis_agentkit.service.team._invitation import apply_invitations

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    reviewer = await AgentManager(storage).add_definition(
        alice, expert(name="reviewer")
    )
    async with InMemoryMessageBus() as bus:
        await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "one", [ref(writer)]
        )
        replaced = await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "two", [ref(reviewer)]
        )
        assert [m["definition_id"] for m in replaced["collaborators"]] == [
            reviewer["id"]
        ]
        cleared = await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "three", []
        )
        assert cleared["collaborators"] == []
        # Retrying a superseded request must not reintroduce its roster.
        retried = await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "one", [ref(writer)]
        )
        assert retried["collaborators"] == []
        async with SessionPreparationService(storage, tmp_path).bind(
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
    from egis_agentkit.service.team._invitation import apply_invitations

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
        await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "one", [ref(writer)]
        )
        monkeypatch.setattr(storage, "set_session_team_id", fail_first_binding)
        async with SessionPreparationService(storage, tmp_path).bind(
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
    from egis_agentkit.service.team._invitation import apply_invitations

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    async with InMemoryMessageBus() as bus:
        await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "one", [ref(writer)]
        )
        async with SessionPreparationService(storage, tmp_path).bind(
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
        async with SessionPreparationService(storage, tmp_path).bind(
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
