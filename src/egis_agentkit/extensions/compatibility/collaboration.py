"""Narrow adapter around native TeamCreate/AgentInvite, never a reply loop.

Native tools own the membership and inbox/wakeup protocol. This adapter binds a
borrowed session before AgentInvite can hand its id to the native bus.

Delivery is at least once: native membership, bus delivery, and the PostgreSQL
journal have separate commit points. Recovery redelivers an uncheckpointed task;
a process death after delivery and before checkpointing can produce a duplicate.
"""

import asyncio
from copy import deepcopy

from agentscope.app._tool._agent_invite import AgentInvite, _display_name
from agentscope.app._tool._team_create import TeamCreate
from agentscope.app._tool._team_say import TeamSay
from agentscope.app.storage import AgentData, AgentRecord
from agentscope.message import Msg, ToolResultState
from sqlalchemy import delete, insert

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.project.service import purge_session_bus
from egis_agentkit.service.team.collaboration import (
    collaboration_allowed,
    collaborators,
    validate_collaborators,
)
from egis_agentkit.storage import tables as t


class InvitationStorage:
    def __init__(self, storage, scope, state, user, permissions, member=None):
        self.storage, self.scope, self.state = storage, scope, state
        self.user, self.permissions, self.member = user, permissions, member
        self.created_sessions = []
        self.created_teams = []

    def __getattr__(self, name):
        return getattr(self.storage, name)

    async def get_session(self, user_id, agent_id, session_id):
        # ExecutionStorage's scope is an assembly snapshot. Team tools need the
        # fresh membership that TeamCreate may have committed this same reply.
        if not agent_id:
            agent_id = (
                self.scope.agent_id if session_id == self.scope.session_id else agent_id
            )
        return await self.storage.read_session_unscoped(user_id, agent_id, session_id)

    async def get_agent(self, user_id, agent_id):
        if self.member and agent_id == self.member["native_agent_id"]:
            await self.permissions.validate_snapshot(self.user, self.member["snapshot"])
        return await self.storage.get_agent(user_id, agent_id)

    async def upsert_team(self, user_id, team):
        existing = await self.storage.get_team(user_id, team.id)
        result = await self.storage.upsert_team(user_id, team)
        if existing is None:
            self.created_teams.append(team.id)
        return result

    async def set_session_team_id(self, user_id, session_id, team_id):
        await self.storage.set_session_team_id(user_id, session_id, team_id)
        if session_id == self.scope.session_id:
            self.scope.session_record.team_id = team_id

    async def upsert_session(self, user_id, agent_id, config, **kwargs):
        member = self.member
        if (
            member is None
            or agent_id != member["native_agent_id"]
            or getattr(kwargs.get("origin"), "type", None) != "team"
        ):
            raise Forbidden("邀请会话不属于已批准专家")
        await self.permissions.validate_snapshot(self.user, member["snapshot"])
        snapshot = member["snapshot"]
        binding = self.state.middle_context["egis_agentkit"]
        config.workspace_id = binding["project_id"]
        config.cwd = self.scope.session_record.config.cwd
        kwargs["state"].middle_context["egis_agentkit"] = {
            "project_id": binding["project_id"],
            "is_initialized": True,
            "definition_id": member["definition_id"],
            "root_resource_type": "agent",
            "resource_view_type": member["resource_view_type"],
            "agent_node_key": snapshot["root_key"],
            "snapshot": deepcopy(snapshot),
            "is_team": False,
            "selected_model_key": None,
            "definition_display_name": member["display_name"],
            "collaboration_parent_session_id": self.scope.session_id,
        }
        session = await self.storage.upsert_session(user_id, agent_id, config, **kwargs)
        self.created_sessions.append(session.id)
        async with self.storage.transaction() as conn:
            await conn.execute(
                insert(t.project_sessions).values(
                    session_id=session.id, project_id=binding["project_id"]
                )
            )
        return session


class InvitationWorkspace:
    def __init__(self, scope):
        self.scope = scope

    async def assign_workspace_id(self, **kwargs):
        return self.scope.session_record.config.workspace_id


async def _save(storage, scope, state):
    # Both the running AgentState and stored session must retain the request
    # journal. Native persistence later writes this same state after reply.
    scope.binding["collaboration"] = state.middle_context["egis_agentkit"][
        "collaboration"
    ]
    scope.session_record.state.middle_context["egis_agentkit"]["collaboration"] = (
        scope.binding["collaboration"]
    )
    await storage.update_session_state(
        scope.user_id, scope.agent_id, scope.session_id, state
    )


def _check(result):
    if result.state == ToolResultState.ERROR:
        raise Conflict(
            "\n".join(getattr(block, "text", "") for block in result.content)
        )


async def apply_invitations(user, scope, state, native_create, inputs, permissions):
    # Stop/cancel must not tear the native membership write away from its inbox
    # delivery and parent checkpoint. This bounded storage/bus operation finishes
    # while the native session lock is still held; the reply then stops normally.
    operation = asyncio.create_task(
        _apply_invitations(user, scope, state, native_create, inputs, permissions)
    )
    try:
        await asyncio.shield(operation)
    except asyncio.CancelledError:
        try:
            await operation
        except Exception:
            pass
        raise


async def _apply_invitations(user, scope, state, native_create, inputs, permissions):
    binding = state.middle_context["egis_agentkit"]
    journal = binding.get("collaboration", {})
    requests = [r for r in journal.get("requests", []) if not r.get("completed")]
    messages = (
        [inputs]
        if isinstance(inputs, Msg)
        else inputs
        if isinstance(inputs, list)
        else []
    )
    messages = [m for m in messages if isinstance(m, Msg) and m.role == "user"]
    # Native inbox wakes and HITL replay/resume cannot consume an invitation.
    if not requests or not messages:
        return
    if not collaboration_allowed(scope.session_record, binding):
        raise Forbidden("只有个人智能体主对话可以邀请专家协作")
    await validate_collaborators(permissions, user, binding)
    if native_create is None:
        raise Forbidden("当前会话没有原生协作能力")
    storage = permissions.storage
    adapter = InvitationStorage(storage, scope, state, user, permissions)
    deps = dict(
        storage=adapter,
        message_bus=native_create._message_bus,
        workspace_manager=InvitationWorkspace(scope),
        user_id=scope.user_id,
        session_id=scope.session_id,
        agent_id=scope.agent_id,
    )
    prompt = "\n".join(m.get_text_content() or str(m.content) for m in messages)
    # Pin the accepted user task before any native membership or bus effect.
    # A later retry turn can then recover the original task rather than merely
    # forwarding a new "please retry" message.
    for request in requests:
        request.setdefault("prompt", prompt)
    await _save(storage, scope, state)
    parent = await storage.read_session_unscoped(
        scope.user_id, scope.agent_id, scope.session_id
    )
    if parent.team_id is None:
        try:
            _check(
                await TeamCreate(**deps)(
                    name="当前对话专家协作",
                    description="用户明确邀请的预配置专家在当前项目协作；使用 TeamSay 汇报成果。",
                )
            )
        except BaseException:
            for team_id in adapter.created_teams:
                await storage.delete_team(scope.user_id, team_id)
            scope.session_record.team_id = None
            raise
    else:
        scope.session_record.team_id = parent.team_id
    for request in requests:
        prompt = request["prompt"]
        for selected in request["experts"]:
            key = selected["definition_id"] + ":" + selected["resource_view_type"]
            if key in request.get("delivered", []):
                continue
            member = next(
                m
                for m in collaborators(binding)
                if all(
                    m[k] == selected[k] for k in ("definition_id", "resource_view_type")
                )
            )
            snapshot = member["snapshot"]
            root = snapshot["root"]
            team = await storage.get_team(scope.user_id, scope.session_record.team_id)
            existing = next(
                (
                    m
                    for m in team.data.members
                    if m.agent_id == member["native_agent_id"]
                ),
                None,
            )
            if existing:
                # Membership commits before the native initial inbox delivery,
                # so it proves neither delivery nor a completed request. Resend
                # conservatively until the request's delivery checkpoint exists.
                _check(
                    await TeamSay(**deps, role="leader")(
                        content=prompt,
                        to=_display_name(root["agent_name"], member["native_agent_id"]),
                    )
                )
                member.update(status="joined", worker_session_id=existing.session_id)
            else:
                record = AgentRecord(
                    id=member["native_agent_id"],
                    user_id=scope.user_id,
                    source="team",
                    data=AgentData(
                        name=root["agent_name"],
                        system_prompt=root["payload"]["system_prompt"],
                        chat_config={
                            "invite_config": {
                                "invitable": True,
                                "invite_description": member["display_name"],
                            }
                        },
                    ),
                )
                await storage.upsert_agent(scope.user_id, record)
                adapter.member = member
                adapter.created_sessions = []
                try:
                    _check(
                        await AgentInvite(**deps, invitable_pool=[record])(
                            target=_display_name(record.data.name, record.id),
                            prompt="你已受用户明确邀请，和个人智能体在同一项目工作目录协作。请完成与你专长有关的工作，并用 TeamSay 向组长汇报。当前用户输入：\n"
                            + prompt,
                        )
                    )
                except BaseException:
                    # Restore only allocations made by this invitation. The
                    # journal remains pending for the next actual user turn.
                    latest = await storage.get_team(scope.user_id, team.id)
                    latest.data.members = [
                        m for m in latest.data.members if m.agent_id != record.id
                    ]
                    await storage.upsert_team(scope.user_id, latest)
                    for sid in adapter.created_sessions:
                        await storage.delete_session(scope.user_id, record.id, sid)
                        await purge_session_bus(native_create._message_bus, sid)
                        async with storage.transaction() as conn:
                            await conn.execute(
                                delete(t.project_sessions).where(
                                    t.project_sessions.c.session_id == sid
                                )
                            )
                    await storage.delete_agent(scope.user_id, record.id)
                    raise
                member.update(
                    status="joined", worker_session_id=adapter.created_sessions[0]
                )
            request.setdefault("delivered", []).append(key)
            await _save(storage, scope, state)
        request["completed"] = True
        await _save(storage, scope, state)
