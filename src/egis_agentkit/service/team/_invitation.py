"""回复开始时由工具权限 Middleware 调用，执行已授权的专家邀请。

原生入团、Inbox 投递和本地进度分别提交，恢复时重投尚未确认的任务。"""

import asyncio

from agentscope.app._tool._agent_invite import AgentInvite, _display_name
from agentscope.app._tool._team_create import TeamCreate
from agentscope.app._tool._team_say import TeamSay
from agentscope.app.storage import SessionRecord
from agentscope.message import Msg, ToolResultState

from egis_agentkit.client.nacos._settings import TeamInviteConfig
from egis_agentkit.core.agent._definition_base import AgentDefinitionBase
from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.project._project_manager import purge_session_bus
from egis_agentkit.service.team._collaboration import (
    collaboration_allowed,
    collaborators,
    validate_collaborators,
)
from egis_agentkit.storage.model import new_id
from egis_agentkit.storage.model._definition import build_definition_reference


class InvitationStorage:
    """将原生邀请产生的成员会话绑定到已选择的专家版本。"""

    def __init__(self, storage, scope, state, user, permissions, snapshot, member=None):
        self.storage, self.scope, self.state = storage, scope, state
        self.user, self.permissions, self.member = user, permissions, member
        self.snapshot = snapshot
        self.created_sessions = []
        self.created_teams = []
        # This adapter exists for one lock-held invitation operation only.
        self.session_records = {scope.session_id: scope.session_record}
        self.team_records = {}
        self.agent_records = {}
        self.primary_sessions = {}
        self.projection_ids = set()

    def __getattr__(self, name):
        return getattr(self.storage, name)

    async def prime_members(self, agent_ids):
        """一次读取已存在的投影，兼容进程在写 Agent 后意外退出的重试。"""
        self.projection_ids = set(agent_ids)
        agents, sessions = await self.storage.sessions.invitation_records(
            self.scope.user_id, self.projection_ids
        )
        self.agent_records.update(
            ((self.scope.user_id, identifier), record)
            for identifier, record in agents.items()
        )
        self.primary_sessions = sessions
        self.session_records.update(
            (record.id, record)
            for record in sessions.values()
            if record.id != self.scope.session_id
        )

    async def list_sessions(self, user_id, agent_id):
        """AgentInvite 只使用最新主会话作配置回退，复用本次范围查询。"""
        if user_id == self.scope.user_id and agent_id in self.projection_ids:
            record = self.primary_sessions.get(agent_id)
            return [record.model_copy(deep=True)] if record is not None else []
        return await self.storage.list_sessions(user_id, agent_id)

    async def get_session(self, user_id, agent_id, session_id):
        # 原生工具读取 TeamCreate 在同一回合刚写入的最新成员关系。
        if not agent_id:
            agent_id = (
                self.scope.agent_id if session_id == self.scope.session_id else agent_id
            )
        record = self.session_records.get(session_id)
        if record is None:
            record = await self.storage.read_session_unscoped(
                user_id, agent_id, session_id
            )
            if record is not None:
                self.session_records[session_id] = record
        if (
            record is None
            or record.user_id != user_id
            or (agent_id and record.agent_id != agent_id)
        ):
            return None
        return record.model_copy(deep=True)

    async def get_team(self, user_id, team_id):
        key = user_id, team_id
        if key not in self.team_records:
            self.team_records[key] = await self.storage.get_team(user_id, team_id)
        record = self.team_records[key]
        return record.model_copy(deep=True) if record is not None else None

    async def get_agent(self, user_id, agent_id):
        if self.member and agent_id == self.member["native_agent_id"]:
            definition = await self.snapshot.definition_view(self.member)
            await self.snapshot.require_execution(definition, allow_pinned=True)
        record = self.agent_records.get((user_id, agent_id))
        if record is None:
            record = await self.storage.get_agent(user_id, agent_id)
            if record is not None:
                self.agent_records[user_id, agent_id] = record.model_copy(deep=True)
        return record.model_copy(deep=True) if record is not None else None

    async def upsert_team(self, user_id, team):
        existing = await self.get_team(user_id, team.id)
        if existing is None:
            await self.storage.sessions.publish_created([], [], team)
            self.created_teams.append(team.id)
        else:
            team.created_at = existing.created_at
            await self.storage.sessions.save_team(team)
        self.team_records[user_id, team.id] = team.model_copy(deep=True)
        return team

    async def set_session_team_id(self, user_id, session_id, team_id):
        await self.storage.set_session_team_id(user_id, session_id, team_id)
        if session_id in self.session_records:
            self.session_records[session_id].team_id = team_id

    async def upsert_session(self, user_id, agent_id, config, **kwargs):
        """校验邀请专家的固定版本并持久化成员会话引用。"""
        member = self.member
        if (
            member is None
            or agent_id != member["native_agent_id"]
            or getattr(kwargs.get("origin"), "type", None) != "team"
        ):
            raise Forbidden("邀请会话不属于已批准专家")
        definition = await self.snapshot.definition_view(member)
        await self.snapshot.require_execution(definition, allow_pinned=True)
        binding = self.state.middle_context["egis_agentkit"]
        config, assembled = AgentDefinitionBase.build_session(
            definition,
            project_id=binding["project_id"],
            workspace_id=self.scope.session_record.config.workspace_id,
            session_id=kwargs.setdefault("session_id", new_id()),
            cwd=self.scope.session_record.config.cwd,
            config=config,
            is_initialized=True,
        )
        assembled.middle_context["egis_agentkit"]["collaboration_parent_session_id"] = (
            self.scope.session_id
        )
        kwargs["state"].middle_context["egis_agentkit"] = assembled.middle_context[
            "egis_agentkit"
        ]
        kwargs["state"].session_id = assembled.session_id
        kwargs["state"].permission_context.mode = self.state.permission_context.mode
        session = SessionRecord(
            user_id=user_id,
            agent_id=agent_id,
            config=config,
            id=kwargs["session_id"],
            state=kwargs["state"],
            origin=kwargs["origin"],
        )
        await self.storage.sessions.publish_created([], [session])
        self.created_sessions.append(session.id)
        self.session_records[session.id] = session.model_copy(deep=True)
        return session


class InvitationWorkspace:
    def __init__(self, scope):
        self.scope = scope

    async def assign_workspace_id(self, **kwargs):
        return self.scope.session_record.config.workspace_id


async def _save(storage, scope, state):
    """同步本轮状态和会话中的邀请进度，供回复结束持久化和重试使用。"""
    scope.binding["collaboration"] = state.middle_context["egis_agentkit"][
        "collaboration"
    ]
    scope.session_record.state.middle_context["egis_agentkit"]["collaboration"] = (
        scope.binding["collaboration"]
    )
    scope.session_record.state = state
    await storage.sessions.save_state(scope.session_record)


def _check(result):
    if result.state == ToolResultState.ERROR:
        raise Conflict(
            "\n".join(getattr(block, "text", "") for block in result.content)
        )


async def invite_candidate(
    user, scope, state, native_create, definition, prompt, permissions
):
    """在当前会话锁内登记模型选中的候选，并执行一次原生邀请。"""
    if not collaboration_allowed(scope.session_record, scope.root_definition):
        raise Forbidden("只有个人助理对话可以邀请专家协作")
    selected = build_definition_reference(definition)
    reference = {
        key: selected[key]
        for key in ("definition_id", "resource_view_type", "resource_version")
    }
    binding = state.middle_context["egis_agentkit"]
    journal = binding.setdefault("collaboration", {"requests": [], "members": []})
    members = collaborators(binding)
    member = next(
        (
            item
            for item in members
            if item["definition_id"] == selected["definition_id"]
            and item["resource_view_type"] == selected["resource_view_type"]
        ),
        None,
    )
    if member is not None:
        if member["status"] == "joined":
            handle = _display_name(definition["agent_name"], member["native_agent_id"])
            return f"{handle} 已在当前会话中。"
        if member["resource_version"] != selected["resource_version"]:
            raise Conflict("等待邀请的专家版本已变化")
    else:
        limits = (
            scope.configuration.team.invite
            if scope.configuration
            else TeamInviteConfig()
        )
        if len(members) >= limits.max_members:
            raise Conflict(f"当前对话最多邀请 {limits.max_members} 位专家")
        member = {
            **selected,
            "display_name": definition["agent_display_name"],
            "avatar": definition["payload"].get("avatar"),
            "native_agent_id": new_id(),
            "status": "pending",
        }
        journal["members"].append(member)
    request_id = next(
        (
            record["request_id"]
            for record in journal["requests"]
            if record.get("source") == "agent_invite"
            and not record["completed"]
            and record["experts"] == [reference]
        ),
        None,
    )
    if request_id is None:
        request_id = f"agent-invite:{new_id()}"
        journal["requests"].append(
            {
                "request_id": request_id,
                "experts": [reference],
                "completed": False,
                "source": "agent_invite",
                "prompt": prompt,
            }
        )
    await apply_invitations(
        user,
        scope,
        state,
        native_create,
        None,
        permissions,
        automatic_request_id=request_id,
    )
    if member["status"] != "joined":
        raise Conflict("专家尚未完成邀请")
    handle = _display_name(definition["agent_name"], member["native_agent_id"])
    return f"{handle} 已加入协作，任务已发送。"


async def apply_invitations(
    user, scope, state, native_create, inputs, permissions, *, automatic_request_id=None
):
    """在会话锁内完成原生入团、任务投递和进度提交，再响应取消。"""
    operation = asyncio.create_task(
        _apply_invitations(
            user, scope, state, native_create, inputs, permissions, automatic_request_id
        )
    )
    try:
        await asyncio.shield(operation)
    except asyncio.CancelledError:
        try:
            await operation
        except Exception:
            pass
        raise


async def _apply_invitations(
    user, scope, state, native_create, inputs, permissions, automatic_request_id
):
    """完成待处理邀请，保证重试使用原专家版本和原用户任务。"""
    binding = state.middle_context["egis_agentkit"]
    journal = binding.get("collaboration", {})
    requests = [
        request
        for request in journal.get("requests", [])
        if not request.get("completed")
        and (
            request["request_id"] == automatic_request_id
            if automatic_request_id is not None
            else request.get("source") != "agent_invite"
        )
    ]
    messages = (
        [inputs]
        if isinstance(inputs, Msg)
        else inputs
        if isinstance(inputs, list)
        else []
    )
    messages = [m for m in messages if isinstance(m, Msg) and m.role == "user"]
    # 只有真实用户输入能消费邀请，Inbox 唤醒和工具确认恢复不会触发。
    if not requests or (automatic_request_id is None and not messages):
        return
    if not collaboration_allowed(scope.session_record, scope.root_definition):
        raise Forbidden("只有个人智能体主对话可以邀请专家协作")
    # Begin a fresh authorization boundary for this invitation operation. Native
    # adapters reuse it within the held leader lock; each worker prepares afresh.
    snapshot = await permissions.read_snapshot(user, collaborators(binding))
    definitions = await validate_collaborators(
        permissions, user, binding, snapshot=snapshot
    )
    scope.collaborator_definitions.update(definitions)
    if native_create is None:
        raise Forbidden("当前会话没有原生协作能力")
    storage = permissions.storage
    adapter = InvitationStorage(storage, scope, state, user, permissions, snapshot)
    await adapter.prime_members(
        {
            scope.agent_id,
            *[member["native_agent_id"] for member in collaborators(binding)],
        }
    )
    deps = dict(
        storage=adapter,
        message_bus=native_create._message_bus,
        workspace_manager=InvitationWorkspace(scope),
        user_id=scope.user_id,
        session_id=scope.session_id,
        agent_id=scope.agent_id,
    )
    prompt = "\n".join(m.get_text_content() or str(m.content) for m in messages)
    # 先记录原始任务，保证重试继续投递该任务。
    for request in requests:
        request.setdefault("prompt", prompt)
    await _save(storage, scope, state)
    parent = scope.session_record
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
                if all(m[k] == value for k, value in selected.items())
            )
            root = definitions[member["native_agent_id"]]
            team = await adapter.get_team(scope.user_id, scope.session_record.team_id)
            existing = next(
                (
                    m
                    for m in team.data.members
                    if m.agent_id == member["native_agent_id"]
                ),
                None,
            )
            if existing:
                # 入团先于 Inbox 投递。缺少投递检查点时重新发送原任务。
                _check(
                    await TeamSay(**deps, role="leader")(
                        content=prompt,
                        to=_display_name(root["agent_name"], member["native_agent_id"]),
                    )
                )
                member.update(status="joined", worker_session_id=existing.session_id)
            else:
                record = adapter.agent_records.get(
                    (scope.user_id, member["native_agent_id"])
                )
                if record is None:
                    record = AgentDefinitionBase.build_agent(
                        scope.user_id,
                        root,
                        source="team",
                        record_id=member["native_agent_id"],
                        invite_description=member["display_name"],
                    )
                    await storage.sessions.publish_created([record], [])
                    adapter.agent_records[scope.user_id, record.id] = record.model_copy(
                        deep=True
                    )
                adapter.member = member
                adapter.created_sessions = []
                try:
                    first_task = prompt
                    if request.get("source") != "agent_invite":
                        first_task = (
                            "你已受用户明确邀请，和个人智能体在同一项目工作目录协作。请完成与你专长有关的工作，并用 TeamSay 向组长汇报。当前用户输入：\n"
                            + prompt
                        )
                    _check(
                        await AgentInvite(**deps, invitable_pool=[record])(
                            target=_display_name(record.data.name, record.id),
                            prompt=first_task,
                        )
                    )
                except BaseException:
                    # 仅撤销本次邀请新建的关系，保留请求等待下一次用户输入重试。
                    latest = await storage.get_team(scope.user_id, team.id)
                    latest.data.members = [
                        m for m in latest.data.members if m.agent_id != record.id
                    ]
                    await storage.upsert_team(scope.user_id, latest)
                    for sid in adapter.created_sessions:
                        await storage.delete_session(scope.user_id, record.id, sid)
                        await purge_session_bus(native_create._message_bus, sid)
                    await storage.delete_agent(scope.user_id, record.id)
                    raise
                member.update(
                    status="joined", worker_session_id=adapter.created_sessions[0]
                )
            request.setdefault("delivered", []).append(key)
            await _save(storage, scope, state)
            progress = getattr(
                native_create._message_bus, "collaborator_progress", None
            )
            if progress is not None:
                await progress.dispatched(scope.session_id, member)
        request["completed"] = True
        await _save(storage, scope, state)
