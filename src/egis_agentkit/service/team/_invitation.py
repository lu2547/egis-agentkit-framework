"""回复开始时由工具权限 Middleware 调用，执行已授权的专家邀请。

原生入团、Inbox 投递和本地进度分别提交，恢复时重投尚未确认的任务。"""

import asyncio

from agentscope.app._tool._agent_invite import AgentInvite, _display_name
from agentscope.app._tool._team_create import TeamCreate
from agentscope.app._tool._team_say import TeamSay
from agentscope.message import Msg, ToolResultState

from egis_agentkit.core.agent._definition_base import AgentDefinitionBase
from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.project._project_manager import purge_session_bus
from egis_agentkit.service.team._collaboration import (
    collaboration_allowed,
    collaborators,
    validate_collaborators,
)
from egis_agentkit.storage.model import new_id


class InvitationStorage:
    """将原生邀请产生的成员会话绑定到已选择的专家版本。"""

    def __init__(self, storage, scope, state, user, permissions, member=None):
        self.storage, self.scope, self.state = storage, scope, state
        self.user, self.permissions, self.member = user, permissions, member
        self.created_sessions = []
        self.created_teams = []

    def __getattr__(self, name):
        return getattr(self.storage, name)

    async def get_session(self, user_id, agent_id, session_id):
        # 原生工具读取 TeamCreate 在同一回合刚写入的最新成员关系。
        if not agent_id:
            agent_id = (
                self.scope.agent_id if session_id == self.scope.session_id else agent_id
            )
        return await self.storage.read_session_unscoped(user_id, agent_id, session_id)

    async def get_agent(self, user_id, agent_id):
        if self.member and agent_id == self.member["native_agent_id"]:
            definition = await self.storage.definitions.get_definition_by_reference(
                self.member
            )
            await self.permissions.require_execution(
                self.user, definition, allow_pinned=True
            )
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
        """校验邀请专家的固定版本并持久化成员会话引用。"""
        member = self.member
        if (
            member is None
            or agent_id != member["native_agent_id"]
            or getattr(kwargs.get("origin"), "type", None) != "team"
        ):
            raise Forbidden("邀请会话不属于已批准专家")
        definition = await self.storage.definitions.get_definition_by_reference(member)
        await self.permissions.require_execution(
            self.user, definition, allow_pinned=True
        )
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
        session = await self.storage.upsert_session(user_id, agent_id, config, **kwargs)
        self.created_sessions.append(session.id)
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
    await storage.update_session_state(
        scope.user_id, scope.agent_id, scope.session_id, state
    )


def _check(result):
    if result.state == ToolResultState.ERROR:
        raise Conflict(
            "\n".join(getattr(block, "text", "") for block in result.content)
        )


async def apply_invitations(user, scope, state, native_create, inputs, permissions):
    """在会话锁内完成原生入团、任务投递和进度提交，再响应取消。"""
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
    """完成待处理邀请，保证重试使用原专家版本和原用户任务。"""
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
    # 只有真实用户输入能消费邀请，Inbox 唤醒和工具确认恢复不会触发。
    if not requests or not messages:
        return
    if not collaboration_allowed(scope.session_record, scope.root_definition):
        raise Forbidden("只有个人智能体主对话可以邀请专家协作")
    definitions = await validate_collaborators(permissions, user, binding)
    scope.collaborator_definitions.update(definitions)
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
    # 先记录原始任务，保证重试继续投递该任务。
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
                if all(m[k] == value for k, value in selected.items())
            )
            root = definitions[member["native_agent_id"]]
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
                # 入团先于 Inbox 投递。缺少投递检查点时重新发送原任务。
                _check(
                    await TeamSay(**deps, role="leader")(
                        content=prompt,
                        to=_display_name(root["agent_name"], member["native_agent_id"]),
                    )
                )
                member.update(status="joined", worker_session_id=existing.session_id)
            else:
                record = AgentDefinitionBase.build_agent(
                    scope.user_id,
                    root,
                    source="team",
                    record_id=member["native_agent_id"],
                    invite_description=member["display_name"],
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
        request["completed"] = True
        await _save(storage, scope, state)
