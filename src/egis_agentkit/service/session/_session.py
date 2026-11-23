"""会话生命周期及输入区可选能力。"""

import asyncio
import logging
import time

from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.storage import (
    SessionRecord,
    TeamMember,
)
from agentscope.app.storage._model._session import ScheduleOrigin, TeamOrigin, UserOrigin
from agentscope.app.storage._utils import _resolve_team_leader
from agentscope.event import ReplyEndEvent, ReplyFinishedReason
from agentscope.permission import PermissionMode

from egis_agentkit.client._tracing import mark_runtime_error, trace_phase
from egis_agentkit.client.nacos._settings import (
    SessionBranchConfig,
    SessionListingConfig,
    SessionNamingConfig,
    SessionPermissionsConfig,
    SessionSteeringConfig,
)
from egis_agentkit.core.agent._definition_base import (
    AgentDefinitionBase,
)
from egis_agentkit.core.agent._personal_definition import PersonalDefinition
from egis_agentkit.core.session._context import get_session_execution_context
from egis_agentkit.core.workspace._identity import workspace_id_for
from egis_agentkit.errors import Conflict, Forbidden, InvalidRequest, NotFound
from egis_agentkit.service._reply import build_branch_context
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.service.project._project_manager import (
    ProjectManager,
    purge_session_bus,
)
from egis_agentkit.service.team._collaboration import (
    collaboration_allowed,
    collaborators,
    pending_request_id,
    validate_collaborators,
)
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.storage.model import new_id
from egis_agentkit.storage.model._definition import (
    build_definition_options,
)
from egis_agentkit.storage.project._files import ProjectFileStorage

logger = logging.getLogger(__name__)


class SessionService:
    """创建并维护原生会话，使用固定定义版本获取运行及展示信息。"""

    def rewrite_message(self, user, session_id, agent_id, message_id, bus):
        from ._message_rewrite import MessageRewriteService

        return MessageRewriteService(self).replace(user, session_id, agent_id, message_id, bus)

    def get_steering_settings(self):
        return (
            self.config.snapshot.session.steering
            if self.config
            else SessionSteeringConfig()
        )

    def __init__(
        self,
        storage,
        workspace_root,
        models=None,
        config_provider=None,
        *,
        projects=None,
        personal=None,
        authorizations=None,
    ):
        self.storage = storage
        self.authorizations = authorizations
        self.files = ProjectFileStorage(workspace_root, config_provider=config_provider)
        self.models, self.config = models, config_provider
        self.personal = personal or PersonalManager(
            storage, workspace_root=workspace_root, config=config_provider
        )
        self.projects = (
            projects
            if projects is not None
            else ProjectManager(
                storage, workspace_root, config_provider=config_provider
            )
        )

    def list_permission_modes(self):
        """列出工作台开放的原生权限模式。"""
        return [
            mode
            for mode in self._get_permission_settings().allowed_modes
            if mode != PermissionMode.DONT_ASK
        ]

    def _get_permission_settings(self):
        return (
            self.config.snapshot.session.permissions
            if self.config
            else SessionPermissionsConfig()
        )

    def validate_permission_mode(self, mode, *, permissions=None):
        """只接受配置中开放的工作台模式。"""
        mode = PermissionMode(mode)
        permissions = (
            permissions if permissions is not None else self._get_permission_settings()
        )
        if mode == PermissionMode.DONT_ASK or mode not in permissions.allowed_modes:
            raise Forbidden("此权限模式未开放")
        return mode

    def get_default_permission_mode(self):
        """返回新主会话的配置默认模式。"""
        return self._get_permission_settings().default_mode

    async def add_session(
        self,
        user,
        definition_id,
        view,
        *,
        resource_version=None,
        name=None,
        model_key=None,
        permission_mode=None,
        project_id=None,
        schedule_id=None,
    ):
        """在项目范围内按指定版本创建会话。"""
        configuration = self.config.snapshot if self.config else None
        naming = (
            configuration.session.naming if configuration else SessionNamingConfig()
        )
        if name is not None and len(name) > naming.create_max_length:
            raise InvalidRequest(f"会话名称最多 {naming.create_max_length} 个字符")
        projects = self.projects
        project_record = (
            await projects.get(user, project_id)
            if project_id
            else await projects.default(user)
        )
        async with projects.guard(user, project_record["id"]):
            return await self._add_session(
                user,
                definition_id,
                view,
                resource_version=resource_version,
                name=name,
                model_key=model_key,
                permission_mode=permission_mode,
                project_id=project_record["id"],
                configuration=configuration,
                naming=naming,
                schedule_id=schedule_id,
            )

    async def _add_session(
        self,
        user,
        definition_id,
        view,
        *,
        resource_version,
        name=None,
        model_key=None,
        permission_mode,
        project_id,
        configuration,
        naming,
        branch_history=None,
        branch_source=None,
        schedule_id=None,
    ):
        """解析授权版本，创建主会话以及固定 Team 的成员会话。"""
        permissions_config = (
            configuration.session.permissions
            if configuration
            else SessionPermissionsConfig()
        )
        if schedule_id is not None and permission_mode == PermissionMode.DONT_ASK:
            permission_mode = PermissionMode.DONT_ASK
        else:
            permission_mode = self.validate_permission_mode(
                permissions_config.default_mode
                if permission_mode is None
                else permission_mode,
                permissions=permissions_config,
            )
        source = await self.storage.get_definition(definition_id)
        policies = configuration.access if configuration else None
        permissions = PermissionService(self.storage, policies)
        reference = {
            "root_resource_type": source["definition_type"],
            "definition_id": definition_id,
            "resource_view_type": view,
            "resource_version": resource_version,
            "allow_pinned": False,
        }
        snapshot = await permissions.read_snapshot(user, [reference], loaded=[source])
        user = snapshot.user
        root = await snapshot.definition_view(reference)
        await snapshot.require_execution(root)
        if root["definition_type"] == "agent" and root["agent_type"] == "orchestrator":
            raise Forbidden("编排智能体只能在专家团内使用")
        # 普通会话与分支均持有项目保护；只补齐缺失目录，不覆盖已有文件。
        await asyncio.to_thread(self.files.initialize_project, user.user_id, project_id)
        is_team = root["definition_type"] == "team"
        agents = await snapshot.members(root)
        leader_id = (
            root["payload"]["orchestrator"]["definition_id"] if is_team else root["id"]
        )
        created_agents, created_sessions = [], []
        created_team_id = None
        native_agents, native_sessions = [], []
        published = False

        async def project(definition, *, member=False):
            personal = definition["agent_type"] == "personal"
            builder = PersonalDefinition if personal else AgentDefinitionBase
            preferences = (
                await self.personal.get_preferences(user) if personal else None
            )
            native = builder.build_agent(
                user.user_id,
                definition,
                source="team" if member else "user",
                preferences=preferences,
            )
            native_agents.append(native)
            created_agents.append(native.id)
            sid = new_id()
            model = (
                branch_source.config.chat_model_config
                if branch_source is not None and not member
                else (
                    await self.models.resolve(
                        user,
                        model_key or definition["payload"].get("model_key"),
                        is_override=model_key is not None,
                        snapshot=configuration,
                    )
                    if self.models
                    else None
                )
            )
            title = name or naming.default_name
            session_config, state = builder.build_session(
                definition,
                root=root,
                project_id=project_id,
                workspace_id=workspace_id_for(user.user_id),
                session_id=sid,
                cwd=str(self.files.area_root(user.user_id, project_id, "work")),
                name=title,
                auto_name=name is None and not member,
                model=model,
                selected_model_key=model_key,
                permission_mode=PermissionMode.DEFAULT if member else permission_mode,
            )
            if branch_source is not None and not member:
                session_config = branch_source.config.model_copy(
                    update={
                        "workspace_id": session_config.workspace_id,
                        "cwd": session_config.cwd,
                        "name": title,
                        "naming": session_config.naming,
                    },
                    deep=True,
                )
            session = SessionRecord(
                user_id=user.user_id,
                agent_id=native.id,
                config=session_config,
                id=sid,
                state=state,
                origin=(
                    TeamOrigin() if member else
                    ScheduleOrigin(schedule_id=schedule_id) if schedule_id else UserOrigin()
                ),
            )
            native_sessions.append(session)
            created_sessions.append((native.id, session.id))
            return native, session

        try:
            leader, leader_session = await project(agents[leader_id])
            team = None
            if is_team:
                members = []
                for ref in root["payload"]["members"]:
                    native, session = await project(
                        agents[ref["definition_id"]], member=True
                    )
                    members.append(
                        TeamMember(
                            owner_id=user.user_id,
                            agent_id=native.id,
                            session_id=session.id,
                            role="created",
                        )
                    )
                team = TeamDefinition.make_record(
                    user.user_id,
                    leader_agent_id=leader.id,
                    leader_session_id=leader_session.id,
                    name=root["team_display_name"],
                    description=root["payload"]["description"],
                    members=members,
                )
                created_team_id = team.id
                for session in native_sessions:
                    session.team_id = team.id
            # 所有会话和团队关系就绪后才开放执行。
            for session in reversed(native_sessions):
                sid = session.id
                session.state.middle_context["egis_agentkit"]["is_initialized"] = (
                    branch_history is None
                )
                if branch_history is not None and sid == leader_session.id:
                    source_sid, reply_id, messages = branch_history
                    session.state.context = build_branch_context(
                        messages, branch_source.state.context
                    )
                    session.state.middle_context["egis_agentkit"]["branched_from"] = {
                        "session_id": source_sid,
                        "reply_id": reply_id,
                    }
            await self.storage.sessions.publish_created(
                native_agents, native_sessions, team
            )
            published = True
            if branch_history is not None:
                source_sid, _, messages = branch_history
                await self.storage.sessions.save_branch_history(
                    user.user_id,
                    leader_session,
                    source_sid,
                    messages,
                )
            result = await self._view_loaded(leader_session, snapshot)
            logger.info(
                "session.created",
                extra={
                    "context": {
                        "user_id": user.user_id,
                        "project_id": project_id,
                        "session_id": leader_session.id,
                        "agent_id": leader.id,
                        "definition_id": definition_id,
                        "is_team": is_team,
                    }
                },
            )
            return result
        except BaseException:
            # 创建失败时只清理由本次请求已经发布的记录。
            if not published:
                raise
            if created_team_id is not None:
                await self.storage.delete_team(user.user_id, created_team_id)
            async with self.storage.session_cleanup() as (sessions, workspace):
                await workspace.delete_session_artifacts(
                    project_id, [sid for _, sid in created_sessions]
                )
                await sessions.delete_sessions(
                    user.user_id,
                    [(sid, aid) for aid, sid in reversed(created_sessions)],
                    created_agents,
                )
            raise

    async def add_session_branch(self, user, session_id, reply_id, bus):
        """从已完成回复建立独立主会话，不复制后续历史及临时执行状态。"""
        session = await self.get_session_record(user, session_id)
        if session.origin.type != "user":
            raise Forbidden("只能从主对话创建分支")
        binding = session.state.middle_context["egis_agentkit"]
        async with self.projects.guard(user, binding["project_id"]):
            key = MessageBusKeys.session_lock(session_id)
            if not await bus.try_lock(key):
                raise Conflict("对话正在运行，请先停止生成后再创建分支")
            try:
                session = await self.get_session_record(user, session_id)
                binding = session.state.middle_context["egis_agentkit"]
                configuration = self.config.snapshot if self.config else None
                branch = (
                    configuration.session.branch
                    if configuration
                    else SessionBranchConfig()
                )
                naming = (
                    configuration.session.naming
                    if configuration
                    else SessionNamingConfig()
                )
                messages = await self.storage.sessions.list_messages_through(
                    session_id, reply_id, limit=branch.max_messages
                )
                if len(messages) > branch.max_messages:
                    raise InvalidRequest(
                        f"分支历史超过 {branch.max_messages} 条，请选择较早的回复"
                    )
                if not messages:
                    raise NotFound("回复不存在或不可访问")
                reply = messages[-1]
                if reply.role != "assistant" or not reply.finished_at:
                    raise InvalidRequest("请选择已经完成的助手回复")
                return await self._add_session(
                    user,
                    binding["definition_id"],
                    binding["resource_view_type"],
                    resource_version=binding["resource_version"],
                    project_id=binding["project_id"],
                    name=(
                        session.config.name[
                            : max(0, naming.create_max_length - len(branch.name_suffix))
                        ]
                        + branch.name_suffix
                    )[: naming.create_max_length],
                    model_key=binding.get("selected_model_key"),
                    permission_mode=session.state.permission_context.mode,
                    configuration=configuration,
                    naming=naming,
                    branch_history=(session_id, reply_id, messages),
                    branch_source=session,
                )
            finally:
                await bus.unlock(key)

    async def _read_session(
        self,
        user,
        session_id,
        *,
        agent_id=None,
        require_use=True,
        check_collaborators=True,
    ):
        """一个读取边界内共享当前身份、所属项目和固定定义；锁内调用仍重新读取。"""
        from egis_agentkit.service._user import UserService

        user = await UserService(self.storage).get(user.user_id)
        session, project = await self.storage.sessions.read_with_project(
            session_id, user_id=user.user_id, agent_id=agent_id
        )
        if session is None:
            raise Forbidden("会话不存在或不可访问")
        binding = session.state.middle_context.get("egis_agentkit", {})
        if not binding.get("is_initialized"):
            raise Forbidden("会话尚未初始化")
        self.projects.project.require_available(project)
        snapshot = None
        if require_use:
            policies = self.config.snapshot.access if self.config else None
            permissions = PermissionService(self.storage, policies)
            references = [
                binding,
                *(collaborators(binding) if check_collaborators else []),
            ]
            snapshot = await permissions.read_snapshot(
                user, references, refresh_user=False
            )
            await snapshot.require(
                binding["root_resource_type"],
                binding["definition_id"],
                binding["resource_view_type"],
                **build_definition_options(binding),
            )
            if check_collaborators:
                await validate_collaborators(
                    permissions, user, binding, snapshot=snapshot
                )
        return user, session, snapshot

    async def get_session_record(
        self,
        user,
        session_id,
        *,
        agent_id=None,
        require_use=True,
        check_collaborators=True,
    ):
        """定位用户会话，并按需检查固定根和已选专家的当前权限。"""
        _, session, _ = await self._read_session(
            user,
            session_id,
            agent_id=agent_id,
            require_use=require_use,
            check_collaborators=check_collaborators,
        )
        return session

    async def _view_loaded(self, session, snapshot):
        binding = session.state.middle_context["egis_agentkit"]
        root = await snapshot.definition_view(binding)
        agents = {root["id"]: root}
        if root["definition_type"] == "team":
            agents = {
                ref["definition_id"]: await snapshot.definition_view(
                    ref, dependency=True
                )
                for ref in (
                    root["payload"]["orchestrator"],
                    *root["payload"]["members"],
                )
            }
        result = self._session_view(session, root, agents)
        if session.origin.type == "schedule":
            completed = await self.storage.schedules.latest_completed_run_ids(
                session.user_id,
                {session.id: (session.origin.schedule_id, binding["project_id"])},
            )
            result["latest_schedule_run_id"] = completed.get(session.id)
        return result

    async def get_session(self, user, session_id):
        """从固定版本读取对话展示信息，保留可清除的待邀请选项。"""
        _, session, snapshot = await self._read_session(
            user, session_id, check_collaborators=False
        )
        return await self._view_loaded(session, snapshot)

    @staticmethod
    def _session_view(session, root, agents):
        binding = session.state.middle_context["egis_agentkit"]
        return {
            "project_id": binding["project_id"],
            "collaboration_allowed": collaboration_allowed(session, root),
            "pending_collaboration_request_id": pending_request_id(binding),
            "collaborators": [
                {
                    "definition_id": member["definition_id"],
                    "resource_view_type": member["resource_view_type"],
                    "resource_version": member["resource_version"],
                    "display_name": member["display_name"],
                    "status": member["status"],
                    "avatar": member.get("avatar"),
                    **(
                        {"worker_session_id": member["worker_session_id"]}
                        if member.get("worker_session_id")
                        else {}
                    ),
                }
                for member in collaborators(binding)
            ],
            "session_id": session.id,
            "source": session.origin.type,
            "latest_schedule_run_id": None,
            "agent_id": session.agent_id,
            "permission_mode": session.state.permission_context.mode.value,
            "model_key": binding.get("selected_model_key"),
            "is_waiting": session.state.has_awaiting_tool_calls(
                agents[binding["agent_definition_id"]]["agent_name"]
            ),
            "plan": [
                {"id": task.id, "subject": task.subject, "state": task.state}
                for task in session.state.tasks_context.tasks
            ],
            "name": session.config.name,
            "definition_id": binding["definition_id"],
            "definition_display_name": root.get("team_display_name")
            or root.get("agent_display_name"),
            "agent_display_names": {
                agent["agent_name"]: agent.get("agent_display_name") or "养老险GPT"
                for agent in agents.values()
            },
            "updated_at": session.updated_at,
            "resource_view_type": binding["resource_view_type"],
            "resource_version": binding["resource_version"],
        }

    async def list_sessions(self, user, bus=None):
        """读取当前用户可访问的主会话及运行状态。"""
        listing = (
            self.config.snapshot.session.listing
            if self.config
            else SessionListingConfig()
        )
        from ._listing import list_sessions

        return await list_sessions(self, user, bus, limit=listing.max_sessions)

    async def delete_session(self, user, session_id, bus):
        """在项目和会话锁内删除主会话及其成员记录。"""
        session = await self.get_session_record(user, session_id, require_use=False)
        if session.origin.type != "user":
            raise Forbidden("只能删除主对话")
        project_id = session.state.middle_context["egis_agentkit"]["project_id"]
        async with self.projects.guard(user, project_id):
            await self._delete_session(user, session_id, project_id, bus)

    async def _delete_session(self, user, session_id, project_id, bus):
        """锁住主会话后读取最终成员名单，清理失败时保留可重试记录。"""
        leader_key = MessageBusKeys.session_lock(session_id)
        if not await bus.try_lock(leader_key):
            raise Conflict("对话正在运行，请先停止生成后再删除")
        held = [leader_key]
        try:
            session = await self.get_session_record(user, session_id, require_use=False)
            member_ids = set()
            binding = session.state.middle_context["egis_agentkit"]
            all_members = collaborators(binding, include_removed=True)
            if session.team_id:
                team = await self.storage.get_team(user.user_id, session.team_id)
                if team:
                    member_ids = {member.session_id for member in team.data.members}
                    member_ids.discard(session_id)
            member_ids.update(
                member["worker_session_id"]
                for member in all_members
                if member.get("worker_session_id")
            )
            for sid in sorted(member_ids):
                key = MessageBusKeys.session_lock(sid)
                if not await bus.try_lock(key):
                    raise Conflict("对话正在运行，请先停止生成后再删除")
                held.append(key)
            session_ids = [session_id, *sorted(member_ids)]
            projections = [member["native_agent_id"] for member in all_members]
            async with self.storage.session_cleanup() as (sessions, workspace):
                await sessions.delete_sessions(
                    user.user_id, [(session.id, session.agent_id)], projections
                )
                await workspace.delete_session_artifacts(project_id, session_ids)
                await purge_session_bus(bus, session_ids)
        finally:
            for key in reversed(held):
                await bus.unlock(key)

    async def update_session_name(self, user, session_id, bus, name):
        from agentscope.app.message_bus import MessageBusKeys
        from agentscope.app.storage import SessionNaming

        naming = (
            self.config.snapshot.session.naming
            if self.config
            else SessionNamingConfig()
        )
        name = name.strip()
        if not name:
            raise Conflict("会话名称不能为空")
        if len(name) > naming.max_length:
            raise InvalidRequest(f"会话名称最多 {naming.max_length} 个字符")
        await self.get_session_record(user, session_id, require_use=False)
        key = MessageBusKeys.session_lock(session_id)
        if not await bus.try_lock(key):
            raise Conflict("对话正在运行，请先停止生成后再重命名")
        try:
            user, session, snapshot = await self._read_session(user, session_id)
            if session.origin.type != "user":
                raise Forbidden("只能重命名主对话")
            session.config.name = name
            session.config.naming = SessionNaming(auto=False)
            await self.storage.sessions.save_state(session)
            result = await self._view_loaded(session, snapshot)
        finally:
            await bus.unlock(key)
        return result

    async def update_session_settings(
        self, user, session_id, bus, *, permission_mode=None, model_key=None
    ):
        from agentscope.app.message_bus import MessageBusKeys

        await self.get_session_record(user, session_id, require_use=False)
        key = MessageBusKeys.session_lock(session_id)
        if not await bus.try_lock(key):
            raise Conflict("对话正在运行，请先停止生成后再修改设置")
        try:
            user, session, snapshot = await self._read_session(user, session_id)
            if session.origin.type != "user":
                raise Forbidden("只能修改主对话")
            if permission_mode is not None:
                session.state.permission_context.mode = self.validate_permission_mode(
                    permission_mode
                )
            if model_key is not None:
                if self.models is None:
                    raise Forbidden("模型配置不可用")
                session.config.chat_model_config = await self.models.resolve(
                    user, model_key, is_override=True
                )
                session.state.middle_context["egis_agentkit"]["selected_model_key"] = (
                    model_key
                )
            await self.storage.sessions.save_state(session)
            result = await self._view_loaded(session, snapshot)
        finally:
            await bus.unlock(key)
        return result

    async def get_team_leader_name(self, scope):
        """为远程专家取得原生团队组长，独立会话及组长自身不需要汇报目标。"""
        if scope.session_record.team_id is None:
            return None
        team = await self.storage.get_team(scope.user_id, scope.session_record.team_id)
        if team is None or team.session_id == scope.session_id:
            return None
        leader = await _resolve_team_leader(self.storage, scope.user_id, team)
        if leader is None:
            raise Forbidden("未找到远程专家的组长会话")
        return leader.name

    async def build_tools(
        self, user_id, agent_id, session_id, *, resources=None, users=None
    ):
        """根据本轮已绑定身份解析平台工具，授权及部署规则由资源管理能力执行。"""
        scope = get_session_execution_context(user_id, agent_id, session_id)
        if resources is None or resources.tool_registry is None:
            return []
        user = await users.get(user_id)
        with trace_phase("tools.prepare"):
            return await resources.tool_registry.resolve(user, scope)

    async def on_workspace_reply(
        self, manager, workspace, handler, agent, input_kwargs, next_handler
    ):
        """在整个回复流期间持有沙箱租约，项目产物仍由原回复回调处理。"""
        started = time.monotonic()
        context = {
            "user_id": workspace.egis_user_id,
            "project_id": workspace.egis_project_id,
            "session_id": workspace.egis_session_id,
            "agent_id": workspace.egis_agent_id,
        }
        logger.info(
            "reply.started",
            extra={
                "component": "SessionService.on_workspace_reply",
                "context": context,
            },
        )
        reason = "interrupted"
        try:
            async with manager.renew_workspace(workspace):
                async for event in handler(agent, input_kwargs, next_handler):
                    if isinstance(event, ReplyEndEvent):
                        reason = ReplyFinishedReason(event.finished_reason).value
                        if reason == ReplyFinishedReason.ERROR.value:
                            mark_runtime_error("ReplyError")
                    yield event
        except Exception as exc:
            reason = "error"
            mark_runtime_error(type(exc).__name__)
            logger.error(
                "reply.failed",
                extra={
                    "component": "SessionService.on_workspace_reply",
                    "context": {
                        **context,
                        "error_type": type(exc).__name__,
                    },
                },
            )
            raise
        finally:
            logger.info(
                "reply.finished",
                extra={
                    "component": "SessionService.on_workspace_reply",
                    "context": {
                        **context,
                        "reason": reason,
                        "duration_ms": round((time.monotonic() - started) * 1000),
                    },
                },
            )
