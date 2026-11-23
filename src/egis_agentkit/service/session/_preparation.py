"""在会话锁内组织权限校验、项目准备和固定定义装配。

核心能力
- bind 读取会话及定义，组织用户、项目、模型、个人助理等 Service。
- 执行期间绑定 Core 上下文，结束或取消时恢复调用前状态。
- 不拥有原生锁实现，不负责注册中间件。
"""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from dataclasses import replace

from egis_agentkit.client._tracing import annotate_runtime, trace_phase
from egis_agentkit.core.session._context import (
    SessionExecutionContext,
    get_verified_chat_token,
    session_execution_context,
    verified_chat_authorization,
)
from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service._user import UserService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.service.project._project_manager import ProjectManager
from egis_agentkit.service.team._collaboration import (
    collaborators,
    validate_collaborators,
)
from egis_agentkit.storage.project._files import ProjectFileStorage
from egis_agentkit.tool._schedule_tools import require_schedule_tool_confirmation


class SessionPreparationService:
    """在原生会话加锁后装配固定版本并检查实时权限。"""

    def __init__(
        self,
        storage,
        workspace_root,
        config_provider=None,
        *,
        models=None,
        projects=None,
        users=None,
        personal=None,
        authorizations=None,
    ):
        self.storage = storage
        self.authorizations = authorizations
        self.files = ProjectFileStorage(workspace_root, config_provider=config_provider)
        self.config_provider = config_provider
        self.models = models
        self.personal = personal or PersonalManager(
            storage, workspace_root=workspace_root, config=config_provider
        )
        self.users = users if users is not None else UserService(storage)
        self.projects = (
            projects
            if projects is not None
            else ProjectManager(
                storage, workspace_root, config_provider=config_provider
            )
        )

    async def _prepare(self, session_id):
        """加载身份与固定定义，校验实时授权并装配本次执行上下文。"""
        session, project = await self.storage.sessions.read_with_project(session_id)
        if session is None:
            raise NotFound("会话不存在")
        user_id, agent_id = session.user_id, session.agent_id
        user = await self.users.get(user_id)
        projects = self.projects
        if not session.state.middle_context.get("egis_agentkit", {}).get(
            "is_initialized"
        ):
            raise Forbidden("会话尚未完成初始化")
        projects.project.require_available(project)
        binding = deepcopy(session.state.middle_context["egis_agentkit"])
        configuration = self.config_provider.snapshot if self.config_provider else None
        policies = configuration.access if configuration is not None else None
        permissions = PermissionService(self.storage, policies)
        # 普通回复、工具确认恢复和 Team 成员均使用会话绑定的版本。
        snapshot = await permissions.read_snapshot(
            user, [binding, *collaborators(binding)], refresh_user=False
        )
        root = await snapshot.definition_view(binding)
        members = await snapshot.members(root)
        definition = members.get(binding["agent_definition_id"])
        if definition is None:
            raise Forbidden("会话绑定的智能体不在固定版本中")
        await snapshot.require_execution(root, allow_pinned=True)
        await self._inherit_collaboration_mode(session, binding, user_id)
        if session.origin.type == "user" and definition["agent_type"] == "personal":
            require_schedule_tool_confirmation(session.state.permission_context)
        invited = await validate_collaborators(
            permissions, user, binding, snapshot=snapshot
        )
        parked = session.state.has_awaiting_tool_calls(definition["agent_name"])
        if parked and binding.get("chat_model_config"):
            from agentscope.app.storage import ChatModelConfig

            session.config.chat_model_config = ChatModelConfig.model_validate(
                binding["chat_model_config"]
            )
        elif not binding.get("is_team") and self.models:
            session.config.chat_model_config = await self.models.resolve(
                user,
                binding.get("selected_model_key")
                or definition["payload"].get("model_key"),
                is_override=binding.get("selected_model_key") is not None,
                snapshot=configuration,
            )
        if session.config.chat_model_config:
            binding["chat_model_config"] = session.config.chat_model_config.model_dump(
                mode="json"
            )
        session.state.middle_context["egis_agentkit"] = binding
        workdir = self.files.area_root(user_id, project["id"], "work")
        artifacts = self.files.area_root(user_id, project["id"], "artifacts")
        scope = SessionExecutionContext(
            user_id,
            agent_id,
            session_id,
            definition,
            root,
            binding,
            session,
            workdir,
            artifacts,
            project["id"],
            policies=policies,
            collaborator_definitions=invited,
            member_definitions=members,
            configuration=configuration,
            preferences=await self.personal.get_preferences(user)
            if definition["agent_type"] == "personal"
            else None,
        )
        return scope, user

    async def _inherit_collaboration_mode(self, session, binding, user_id):
        """Derive the worker's approval mode from its current parent session."""
        parent_id = binding.get("collaboration_parent_session_id")
        if parent_id is None:
            return
        parent, _ = await self._collaboration_parent(
            session, binding, user_id, parent_id
        )
        session.state.permission_context.mode = parent.state.permission_context.mode

    async def _collaboration_parent(self, session, binding, user_id, parent_id):
        if session.origin.type != "team":
            raise Forbidden("专家协作关系已失效")
        parent, _ = await self.storage.sessions.read_with_project(
            parent_id, user_id=user_id
        )
        if (
            parent is None
            or parent.origin.type != "user"
            or parent.team_id != session.team_id
            or not session.team_id
        ):
            raise Forbidden("专家协作关系已失效")
        host = parent.state.middle_context["egis_agentkit"]
        member = next(
            (
                item
                for item in collaborators(host)
                if item["native_agent_id"] == session.agent_id
            ),
            None,
        )
        if host["project_id"] != binding["project_id"] or member is None:
            raise Forbidden("专家协作关系已失效")
        return parent, member

    async def collaboration_progress_target(self, session_id):
        """会话准备失败时，从数据库核对组长和受邀成员的关系。"""
        session, _ = await self.storage.sessions.read_with_project(session_id)
        if session is None:
            return None
        user_id, agent_id = session.user_id, session.agent_id
        binding = session.state.middle_context.get("egis_agentkit", {})
        parent_id = binding.get("collaboration_parent_session_id")
        if parent_id is None:
            return None
        try:
            _, member = await self._collaboration_parent(
                session, binding, user_id, parent_id
            )
        except Forbidden:
            return None
        return parent_id, agent_id, member["display_name"]

    @asynccontextmanager
    async def bind(self, session_id):
        """分别观察准备与项目目录阶段，整个执行期间保留原有上下文与项目保护。"""
        # 1. 准备阶段只计身份、定义和授权装配，不把整个回复算作准备时间。
        with trace_phase("session.prepare"):
            scope, user = await self._prepare(session_id)
        annotate_runtime(
            attributes={
                "user.id": scope.user_id,
                "agentkit.agent.id": scope.agent_id,
                "agentkit.project.id": scope.project_id,
                "agentkit.agent.type": scope.definition["agent_type"],
                "agentkit.definition.id": scope.definition["id"],
                **(
                    {"agentkit.team.id": scope.session_record.team_id}
                    if scope.session_record.team_id
                    else {}
                ),
            }
        )
        # 2. 复用原项目保护，补齐目录后才开放执行；退出、失败和取消均恢复上下文。
        async with self.projects.guard(user, scope.project_id):
            with trace_phase("project.prepare"):
                await asyncio.to_thread(
                    self.projects.initialize_files, scope.user_id, scope.project_id
                )
            authorization = verified_chat_authorization.get()
            if get_verified_chat_token(scope.user_id, scope.agent_id, scope.session_id) is None:
                authorization = await self._load_authorization(scope)
            token = session_execution_context.set(scope)
            authorization_marker = verified_chat_authorization.set(authorization)
            try:
                yield scope
            finally:
                verified_chat_authorization.reset(authorization_marker)
                session_execution_context.reset(token)

    async def _load_authorization(self, scope):
        if self.authorizations is None:
            return None
        authorization = await self.authorizations.load(
            scope.user_id, scope.agent_id, scope.session_id
        )
        parent_id = scope.binding.get("collaboration_parent_session_id")
        if authorization is None and parent_id:
            parent, _ = await self._collaboration_parent(
                scope.session_record, scope.binding, scope.user_id, parent_id
            )
            authorization = await self.authorizations.load(
                scope.user_id, parent.agent_id, parent_id
            )
            if authorization is not None:
                authorization = replace(
                    authorization, agent_id=scope.agent_id, session_id=scope.session_id
                )
        return authorization
