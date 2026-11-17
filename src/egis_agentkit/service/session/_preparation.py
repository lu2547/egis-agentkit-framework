"""在会话锁内组织权限校验、项目准备和固定定义装配。

核心能力
- bind 读取会话及定义，组织用户、项目、模型、个人助理等 Service。
- 执行期间绑定 Core 上下文，结束或取消时恢复调用前状态。
- 不拥有原生锁实现，不负责注册中间件。
"""

import asyncio
from contextlib import asynccontextmanager
from copy import deepcopy
from pathlib import Path

from egis_agentkit.client._tracing import annotate_runtime, trace_phase
from egis_agentkit.core.session._context import (
    SessionExecutionContext,
    session_execution_context,
)
from egis_agentkit.errors import Forbidden, NotFound
from egis_agentkit.service._user import UserService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.agent._personal_manager import PersonalManager
from egis_agentkit.service.project._project_manager import ProjectManager
from egis_agentkit.service.team._collaboration import validate_collaborators
from egis_agentkit.service.team._members import team_member_definitions
from egis_agentkit.storage.project._files import ProjectFileStorage


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
    ):
        self.storage = storage
        self.files = ProjectFileStorage(Path(workspace_root))
        self.config_provider = config_provider
        self.models = models
        self.personal = personal or PersonalManager(
            storage, workspace_root=workspace_root, config=config_provider
        )
        self.users = users if users is not None else UserService(storage)
        self.projects = (
            projects
            if projects is not None
            else ProjectManager(storage, workspace_root)
        )

    async def _prepare(self, session_id):
        """加载身份与固定定义，校验实时授权并装配本次执行上下文。"""
        locator = await self.storage.locate_session(session_id)
        if locator is None:
            raise NotFound("会话不存在")
        user_id, agent_id = locator
        session = await self.storage.read_session_unscoped(
            user_id, agent_id, session_id
        )
        user = await self.users.get(user_id)
        projects = self.projects
        if not session.state.middle_context.get("egis_agentkit", {}).get(
            "is_initialized"
        ):
            raise Forbidden("会话尚未完成初始化")
        project = await projects.session_project(user, session)
        binding = deepcopy(session.state.middle_context["egis_agentkit"])
        configuration = self.config_provider.snapshot if self.config_provider else None
        policies = configuration.access if configuration is not None else None
        permissions = PermissionService(self.storage, policies)
        # 普通回复、工具确认恢复和 Team 成员均使用会话绑定的版本。
        root = await self.storage.definitions.get_definition_by_reference(binding)
        members = await team_member_definitions(self.storage, root)
        definition = members.get(binding["agent_definition_id"])
        if definition is None:
            raise Forbidden("会话绑定的智能体不在固定版本中")
        await permissions.require_execution(user, root, allow_pinned=True)
        invited = await validate_collaborators(permissions, user, binding)
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
            token = session_execution_context.set(scope)
            try:
                yield scope
            finally:
                session_execution_context.reset(token)
