"""Create native records; native services execute and persist all replies."""

from copy import deepcopy
from pathlib import Path

from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.storage import (
    AgentData,
    AgentRecord,
    SessionConfig,
    TeamData,
    TeamMember,
    TeamRecord,
)
from agentscope.app.storage._model._session import TeamOrigin
from agentscope.permission import PermissionMode
from agentscope.state import AgentState
from sqlalchemy import delete, insert, text

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.models import new_id
from egis_agentkit.service.access.authorization import PermissionService
from egis_agentkit.service.access.snapshot import SnapshotService
from egis_agentkit.service.project.service import ProjectService, purge_session_bus
from egis_agentkit.service.team.collaboration import (
    CollaborationService,
    collaboration_allowed,
    collaborators,
    pending_request_id,
    validate_collaborators,
)
from egis_agentkit.service.workspace.files import WorkspaceFiles
from egis_agentkit.storage import tables as t


class TaskService:
    def __init__(
        self,
        storage,
        workspace_root,
        models=None,
        config_provider=None,
        *,
        projects=None,
    ):
        self.storage = storage
        self.collaboration = CollaborationService(self)
        self.files = WorkspaceFiles(Path(workspace_root))
        self.models, self.config = models, config_provider
        self.projects = (
            projects
            if projects is not None
            else ProjectService(storage, workspace_root)
        )

    def allowed_permission_modes(self):
        if self.config:
            return [
                mode
                for mode in self.config.snapshot.policies.allowed_permission_modes
                if mode != PermissionMode.DONT_ASK
            ]
        return [
            PermissionMode.DEFAULT,
            PermissionMode.ACCEPT_EDITS,
            PermissionMode.EXPLORE,
            PermissionMode.BYPASS,
        ]

    def require_mode(self, mode):
        mode = PermissionMode(mode)
        if mode not in self.allowed_permission_modes():
            raise Forbidden("此权限模式未开放")
        return mode

    async def create(
        self,
        user,
        definition_id,
        view,
        *,
        name=None,
        model_key=None,
        permission_mode=PermissionMode.DEFAULT,
        project_id=None,
    ):
        projects = self.projects
        project_record = (
            await projects.get(user, project_id)
            if project_id
            else await projects.default(user)
        )
        async with projects.guard(user, project_record["id"]):
            return await self._create(
                user,
                definition_id,
                view,
                name=name,
                model_key=model_key,
                permission_mode=permission_mode,
                project_id=project_record["id"],
            )

    async def _create(
        self,
        user,
        definition_id,
        view,
        *,
        name=None,
        model_key=None,
        permission_mode=PermissionMode.DEFAULT,
        project_id,
    ):
        permission_mode = self.require_mode(permission_mode)
        source = await self.storage.get_definition(definition_id)
        policies = self.config.snapshot.policies if self.config else None
        await PermissionService(self.storage, policies).require(
            user, source["definition_type"], definition_id, view
        )
        snapshot = await SnapshotService(self.storage).capture(definition_id, view)
        root = snapshot["root"]
        if root["definition_type"] == "agent" and root["agent_type"] == "orchestrator":
            raise Forbidden("编排智能体只能在专家团内使用")
        is_team = root["definition_type"] == "team"
        if is_team:
            ref = root["payload"]["orchestrator"]
            leader_key = f"agent:{ref['resource_view_type']}:{ref['definition_id']}"
            # Shared nested roots keep the same selected ref key.
            if leader_key not in snapshot["nodes"]:
                raise Forbidden("团队快照缺少编排智能体")
        else:
            leader_key = snapshot["root_key"]
        created_agents, created_sessions = [], []
        workspace_id = project_id

        async def project(node_key, *, member=False):
            definition = snapshot["nodes"][node_key]["data"]
            native = AgentRecord(
                user_id=user.user_id,
                source="team" if member else "user",
                data=AgentData(
                    name=definition["agent_name"],
                    system_prompt=definition["payload"]["system_prompt"],
                    chat_config={"invite_config": {"invitable": False}},
                ),
            )
            await self.storage.upsert_agent(user.user_id, native)
            created_agents.append(native.id)
            sid = new_id()
            binding = {
                "project_id": project_id,
                "is_initialized": False,
                "definition_id": definition_id,
                "root_resource_type": root["definition_type"],
                "resource_view_type": view,
                "agent_node_key": node_key,
                "snapshot": deepcopy(snapshot),
                "is_team": is_team,
                "selected_model_key": model_key,
                "definition_display_name": root.get("team_display_name")
                or root.get("agent_display_name"),
            }
            model = (
                await self.models.resolve(
                    user,
                    model_key or definition["payload"].get("model_key"),
                    is_override=model_key is not None,
                )
                if self.models
                else None
            )
            title = name or "新对话"
            session = await self.storage.upsert_session(
                user.user_id,
                native.id,
                SessionConfig(
                    workspace_id=workspace_id,
                    name=title,
                    naming={"auto": name is None and not member},
                    cwd=str(self.files.area_root(user.user_id, project_id, "work")),
                    chat_model_config=model,
                ),
                session_id=sid,
                state=AgentState(
                    session_id=sid,
                    middle_context={"egis_agentkit": binding},
                    permission_context={
                        "mode": PermissionMode.DEFAULT if member else permission_mode
                    },
                ),
                origin=TeamOrigin() if member else None,
            )
            created_sessions.append((native.id, session.id))
            async with self.storage.transaction() as conn:
                await conn.execute(
                    insert(t.project_sessions).values(
                        session_id=sid, project_id=project_id
                    )
                )
            return native, session

        try:
            leader, leader_session = await project(leader_key)
            if is_team:
                members = []
                for ref in root["payload"]["members"]:
                    key = f"agent:{ref['resource_view_type']}:{ref['definition_id']}"
                    native, session = await project(key, member=True)
                    members.append(
                        TeamMember(
                            owner_id=user.user_id,
                            agent_id=native.id,
                            session_id=session.id,
                            role="created",
                        )
                    )
                team = TeamRecord(
                    user_id=user.user_id,
                    session_id=leader_session.id,
                    leader_agent_id=leader.id,
                    data=TeamData(
                        name=root["team_display_name"],
                        description=root["payload"]["description"],
                        members=members,
                    ),
                )
                await self.storage.upsert_team(user.user_id, team)
                for _, sid in created_sessions:
                    await self.storage.set_session_team_id(user.user_id, sid, team.id)
            # All relations exist before any entry may execute. No AgentInvite/wakeup.
            for aid, sid in created_sessions:
                session = await self.storage.read_session_unscoped(
                    user.user_id, aid, sid
                )
                session.state.middle_context["egis_agentkit"]["is_initialized"] = True
                await self.storage.update_session_state(
                    user.user_id, aid, sid, session.state
                )
                for area in ("work", "artifacts", "uploads"):
                    self.files.area_root(user.user_id, project_id, area).mkdir(
                        parents=True, exist_ok=True
                    )
            return await self.get(user, leader_session.id)
        except BaseException:
            # Only records allocated by this call are eligible for compensation.
            for aid, sid in reversed(created_sessions):
                await self.storage.delete_session(user.user_id, aid, sid)
                async with self.storage.transaction() as conn:
                    await conn.execute(
                        delete(t.project_sessions).where(
                            t.project_sessions.c.session_id == sid
                        )
                    )
            for aid in reversed(created_agents):
                await self.storage.delete_agent(user.user_id, aid)
            raise

    async def session(
        self, user, session_id, *, agent_id=None, require_use=True,
        check_collaborators=True,
    ):
        from egis_agentkit.service.user.service import UserService

        user = await UserService(self.storage).get(user.user_id)
        async with self.storage.engine.connect() as conn:
            locator = (
                (
                    await conn.execute(
                        text("SELECT user_id, agent_id FROM sessions WHERE id=:id"),
                        {"id": session_id},
                    )
                )
                .mappings()
                .first()
            )
        if (
            locator is None
            or locator["user_id"] != user.user_id
            or agent_id
            and locator["agent_id"] != agent_id
        ):
            raise Forbidden("会话不存在或不可访问")
        session = await self.storage.read_session_unscoped(
            user.user_id, locator["agent_id"], session_id
        )
        binding = session.state.middle_context.get("egis_agentkit", {})
        if not binding.get("is_initialized"):
            raise Forbidden("会话尚未初始化")
        await self.projects.session_project(user, session)
        if require_use:
            policies = self.config.snapshot.policies if self.config else None
            await PermissionService(self.storage, policies).require(
                user,
                binding["root_resource_type"],
                binding["definition_id"],
                binding["resource_view_type"],
            )
            if check_collaborators:
                await validate_collaborators(
                    PermissionService(self.storage, policies), user, binding
                )
        else:
            from egis_agentkit.service.user.service import UserService

            await UserService(self.storage).get(user.user_id)
        return session

    async def get(self, user, session_id):
        # Metadata remains readable so a revoked pending selection can be
        # removed. Execution/session entry points still validate the roster.
        session = await self.session(user, session_id, check_collaborators=False)
        binding = session.state.middle_context["egis_agentkit"]
        return {
            "project_id": binding["project_id"],
            "collaboration_allowed": collaboration_allowed(session, binding),
            "pending_collaboration_request_id": pending_request_id(binding),
            "collaborators": [
                {k: member[k] for k in (
                    "definition_id", "resource_view_type", "display_name", "status"
                )}
                for member in collaborators(binding)
            ],
            "session_id": session.id,
            "agent_id": session.agent_id,
            "permission_mode": session.state.permission_context.mode.value,
            "model_key": binding.get("selected_model_key"),
            "is_waiting": session.state.has_awaiting_tool_calls(
                binding["snapshot"]["nodes"][binding["agent_node_key"]]["data"][
                    "agent_name"
                ]
            ),
            "plan": [
                {"id": task.id, "subject": task.subject, "state": task.state}
                for task in session.state.tasks_context.tasks
            ],
            "name": session.config.name,
            "definition_id": binding["definition_id"],
            "definition_display_name": binding["definition_display_name"],
            "agent_display_names": {
                node["data"]["agent_name"]: node["data"].get("agent_display_name")
                or "养老险GPT"
                for node in binding.get("snapshot", {}).get("nodes", {}).values()
                if node.get("resource_type") == "agent"
            },
            "updated_at": session.updated_at,
            "resource_view_type": binding["resource_view_type"],
        }

    async def list(self, user, bus=None):
        async with self.storage.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT id, agent_id FROM sessions WHERE user_id=:user AND source='user' ORDER BY updated_at DESC LIMIT 200"
                        ),
                        {"user": user.user_id},
                    )
                )
                .mappings()
                .all()
            )
        result = []
        for row in rows:
            try:
                item = await self.get(user, row["id"])
                if bus is not None:
                    from agentscope.app.message_bus import MessageBusKeys

                    item["is_running"] = await bus.is_locked(
                        MessageBusKeys.session_lock(row["id"])
                    )
                result.append(item)
            except (Forbidden, NotFound):
                continue
        return result

    async def delete(self, user, session_id, bus):

        session = await self.session(user, session_id, require_use=False)
        if session.origin.type != "user":
            raise Forbidden("只能删除主对话")
        sessions = [session.id]
        if session.team_id:
            team = await self.storage.get_team(user.user_id, session.team_id)
            if team:
                sessions.extend(member.session_id for member in team.data.members)
        held = []
        project_id = session.state.middle_context["egis_agentkit"]["project_id"]
        async with self.projects.guard(user, project_id):
            await self._delete_session(user, session, sessions, bus, held)

    async def _delete_session(self, user, session, sessions, bus, held):
        try:
            for sid in sorted(set(sessions)):
                key = MessageBusKeys.session_lock(sid)
                if not await bus.try_lock(key):
                    raise Conflict("对话正在运行，请先停止生成后再删除")
                held.append(key)
            await self.storage.delete_session(
                user.user_id, session.agent_id, session.id
            )
            for member in collaborators(session.state.middle_context["egis_agentkit"]):
                await self.storage.delete_agent(user.user_id, member["native_agent_id"])
            for sid in sorted(set(sessions)):
                await purge_session_bus(bus, sid)
            async with self.storage.transaction() as conn:
                await conn.execute(
                    delete(t.reply_artifacts).where(
                        t.reply_artifacts.c.session_id.in_(sessions)
                    )
                )
                await conn.execute(
                    delete(t.project_sessions).where(
                        t.project_sessions.c.session_id.in_(sessions)
                    )
                )
        finally:
            for key in reversed(held):
                await bus.unlock(key)

    async def rename(self, user, session_id, bus, name):
        from agentscope.app.message_bus import MessageBusKeys
        from agentscope.app.storage import SessionNaming

        name = name.strip()
        if not 1 <= len(name) <= 80:
            raise Conflict("名称长度必须为 1 到 80 个字符")
        await self.session(user, session_id)
        key = MessageBusKeys.session_lock(session_id)
        if not await bus.try_lock(key):
            raise Conflict("对话正在运行，请先停止生成后再重命名")
        try:
            session = await self.session(user, session_id)
            if session.origin.type != "user":
                raise Forbidden("只能重命名主对话")
            session.config.name = name
            session.config.naming = SessionNaming(auto=False)
            await self.storage.upsert_session(
                user.user_id,
                session.agent_id,
                session.config,
                session_id=session.id,
                state=session.state,
            )
        finally:
            await bus.unlock(key)
        return await self.get(user, session_id)

    async def update_settings(
        self, user, session_id, bus, *, permission_mode=None, model_key=None
    ):
        from agentscope.app.message_bus import MessageBusKeys

        await self.session(user, session_id)
        key = MessageBusKeys.session_lock(session_id)
        if not await bus.try_lock(key):
            raise Conflict("对话正在运行，请先停止生成后再修改设置")
        try:
            session = await self.session(user, session_id)
            if session.origin.type != "user":
                raise Forbidden("只能修改主对话")
            if permission_mode is not None:
                session.state.permission_context.mode = self.require_mode(
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
            await self.storage.upsert_session(
                user.user_id,
                session.agent_id,
                session.config,
                session_id=session.id,
                state=session.state,
            )
        finally:
            await bus.unlock(key)
        return await self.get(user, session_id)
