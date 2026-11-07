"""Projects own files; native sessions own conversation state and execution."""

import asyncio
import os
from contextlib import AsyncExitStack, asynccontextmanager
from pathlib import Path

from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from sqlalchemy import delete, insert, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from egis_agentkit.errors import Conflict, NotFound
from egis_agentkit.models import new_id
from egis_agentkit.service.workspace.changes import FileChangeService
from egis_agentkit.service.workspace.files import WorkspaceFiles
from egis_agentkit.storage import tables as t


async def purge_session_bus(bus, sid):
    await bus.log_trim(MessageBusKeys.session_events(sid))
    await bus.queue_delete(MessageBusKeys.inbox(sid))
    for namespace in (
        MessageBusKeys.inbox_consumer(sid),
        MessageBusKeys.bg_tasks(sid),
        MessageBusKeys.session_event_checkpoint(sid),
        MessageBusKeys.projection_namespace(sid),
    ):
        await bus.registry_drop(namespace)


class ProjectService:
    def __init__(
        self, storage, workspace_root, message_bus=None, *, acquire_session_lock=None
    ):
        self.storage = storage
        self.message_bus = (
            message_bus if message_bus is not None else InMemoryMessageBus()
        )
        self.acquire_session_lock = (
            acquire_session_lock or self.message_bus.acquire_lock
        )
        self.files = WorkspaceFiles(Path(workspace_root))

    async def get(self, user, project_id, *, allow_deleting=False, conn=None):
        query = select(t.projects).where(
            t.projects.c.id == project_id, t.projects.c.owner_id == user.user_id
        )
        if conn is None:
            async with self.storage.engine.connect() as connection:
                row = (await connection.execute(query)).mappings().first()
        else:
            row = (await conn.execute(query)).mappings().first()
        if row is None:
            raise NotFound("项目不存在或不可访问")
        if row["status_type"] != "active" and not allow_deleting:
            raise Conflict("项目正在删除，请稍后重试")
        return dict(row)

    @asynccontextmanager
    async def guard(self, user, project_id):
        # Many sessions may run concurrently. Project deletion needs exclusivity.
        async with self.storage.engine.begin() as conn:
            await conn.execute(
                text("SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"),
                {"key": "egis-project:" + project_id},
            )
            yield await self.get(user, project_id, conn=conn)

    def initialize_files(self, user_id, project_id):
        for area in ("uploads", "work", "artifacts"):
            fd = self.files._directory_fd(
                self.files.area_root(user_id, project_id, area), [], create=True
            )
            os.close(fd)

    async def create(self, user, project_name, *, is_default=False, conn=None):
        name = project_name.strip()
        if not 1 <= len(name) <= 128:
            raise Conflict("项目名称长度必须为 1 到 128 个字符")
        values = dict(
            id=new_id(),
            owner_id=user.user_id,
            project_name=name,
            status_type="active",
            is_default=is_default,
            created_by=user.user_id,
            updated_by=user.user_id,
        )
        if conn is None:
            async with self.storage.transaction() as connection:
                return await self._create(connection, values)
        return await self._create(conn, values)

    async def _create(self, conn, values):
        row = (
            (
                await conn.execute(
                    insert(t.projects).values(**values).returning(t.projects)
                )
            )
            .mappings()
            .one()
        )
        await asyncio.to_thread(self.initialize_files, values["owner_id"], values["id"])
        return dict(row)

    async def default(self, user):
        async with self.storage.transaction() as conn:
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": "egis-default-project:" + user.user_id},
            )
            row = (
                (
                    await conn.execute(
                        select(t.projects).where(
                            t.projects.c.owner_id == user.user_id,
                            t.projects.c.is_default,
                        )
                    )
                )
                .mappings()
                .first()
            )
            if row:
                if row["status_type"] != "active":
                    raise Conflict("默认项目正在删除，请选择其他项目")
                return dict(row)
            return await self.create(user, "默认项目", is_default=True, conn=conn)

    async def rename(self, user, project_id, project_name):
        name = project_name.strip()
        if not 1 <= len(name) <= 128:
            raise Conflict("项目名称长度必须为 1 到 128 个字符")
        async with self.guard(user, project_id):
            async with self.storage.transaction() as conn:
                await conn.execute(
                    update(t.projects)
                    .where(t.projects.c.id == project_id)
                    .values(
                        project_name=name,
                        updated_by=user.user_id,
                        updated_at=text("now()"),
                    )
                )
        return await self.get(user, project_id)

    async def _persist_session_project(self, user, session, project_id):
        binding = dict(session.state.middle_context.get("egis_agentkit", {}))
        binding["project_id"] = project_id
        session.state.middle_context["egis_agentkit"] = binding
        session.config.workspace_id = project_id
        session.config.cwd = str(self.files.area_root(user.user_id, project_id, "work"))
        await self.storage.upsert_session(
            user.user_id,
            session.agent_id,
            session.config,
            session_id=session.id,
            state=session.state,
            origin=session.origin,
        )

    @asynccontextmanager
    async def _session_lock(self, session_id, already_held):
        if already_held:
            yield
            return
        key = MessageBusKeys.session_lock(session_id)
        async with AsyncExitStack() as stack:
            try:
                async with asyncio.timeout(1):
                    await stack.enter_async_context(self.acquire_session_lock(key))
            except TimeoutError:
                raise Conflict("旧会话正在运行或迁移，请稍后重试") from None
            yield

    async def session_project(self, user, session, *, session_lock_held=False):
        binding = session.state.middle_context.get("egis_agentkit", {})
        if binding.get("project_id"):
            return await self.get(user, binding["project_id"])
        async with self._session_lock(session.id, session_lock_held):
            # A caller may have loaded this session before another adopter,
            # completed turn or delete. Re-read under the native execution lock.
            fresh = await self.storage.read_session_unscoped(
                user.user_id, session.agent_id, session.id
            )
            if fresh is None:
                raise NotFound("会话已删除")
            session.state, session.config = fresh.state, fresh.config
            binding = fresh.state.middle_context.get("egis_agentkit", {})
            if binding.get("project_id"):
                return await self.get(user, binding["project_id"])
            return await self._adopt_session(user, session)

    async def _adopt_session(self, user, session):
        # A file/database migration can commit before the native state write.
        # Respect that durable membership when repairing an interrupted adoption.
        async with self.storage.engine.connect() as conn:
            existing = await conn.scalar(
                select(t.project_sessions.c.project_id).where(
                    t.project_sessions.c.session_id == session.id
                )
            )
        project = (
            await self.get(user, existing) if existing else await self.default(user)
        )
        project_id = project["id"]
        async with self.guard(user, project_id):
            async with FileChangeService(self.storage, self.files.root).locked(
                [project_id]
            ) as conn:
                existing = await conn.scalar(
                    select(t.project_sessions.c.project_id).where(
                        t.project_sessions.c.session_id == session.id
                    )
                )
                if existing is None:
                    mapping = await asyncio.to_thread(
                        self.files.migrate_legacy_session,
                        user.user_id,
                        session.id,
                        project_id,
                    )
                    history = (
                        (
                            await conn.execute(
                                select(
                                    t.file_changes.c.change_id,
                                    t.file_changes.c.area,
                                    t.file_changes.c.path,
                                ).where(t.file_changes.c.project_id == session.id)
                            )
                        )
                        .mappings()
                        .all()
                    )
                    for row in history:
                        target = mapping.get((row["area"], row["path"]))
                        if target is not None:
                            await conn.execute(
                                update(t.file_changes)
                                .where(t.file_changes.c.change_id == row["change_id"])
                                .values(area=target[0], path=target[1])
                            )
                    await conn.execute(
                        update(t.file_changes)
                        .where(t.file_changes.c.project_id == session.id)
                        .values(project_id=project_id)
                    )
                    for table in (
                        t.file_baselines,
                        t.file_fingerprints,
                        t.file_scan_errors,
                    ):
                        await conn.execute(
                            delete(table).where(table.c.project_id == session.id)
                        )
                    await conn.execute(
                        pg_insert(t.project_sessions)
                        .values(session_id=session.id, project_id=project_id)
                        .on_conflict_do_nothing()
                    )
                elif existing != project_id:
                    raise Conflict("会话的项目归属已变化，请重试")
            # Native storage owns its transaction. Write it after the membership
            # and migrated history commit, so a retry can always finish safely.
            await self._persist_session_project(user, session, project_id)
        return project

    async def migrate_user(self, user):
        async with self.storage.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        text(
                            "SELECT s.id, s.agent_id FROM sessions s LEFT JOIN ak_project_sessions p ON p.session_id=s.id WHERE s.user_id=:uid AND p.session_id IS NULL ORDER BY s.updated_at DESC"
                        ),
                        {"uid": user.user_id},
                    )
                )
                .mappings()
                .all()
            )
        for row in rows:
            session = await self.storage.read_session_unscoped(
                user.user_id, row["agent_id"], row["id"]
            )
            if session and session.state.middle_context.get("egis_agentkit", {}).get(
                "is_initialized"
            ):
                await self.session_project(user, session)

    async def list(self, user):
        await self.migrate_user(user)
        async with self.storage.engine.connect() as conn:
            return [
                dict(row)
                for row in (
                    await conn.execute(
                        select(t.projects)
                        .where(t.projects.c.owner_id == user.user_id)
                        .order_by(t.projects.c.created_at)
                    )
                ).mappings()
            ]

    async def delete(self, user, project_id, bus):
        await self.get(user, project_id, allow_deleting=True)
        async with self.storage.engine.begin() as guard:
            acquired = await guard.scalar(
                text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": "egis-project:" + project_id},
            )
            if not acquired:
                raise Conflict("项目中有正在执行的操作，请停止后再删除")
            await self.get(user, project_id, allow_deleting=True, conn=guard)
            rows = (
                (
                    await guard.execute(
                        select(t.project_sessions.c.session_id).where(
                            t.project_sessions.c.project_id == project_id
                        )
                    )
                )
                .scalars()
                .all()
            )
            held = []
            try:
                for sid in sorted(rows):
                    key = MessageBusKeys.session_lock(sid)
                    if not await bus.try_lock(key):
                        raise Conflict("项目中有对话正在运行，请先停止生成")
                    held.append(key)
                async with self.storage.transaction() as conn:
                    await conn.execute(
                        update(t.projects)
                        .where(t.projects.c.id == project_id)
                        .values(status_type="deleting")
                    )
                async with FileChangeService(self.storage, self.files.root).locked(
                    [project_id]
                ) as conn:
                    # Borrowed collaboration projections are owned by their
                    # parent journal. Native invited-member cleanup keeps the
                    # underlying agent, so collect these before cascade deletes.
                    from egis_agentkit.service.team.collaboration import collaborators

                    owned_projections = set()
                    for sid in rows:
                        row = (
                            await conn.execute(
                                text("SELECT agent_id FROM sessions WHERE id=:sid AND user_id=:uid"),
                                {"sid": sid, "uid": user.user_id},
                            )
                        ).first()
                        if row:
                            record = await self.storage.read_session_unscoped(
                                user.user_id, row.agent_id, sid
                            )
                            binding = record.state.middle_context.get("egis_agentkit", {})
                            owned_projections.update(
                                member["native_agent_id"] for member in collaborators(binding)
                            )
                    for sid in rows:
                        locator = (
                            await conn.execute(
                                text(
                                    "SELECT agent_id FROM sessions WHERE id=:sid AND user_id=:uid"
                                ),
                                {"sid": sid, "uid": user.user_id},
                            )
                        ).first()
                        if locator:
                            await self.storage.delete_session(
                                user.user_id, locator.agent_id, sid
                            )
                        await purge_session_bus(bus, sid)
                    for agent_id in owned_projections:
                        await self.storage.delete_agent(user.user_id, agent_id)
                    await asyncio.to_thread(
                        self.files.remove_project, user.user_id, project_id
                    )
                    for table in (
                        t.file_changes,
                        t.file_fingerprints,
                        t.file_scan_errors,
                        t.file_baselines,
                    ):
                        await conn.execute(
                            delete(table).where(table.c.project_id == project_id)
                        )
                    await conn.execute(
                        delete(t.projects).where(t.projects.c.id == project_id)
                    )
            finally:
                for key in reversed(held):
                    await bus.unlock(key)
