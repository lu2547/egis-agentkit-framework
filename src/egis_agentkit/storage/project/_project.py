"""读写项目记录并管理项目生命周期锁。"""

from contextlib import asynccontextmanager
from datetime import datetime, timezone

from sqlalchemy import delete, func, insert, select, text, update

from egis_agentkit.client._postgres import PostgresTransaction
from egis_agentkit.storage._schedule import ScheduleStorage
from egis_agentkit.storage.model import _tables as t

_PROJECT_FIELDS = tuple(
    field for field in t.projects.c if not field.name.startswith("scan_")
)


class ProjectStorage:
    """提供项目持久化以及项目级事务。"""

    def __init__(self, engine):
        self.engine = engine

    @asynccontextmanager
    async def write(self):
        """将项目写入和调用方的初始化步骤纳入同一事务。"""
        async with self.engine.begin() as conn:
            yield ProjectStorage(PostgresTransaction(conn))

    @asynccontextmanager
    async def lock_default(self, user_id):
        """串行创建同一用户的默认项目。"""
        async with self.engine.begin() as conn:
            await conn.execute(
                text("SELECT pg_advisory_xact_lock(hashtextextended(:key, 0))"),
                {"key": "egis-default-project:" + user_id},
            )
            yield ProjectStorage(PostgresTransaction(conn))

    @asynccontextmanager
    async def lifetime(self, project_id, *, exclusive=False):
        """允许会话并行执行，项目删除尝试独占同一把锁。"""
        async with self.engine.begin() as conn:
            params = {"key": "egis-project:" + project_id}
            if exclusive:
                acquired = await conn.scalar(
                    text("SELECT pg_try_advisory_xact_lock(hashtextextended(:key, 0))"),
                    params,
                )
                if not acquired:
                    yield None
                    return
            else:
                await conn.execute(
                    text(
                        "SELECT pg_advisory_xact_lock_shared(hashtextextended(:key, 0))"
                    ),
                    params,
                )
            yield ProjectStorage(PostgresTransaction(conn))

    async def get(self, user_id, project_id):
        """按所有者读取项目。"""
        return await self._get(
            t.projects.c.owner_id == user_id, t.projects.c.id == project_id
        )

    async def get_many(self, user_id, project_ids):
        """按所有者批量读取项目；不把其他用户的项目带入结果。"""
        if not project_ids:
            return {}
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(*_PROJECT_FIELDS).where(
                        t.projects.c.owner_id == user_id,
                        t.projects.c.id.in_(project_ids),
                    )
                )
            ).mappings()
            return {row["id"]: dict(row) for row in rows}

    async def get_default(self, user_id):
        """读取用户的默认项目。"""
        return await self._get(
            t.projects.c.owner_id == user_id, t.projects.c.is_default
        )

    async def _get(self, *conditions):
        async with self.engine.connect() as conn:
            row = (
                (await conn.execute(select(*_PROJECT_FIELDS).where(*conditions)))
                .mappings()
                .first()
            )
        return dict(row) if row is not None else None

    async def create(self, values):
        """在已开启的事务写入项目。"""
        async with self.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        insert(t.projects).values(**values).returning(*_PROJECT_FIELDS)
                    )
                )
                .mappings()
                .one()
            )
        return dict(row)

    async def rename(self, user_id, project_id, name):
        """在生命周期锁内更新所属项目的名称。"""
        async with self.engine.connect() as conn:
            row = await conn.execute(
                update(t.projects)
                .where(t.projects.c.id == project_id, t.projects.c.owner_id == user_id)
                .values(project_name=name, updated_by=user_id, updated_at=func.now())
                .returning(*_PROJECT_FIELDS)
            )
            result = row.mappings().first()
            return dict(result) if result is not None else None

    async def list(self, user_id):
        """按创建时间列出用户的项目。"""
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(*_PROJECT_FIELDS)
                    .where(t.projects.c.owner_id == user_id)
                    .order_by(t.projects.c.created_at)
                )
            ).mappings()
            return [dict(row) for row in rows]

    async def mark_deleting(self, user_id, project_id):
        """提交可供重试识别的删除状态。"""
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(t.projects)
                .where(t.projects.c.id == project_id, t.projects.c.owner_id == user_id)
                .values(status_type="deleting")
            )
            if result.rowcount:
                await ScheduleStorage(PostgresTransaction(conn)).disable_project(
                    user_id, project_id, datetime.now(timezone.utc)
                )

    async def delete(self, user_id, project_id):
        """在清理事务内删除所属项目。"""
        async with self.engine.connect() as conn:
            await conn.execute(
                delete(t.projects).where(
                    t.projects.c.id == project_id, t.projects.c.owner_id == user_id
                )
            )
