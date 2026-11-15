"""持久化项目文件指纹、变更流水和回复产物关系。"""

from contextlib import asynccontextmanager

from sqlalchemy import and_, delete, func, insert, literal, select, text, update
from sqlalchemy.dialects.postgresql import insert as pg_insert

from egis_agentkit.client._postgres import PostgresTransaction
from egis_agentkit.storage.model import _tables as t


async def lock_project_files(conn, project_id):
    """串行化同一项目的扫描、交付、文件修改和清理。"""
    await conn.execute(
        text("SELECT pg_advisory_xact_lock(hashtextextended(:pid, 0))"),
        {"pid": project_id},
    )


class ProjectFileRecordStorage:
    """保存文件扫描的事务内状态以及可分页的变更历史。"""

    def __init__(self, engine):
        self.engine = engine

    @asynccontextmanager
    async def locked(self, project_ids):
        """按固定顺序获取文件锁，并提供复用事务的存储对象。"""
        async with self.engine.begin() as conn:
            for project_id in sorted(set(project_ids)):
                await lock_project_files(conn, project_id)
            yield ProjectFileRecordStorage(PostgresTransaction(conn))

    async def deleted_artifacts(self, project_id):
        """取得每个已删除产物最近的变更时间。"""
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(
                    select(
                        t.file_changes.c.path, func.max(t.file_changes.c.observed_at)
                    )
                    .where(
                        t.file_changes.c.project_id == project_id,
                        t.file_changes.c.area == "artifacts",
                        t.file_changes.c.change_type == "deleted",
                    )
                    .group_by(t.file_changes.c.path)
                )
            ).all()
        return {
            path: int(observed.timestamp() * 1_000_000_000) for path, observed in rows
        }

    async def collect_reply_artifacts(
        self, project_id: str, session_id: str, reply_id: str
    ):
        """一条 INSERT SELECT 汇总本轮仍存在的新增、修改产物，覆盖更新按路径去重。"""
        # 1. 只读本会话本回复的最新变更；最后一次是删除的路径不参与交付。
        changes, fingerprints = t.file_changes, t.file_fingerprints
        latest = (
            select(
                changes.c.path,
                changes.c.change_id,
                changes.c.change_type,
                func.row_number()
                .over(partition_by=changes.c.path, order_by=changes.c.change_id.desc())
                .label("position"),
            )
            .where(
                changes.c.project_id == project_id,
                changes.c.session_id == session_id,
                changes.c.reply_id == reply_id,
                changes.c.area == "artifacts",
            )
            .subquery()
        )
        # 2. 最后收集前若已被另一会话、另一回复或人工覆盖，不领取别人的当前版本。
        newer = changes.alias("newer_change")
        replaced = (
            select(newer.c.change_id)
            .where(
                newer.c.project_id == project_id,
                newer.c.area == "artifacts",
                newer.c.path == latest.c.path,
                newer.c.change_id > latest.c.change_id,
            )
            .exists()
        )
        # 3. 关联当前指纹以排除已不存在的文件，幂等保存回复与产物的关系。
        source = (
            select(
                literal(project_id),
                literal(session_id),
                literal(reply_id),
                latest.c.path,
                fingerprints.c.digest,
            )
            .select_from(
                latest.join(
                    fingerprints,
                    and_(
                        fingerprints.c.project_id == project_id,
                        fingerprints.c.area == "artifacts",
                        fingerprints.c.path == latest.c.path,
                    ),
                )
            )
            .where(
                latest.c.position == 1,
                ~replaced,
                latest.c.change_type.in_(("created", "modified")),
            )
        )
        stmt = pg_insert(t.reply_artifacts).from_select(
            ["project_id", "session_id", "reply_id", "path", "digest"], source
        )
        async with self.engine.connect() as conn:
            await conn.execute(
                stmt.on_conflict_do_update(
                    index_elements=list(t.reply_artifacts.primary_key.columns),
                    set_={"digest": stmt.excluded.digest},
                )
            )

    async def reply_artifacts(self, project_id, session_id):
        """按项目和会话读取已记录的产物关系。"""
        async with self.engine.connect() as conn:
            return (
                (
                    await conn.execute(
                        select(
                            t.reply_artifacts.c.path, t.reply_artifacts.c.reply_id
                        ).where(
                            t.reply_artifacts.c.project_id == project_id,
                            t.reply_artifacts.c.session_id == session_id,
                        )
                    )
                )
                .mappings()
                .all()
            )

    @staticmethod
    def _active_project(user_id, project_id):
        return (
            t.projects.c.id == project_id,
            t.projects.c.owner_id == user_id,
            t.projects.c.status_type == "active",
        )

    async def baseline_exists(self, user_id, project_id):
        """区分未建立基线、已有基线和不可再扫描的项目。"""
        async with self.engine.connect() as conn:
            return await conn.scalar(
                select(t.projects.c.scan_has_baseline).where(
                    *self._active_project(user_id, project_id)
                )
            )

    async def fingerprints(self, project_id):
        """读取当前项目的文件指纹。"""
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        select(t.file_fingerprints).where(
                            t.file_fingerprints.c.project_id == project_id
                        )
                    )
                )
                .mappings()
                .all()
            )
        return {
            (row["area"], row["path"]): (row["digest"], row["size"]) for row in rows
        }

    async def save_observation(self, user_id, project_id, current, changes):
        """同一事务内追加历史、替换指纹并清除扫描错误。"""
        async with self.engine.connect() as conn:
            if changes:
                await conn.execute(insert(t.file_changes), changes)
            await conn.execute(
                delete(t.file_fingerprints).where(
                    t.file_fingerprints.c.project_id == project_id
                )
            )
            await conn.execute(
                update(t.projects)
                .where(*self._active_project(user_id, project_id))
                .values(scan_has_baseline=True, scan_error=None, scan_error_at=None)
            )
            if current:
                await conn.execute(
                    insert(t.file_fingerprints),
                    [
                        dict(project_id=project_id, area=a, path=p, digest=d, size=s)
                        for (a, p), (d, s) in current.items()
                    ],
                )

    async def record_scan_error(self, user_id, project_id, message):
        """保存仍有效的项目最近一次扫描错误。"""
        async with self.engine.connect() as conn:
            await conn.execute(
                update(t.projects)
                .where(*self._active_project(user_id, project_id))
                .values(scan_error=message, scan_error_at=func.now())
            )

    async def list_changes(self, project_id, cursor, limit):
        """分页读取变更流水和当前扫描错误。"""
        query = select(t.file_changes).where(t.file_changes.c.project_id == project_id)
        if cursor is not None:
            query = query.where(t.file_changes.c.change_id < cursor)
        async with self.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        query.order_by(t.file_changes.c.change_id.desc()).limit(
                            limit + 1
                        )
                    )
                )
                .mappings()
                .all()
            )
            error = (
                (
                    await conn.execute(
                        select(
                            t.projects.c.id.label("project_id"),
                            t.projects.c.scan_error.label("message"),
                            t.projects.c.scan_error_at.label("observed_at"),
                        ).where(
                            t.projects.c.id == project_id,
                            t.projects.c.scan_error.is_not(None),
                        )
                    )
                )
                .mappings()
                .first()
            )
        return rows, dict(error) if error else None

    async def delete_session_artifacts(self, project_id, session_ids):
        """在会话清理事务内只移除所属项目的回复产物关系。"""
        async with self.engine.connect() as conn:
            await conn.execute(
                delete(t.reply_artifacts).where(
                    t.reply_artifacts.c.project_id == project_id,
                    t.reply_artifacts.c.session_id.in_(session_ids),
                )
            )

    async def delete_project_records(self, project_id):
        """在项目清理事务中删除基线和变更历史。"""
        async with self.engine.connect() as conn:
            for table in (t.file_changes, t.file_fingerprints):
                await conn.execute(
                    delete(table).where(table.c.project_id == project_id)
                )
