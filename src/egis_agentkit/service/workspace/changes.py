"""Bounded, no-follow snapshots of a project's actual files."""

import asyncio
import hashlib
import logging
import os
import stat
from contextlib import asynccontextmanager

from sqlalchemy import delete, func, insert, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from egis_agentkit.errors import Conflict, NotFound
from egis_agentkit.service.workspace.files import WorkspaceFiles
from egis_agentkit.storage import tables as t


class FileChangeService:
    def __init__(self, storage, root):
        self.storage = storage
        self.files = WorkspaceFiles(root)

    @asynccontextmanager
    async def locked(self, project_ids):
        """Serialize file mutation, observation and deletion across nodes."""
        async with self.storage.engine.begin() as conn:
            for pid in sorted(set(project_ids)):
                await conn.execute(
                    text("SELECT pg_advisory_xact_lock(hashtextextended(:pid, 0))"),
                    {"pid": pid},
                )
            yield conn

    def scan(self, user_id, project_id):
        result = {}
        count = total = 0

        def walk(fd, area, prefix, depth):
            nonlocal count, total
            if depth > 32:
                raise Conflict("文件目录过深，无法完整记录变更")
            for name in os.listdir(fd):
                count += 1
                if count > 10000:
                    raise Conflict("文件数量超过变更记录扫描限制")
                info = os.stat(name, dir_fd=fd, follow_symlinks=False)
                if stat.S_ISLNK(info.st_mode):
                    continue
                path = f"{prefix}/{name}" if prefix else name
                if len(path.encode("utf-8")) > 2048:
                    raise Conflict("文件路径过长，无法完整记录变更")
                if stat.S_ISDIR(info.st_mode):
                    child = os.open(
                        name, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW, dir_fd=fd
                    )
                    try:
                        if (
                            area == "uploads"
                            and not prefix
                            and name.casefold() in self.files.RESERVED
                        ):
                            walk(
                                child,
                                "work" if name.casefold() == ".work" else "artifacts",
                                "",
                                depth + 1,
                            )
                        else:
                            walk(child, area, path, depth + 1)
                    finally:
                        os.close(child)
                elif stat.S_ISREG(info.st_mode):
                    if area == "uploads" and not prefix and name.casefold() in self.files.RESERVED:
                        continue
                    total += info.st_size
                    if count > 10000 or total > 200 * 1024 * 1024:
                        raise Conflict("文件数量或大小超过变更记录扫描限制")
                    item = os.open(
                        name, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK, dir_fd=fd
                    )
                    try:
                        before = os.fstat(item)
                        if not stat.S_ISREG(before.st_mode):
                            raise Conflict("文件在扫描期间发生变化")
                        digest = hashlib.sha256()
                        size = 0
                        while chunk := os.read(item, 1024 * 1024):
                            size += len(chunk)
                            if (
                                size > before.st_size
                                or total - info.st_size + size > 200 * 1024 * 1024
                            ):
                                raise Conflict("文件在扫描期间发生变化")
                            digest.update(chunk)
                        after = os.fstat(item)
                        if (before.st_size, before.st_mtime_ns, before.st_ino) != (
                            after.st_size,
                            after.st_mtime_ns,
                            after.st_ino,
                        ) or size != before.st_size:
                            raise Conflict("文件在扫描期间发生变化")
                        result[(area, path)] = (digest.hexdigest(), size)
                    finally:
                        os.close(item)

        try:
            fd = self.files._directory_fd(
                self.files.project_root(user_id, project_id), []
            )
        except NotFound:
            return result
        try:
            walk(fd, "uploads", "", 0)
        finally:
            os.close(fd)
        return result

    async def _deleted_artifacts(self, conn, project_id):
        rows = (await conn.execute(
            select(t.file_changes.c.path, func.max(t.file_changes.c.observed_at))
            .where(t.file_changes.c.project_id == project_id,
                   t.file_changes.c.area == "artifacts",
                   t.file_changes.c.change_type == "deleted")
            .group_by(t.file_changes.c.path)
        )).all()
        return {path: int(observed.timestamp() * 1_000_000_000) for path, observed in rows}

    async def reconcile_deliveries(self, user_id, project_id):
        """Retry late legacy outputs only for durable, verified session members."""
        async with self.locked([project_id]) as conn:
            sessions = (await conn.execute(
                text("SELECT p.session_id FROM ak_project_sessions p "
                     "JOIN sessions s ON s.id=p.session_id "
                     "JOIN ak_projects a ON a.id=p.project_id "
                     "WHERE p.project_id=:pid AND s.user_id=:uid AND a.owner_id=:uid"),
                {"pid": project_id, "uid": user_id},
            )).scalars().all()
            for sid in sessions:
                await asyncio.to_thread(
                    self.files.migrate_legacy_session, user_id, sid, project_id,
                    areas=("artifacts",),
                )

    @asynccontextmanager
    async def track_deliveries(self, user_id, project_id, session_id, reply_id):
        """Serialize one native file-writing tool so shared replies cannot claim each other.

        Only work files changed by this batch are auto-delivered. Pre-existing
        work and artifacts never become this reply's cards merely by being read.
        """
        failure = None
        async with self.locked([project_id]) as conn:
            try:
                before = await asyncio.to_thread(self.scan, user_id, project_id)
            except Exception:
                logging.getLogger(__name__).exception("Could not snapshot reply deliveries")
                before = None
            try:
                yield
            except BaseException as exc:
                # Commit observed deliveries before preserving the native tool's
                # failure/cancellation; file bytes already exist on disk.
                failure = exc
            finally:
                if before is not None:
                    try:
                        # A stale history/tool path may recreate this verified session's
                        # old output tree even after its first project migration.
                        await asyncio.to_thread(
                            self.files.migrate_legacy_session, user_id, session_id, project_id,
                            areas=("artifacts",),
                        )
                        after = await asyncio.to_thread(self.scan, user_id, project_id)
                        changed_work = {
                            path for (area, path), fingerprint in after.items()
                            if area == "work" and before.get((area, path)) != fingerprint
                        }
                        await asyncio.to_thread(
                            self.files.discover_deliveries, user_id, project_id,
                            eligible_paths=changed_work,
                            deleted_after=await self._deleted_artifacts(conn, project_id),
                        )
                        current = await asyncio.to_thread(self.scan, user_id, project_id)
                        rows = [
                            dict(project_id=project_id, session_id=session_id,
                                 reply_id=reply_id, path=path, digest=fingerprint[0])
                            for (area, path), fingerprint in current.items()
                            if area == "artifacts" and before.get((area, path)) != fingerprint
                        ]
                        if rows and reply_id:
                            stmt = pg_insert(t.reply_artifacts).values(rows)
                            await conn.execute(stmt.on_conflict_do_update(
                                index_elements=[t.reply_artifacts.c.project_id,
                                                t.reply_artifacts.c.session_id,
                                                t.reply_artifacts.c.reply_id,
                                                t.reply_artifacts.c.path],
                                set_={"digest": stmt.excluded.digest},
                            ))
                    except Exception:
                        logging.getLogger(__name__).exception("Could not record reply deliveries")
        if failure is not None:
            raise failure

    async def reply_artifacts(self, user_id, project_id, session_id):
        async with self.storage.engine.connect() as conn:
            rows = (await conn.execute(select(t.reply_artifacts).where(
                t.reply_artifacts.c.project_id == project_id,
                t.reply_artifacts.c.session_id == session_id,
            ))).mappings().all()
        files = {item["path"]: item for item in await asyncio.to_thread(
            self.files.list, user_id, project_id, "artifacts", recursive=True,
        ) if not item["is_directory"]}
        return [dict(files[row["path"]], area="artifacts", reply_id=row["reply_id"])
                for row in rows if row["path"] in files]

    async def _observable(self, conn, user_id, project_id):
        status = await conn.scalar(
            text("SELECT status_type FROM ak_projects WHERE id=:pid AND owner_id=:uid"),
            {"pid": project_id, "uid": user_id},
        )
        return status == "active" or (
            status is None and self.files.project_root(user_id, project_id).exists()
        )

    async def observe(self, user_id, project_id, *, discover=False):
        # Serialize both scanning and committing, so overlapping observations
        # cannot apply snapshots out of order.
        async with self.locked([project_id]) as conn:
            # A request authorized before deletion may arrive at this lock later.
            # Missing roots plus a missing project must remain deleted.
            if not await self._observable(conn, user_id, project_id):
                return
            if discover:
                await asyncio.to_thread(
                    self.files.discover_deliveries, user_id, project_id,
                    deleted_after=await self._deleted_artifacts(conn, project_id),
                )
            current = await asyncio.to_thread(self.scan, user_id, project_id)
            baseline = (
                await conn.execute(
                    select(t.file_baselines).where(
                        t.file_baselines.c.project_id == project_id
                    )
                )
            ).first()
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
            previous = {(r["area"], r["path"]): (r["digest"], r["size"]) for r in rows}
            if baseline:
                changes = []
                for key in sorted(current.keys() | previous.keys()):
                    old, new = previous.get(key), current.get(key)
                    if old == new:
                        continue
                    changes.append(
                        dict(
                            project_id=project_id,
                            area=key[0],
                            path=key[1],
                            change_type="created"
                            if old is None
                            else "deleted"
                            if new is None
                            else "modified",
                            size=new[1] if new else None,
                            previous_size=old[1] if old else None,
                        )
                    )
                if changes:
                    await conn.execute(insert(t.file_changes), changes)
            else:
                await conn.execute(
                    insert(t.file_baselines).values(
                        project_id=project_id, user_id=user_id
                    )
                )
            await conn.execute(
                delete(t.file_fingerprints).where(
                    t.file_fingerprints.c.project_id == project_id
                )
            )
            await conn.execute(
                delete(t.file_scan_errors).where(
                    t.file_scan_errors.c.project_id == project_id
                )
            )
            if current:
                await conn.execute(
                    insert(t.file_fingerprints),
                    [
                        dict(project_id=project_id, area=a, path=p, digest=d, size=s)
                        for (a, p), (d, s) in current.items()
                    ],
                )

    async def observe_safe(self, user_id, project_id, *, discover=False):
        try:
            await self.observe(user_id, project_id, discover=discover)
        except Exception as exc:
            logging.getLogger(__name__).warning(
                "File change scan failed for project %s", project_id, exc_info=True
            )
            message = (
                str(exc) if isinstance(exc, Conflict) else "文件扫描失败，请稍后重试"
            )
            try:
                async with self.locked([project_id]) as conn:
                    if not await self._observable(conn, user_id, project_id):
                        return
                    stmt = pg_insert(t.file_scan_errors).values(
                        project_id=project_id, message=message
                    )
                    await conn.execute(
                        stmt.on_conflict_do_update(
                            index_elements=[t.file_scan_errors.c.project_id],
                            set_={"message": message, "observed_at": text("now()")},
                        )
                    )
            except Exception:
                logging.getLogger(__name__).exception(
                    "Could not persist file scan status"
                )

    async def list(self, project_id, cursor=None, limit=50):
        query = select(t.file_changes).where(t.file_changes.c.project_id == project_id)
        if cursor is not None:
            query = query.where(t.file_changes.c.change_id < cursor)
        async with self.storage.engine.connect() as conn:
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
                        select(t.file_scan_errors).where(
                            t.file_scan_errors.c.project_id == project_id
                        )
                    )
                )
                .mappings()
                .first()
            )
        items = [dict(row) for row in rows[:limit]]
        for item in items:
            item.pop("project_id")
        return {
            "items": items,
            "next_cursor": items[-1]["change_id"] if len(rows) > limit else None,
            "scan_error": dict(error) if error else None,
        }
