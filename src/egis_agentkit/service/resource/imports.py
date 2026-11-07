"""PostgreSQL-backed Skill imports with renewable claims and crash recovery."""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from datetime import timedelta
from uuid import uuid4

from sqlalchemy import and_, func, or_, select, update

from egis_agentkit.catalog_icons import catalog_icon_id
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service.user.service import UserService
from egis_agentkit.storage import tables as t

log = logging.getLogger(__name__)


class SkillImportService:
    lease_seconds = 90
    max_attempts = 3

    def __init__(self, storage, resources):
        self.storage, self.resources = storage, resources
        self._worker = None
        self._wake = asyncio.Event()

    @staticmethod
    def _view(row):
        return {
            key: value
            for key, value in dict(row).items()
            if key
            not in {
                "archive",
                "lease_token",
                "lease_until",
                "created_by",
                "updated_by",
                "owner_id",
                "available_at",
                "remote_uploaded",
            }
        }

    async def enqueue(self, user, stream, *, filename, category="other", avatar="book"):
        if not user.is_enabled:
            raise Forbidden()
        category = str(category).strip() or "other"
        if len(category) > 80:
            raise ValueError("Skill category exceeds limit")
        avatar = catalog_icon_id(avatar, default="book")
        # UploadFile has already spooled the request. Copy only the bounded ZIP;
        # archive parsing and checksums are exclusively worker work.
        content = await asyncio.to_thread(
            stream.read, self.resources.max_skill_archive_bytes + 1
        )
        if len(content) > self.resources.max_skill_archive_bytes:
            raise ValueError("Skill archive exceeds upload limit")
        job_id = uuid4().hex
        async with self.storage.transaction() as conn:
            row = (
                (
                    await conn.execute(
                        t.skill_imports.insert()
                        .values(
                            job_id=job_id,
                            owner_id=user.user_id,
                            filename=str(filename or "skill.zip")[:255],
                            category=category,
                            avatar=avatar,
                            archive=content,
                            resource_id="skill-" + job_id,
                            created_by=user.user_id,
                            updated_by=user.user_id,
                        )
                        .returning(
                            *[c for c in t.skill_imports.c if c.name != "archive"]
                        )
                    )
                )
                .mappings()
                .one()
            )
        self._wake.set()
        return self._view(row)

    async def get(self, user, job_id):
        if not user.is_enabled:
            raise Forbidden()
        async with self.storage.engine.connect() as conn:
            row = (
                (
                    await conn.execute(
                        select(
                            *[c for c in t.skill_imports.c if c.name != "archive"]
                        ).where(
                            t.skill_imports.c.job_id == job_id,
                            t.skill_imports.c.owner_id == user.user_id,
                        )
                    )
                )
                .mappings()
                .first()
            )
        if row is None:
            raise NotFound()
        return self._view(row)

    async def list(self, user):
        if not user.is_enabled:
            raise Forbidden()
        async with self.storage.engine.connect() as conn:
            rows = (
                (
                    await conn.execute(
                        select(*[c for c in t.skill_imports.c if c.name != "archive"])
                        .where(
                            t.skill_imports.c.owner_id == user.user_id,
                        )
                        .order_by(t.skill_imports.c.created_at.desc())
                        .limit(100)
                    )
                )
                .mappings()
                .all()
            )
        return [self._view(row) for row in rows]

    async def retry(self, user, job_id):
        await self.get(user, job_id)
        now = func.now()
        async with self.storage.transaction() as conn:
            result = await conn.execute(
                update(t.skill_imports)
                .where(
                    t.skill_imports.c.job_id == job_id,
                    t.skill_imports.c.owner_id == user.user_id,
                    t.skill_imports.c.status == "failed",
                )
                .values(
                    status="queued",
                    stage="queued",
                    progress=0,
                    error=None,
                    attempts=0,
                    lease_token=None,
                    lease_until=None,
                    available_at=now,
                    updated_at=now,
                )
            )
            if not result.rowcount:
                raise Conflict("Only failed imports can be retried")
        self._wake.set()
        return await self.get(user, job_id)

    async def claim(self):
        now = func.now()
        async with self.storage.transaction() as conn:
            row = (
                await conn.execute(
                    select(t.skill_imports.c.job_id)
                    .where(
                        or_(
                            and_(
                                t.skill_imports.c.status == "queued",
                                t.skill_imports.c.available_at <= now,
                            ),
                            and_(
                                t.skill_imports.c.status == "running",
                                t.skill_imports.c.lease_until < now,
                            ),
                        )
                    )
                    .order_by(t.skill_imports.c.created_at)
                    .with_for_update(skip_locked=True)
                    .limit(1)
                )
            ).first()
            if row is None:
                return None
            return dict(
                (
                    await conn.execute(
                        update(t.skill_imports)
                        .where(
                            t.skill_imports.c.job_id == row.job_id,
                        )
                        .values(
                            status="running",
                            stage="validating",
                            progress=10,
                            attempts=t.skill_imports.c.attempts + 1,
                            lease_token=uuid4().hex,
                            lease_until=now + timedelta(seconds=self.lease_seconds),
                            updated_at=now,
                        )
                        .returning(t.skill_imports)
                    )
                )
                .mappings()
                .one()
            )

    async def _update(self, job, **values):
        now = func.now()
        async with self.storage.transaction() as conn:
            result = await conn.execute(
                update(t.skill_imports)
                .where(
                    t.skill_imports.c.job_id == job["job_id"],
                    t.skill_imports.c.lease_token == job["lease_token"],
                    t.skill_imports.c.status == "running",
                )
                .values(**values, updated_at=now)
            )
        return bool(result.rowcount)

    async def _heartbeat(self, job, process):
        while True:
            await asyncio.sleep(self.lease_seconds / 3)
            try:
                renewed = await self._update(
                    job, lease_until=func.now() + timedelta(seconds=self.lease_seconds)
                )
            except Exception:
                log.warning("Skill import %s could not renew its lease", job["job_id"])
                renewed = False
            if not renewed:
                process.cancel()
                return

    async def process(self, job):
        heartbeat = asyncio.create_task(self._heartbeat(job, asyncio.current_task()))
        try:
            user = await UserService(self.storage).get(job["owner_id"])

            async def progress(stage, percent, **extra):
                if not await self._update(job, stage=stage, progress=percent, **extra):
                    raise asyncio.CancelledError

            result = await self.resources.upload_skill(
                user,
                job["archive"],
                category=job["category"],
                avatar=job["avatar"],
                resource_id=job["resource_id"],
                import_job_id=job["job_id"],
                progress=progress,
                remote_uploaded=job["remote_uploaded"],
            )
            await self._update(
                job,
                status="succeeded",
                stage="completed",
                progress=100,
                resource_version=result["resource_version"],
                result=result,
                error=None,
                archive=None,
                lease_until=None,
            )
        except asyncio.CancelledError:
            # Graceful shutdown releases our claim; SIGKILL recovers via expiry.
            await asyncio.shield(
                self._update(job, status="queued", stage="queued", lease_until=None)
            )
            raise
        except Exception as exc:
            retry = (
                not isinstance(exc, (ValueError, Forbidden, Conflict, NotFound))
                and job["attempts"] < self.max_attempts
            )
            # Domain/validation messages are safe; raw provider failures may contain
            # connection strings or credentials and stay in operational logs only.
            message = (
                str(exc)
                if isinstance(exc, (ValueError, Forbidden, Conflict, NotFound))
                else "Skill registration failed; retrying"
                if retry
                else "Skill registration failed; please retry"
            )
            log.warning(
                "Skill import %s failed (%s)", job["job_id"], type(exc).__name__
            )
            await self._update(
                job,
                status="queued" if retry else "failed",
                stage="retrying" if retry else "failed",
                error=message[:1000],
                lease_until=None,
                available_at=func.now()
                + timedelta(seconds=min(60, 5 * job["attempts"])),
            )
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat

    async def run_once(self):
        job = await self.claim()
        if job is None:
            return False
        processing = asyncio.create_task(self.process(job))
        try:
            await processing
        except asyncio.CancelledError:
            # A stolen lease cancels this job, while shutdown cancels the poller.
            # Keep polling after the former; propagate the latter.
            if asyncio.current_task().cancelling():
                raise
        return True

    async def _run(self):
        while True:
            try:
                if await self.run_once():
                    continue
                self._wake.clear()
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), timeout=2)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Skill import worker could not poll PostgreSQL")
                await asyncio.sleep(2)

    async def start(self):
        if self._worker is None:
            self._worker = asyncio.create_task(self._run(), name="skill-import-worker")

    async def close(self):
        if self._worker:
            self._worker.cancel()
            with suppress(asyncio.CancelledError):
                await self._worker
            self._worker = None
