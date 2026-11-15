"""管理 NAS 上传文件的异步技能导入，数据库只保存任务状态和文件定位。

核心能力
- enqueue：校验上传限额、完整写 NAS 后创建任务，失败不产生可领取任务。
- process：通过 NAS 定位恢复上传内容，执行注册并保存结果。
- claim / retry：领取、租约续期及失败重试，不把文件字节传入任务表。
"""

from __future__ import annotations

import asyncio
import logging
from contextlib import suppress
from uuid import uuid4

from egis_agentkit.client.nacos._settings import (
    SkillImportConfig,
)
from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service._user import UserService
from egis_agentkit.service.resource._catalog import catalog_icon_id

log = logging.getLogger(__name__)
VALIDATION_PROGRESS = 10
COMPLETION_PROGRESS = 100
CATEGORY_MAX_LENGTH = 80
FILENAME_MAX_LENGTH = 255
ERROR_MAX_LENGTH = 1000
HEARTBEATS_PER_LEASE = 3


class SkillImportService:
    def __init__(self, storage, resources):
        self.resources = resources
        self.jobs = storage.skill_imports
        self.users = UserService(storage)
        self._worker = None
        self._wake = asyncio.Event()
        self.lease_seconds = None

    def _settings(self) -> SkillImportConfig:
        settings = (
            self.resources.config.snapshot.skill.import_settings
            if self.resources.config is not None
            else SkillImportConfig()
        )
        if self.lease_seconds is not None:
            return settings.model_copy(update={"lease_seconds": self.lease_seconds})
        return settings

    async def enqueue(self, user, stream, *, filename, category="other", avatar="book"):
        if not user.is_enabled:
            raise Forbidden()
        category = str(category).strip() or "other"
        if len(category) > CATEGORY_MAX_LENGTH:
            raise ValueError("Skill category exceeds limit")
        avatar = catalog_icon_id(avatar, default="book")
        # 1. 有界读取上传包；归档格式和内容规则仍交给异步任务校验。
        maximum = self.resources.max_skill_archive_bytes
        content = await asyncio.to_thread(stream.read, maximum + 1)
        if len(content) > maximum:
            raise ValueError("Skill archive exceeds upload limit")
        # 2. NAS 文件完整可读后才能入队；其他 API 实例可从同一挂载目录恢复。
        location = await asyncio.to_thread(
            self.resources.skill_archives.write_archive, content
        )
        job_id = uuid4().hex
        row = await self.jobs.enqueue(
            job_id=job_id,
            owner_id=user.user_id,
            filename=str(filename or "skill.zip")[:FILENAME_MAX_LENGTH],
            category=category,
            avatar=avatar,
            archive_location=location,
            resource_id="skill-" + job_id,
            created_by=user.user_id,
            updated_by=user.user_id,
        )
        self._wake.set()
        return row

    async def get(self, user, job_id):
        if not user.is_enabled:
            raise Forbidden()
        row = await self.jobs.get(user.user_id, job_id)
        if row is None:
            raise NotFound()
        return row

    async def list(self, user):
        if not user.is_enabled:
            raise Forbidden()
        return await self.jobs.list(user.user_id, limit=self._settings().list_limit)

    async def retry(self, user, job_id):
        await self.get(user, job_id)
        row = await self.jobs.retry(user.user_id, job_id)
        if row is None:
            raise Conflict("Only failed imports can be retried")
        self._wake.set()
        return row

    async def claim(self, *, settings=None):
        settings = settings or self._settings()
        return await self.jobs.claim(
            lease_seconds=settings.lease_seconds,
            validation_progress=VALIDATION_PROGRESS,
        )

    async def _update(self, job, **values):
        return await self.jobs.update_claim(job["job_id"], job["lease_token"], **values)

    async def _heartbeat(self, job, process, settings):
        while True:
            await asyncio.sleep(settings.lease_seconds / HEARTBEATS_PER_LEASE)
            try:
                renewed = await self._update(
                    job,
                    renew_after=settings.lease_seconds,
                )
            except Exception:
                log.warning("Skill import %s could not renew its lease", job["job_id"])
                renewed = False
            if not renewed:
                process.cancel()
                return

    async def process(self, job, *, settings=None):
        settings = settings or self._settings()
        heartbeat = asyncio.create_task(
            self._heartbeat(job, asyncio.current_task(), settings)
        )
        try:
            user = await self.users.get(job["owner_id"])

            async def progress(stage, percent, **extra):
                if not await self._update(job, stage=stage, progress=percent, **extra):
                    raise asyncio.CancelledError

            # 1. 读取受管 NAS 包，任务表不保存上传字节。
            archive = await asyncio.to_thread(
                self.resources.skill_archives.read_archive, job["archive_location"]
            )
            result = await self.resources.upload_skill(
                user,
                archive,
                category=job["category"],
                avatar=job["avatar"],
                resource_id=job["resource_id"],
                import_job_id=job["job_id"],
                progress=progress,
            )
            # 2. 注册完成后释放任务引用；失败任务保留引用供重试。
            await self._update(
                job,
                status="succeeded",
                stage="completed",
                progress=COMPLETION_PROGRESS,
                resource_version=result["resource_version"],
                result=result,
                error=None,
                archive_location=None,
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
                and job["attempts"] < settings.max_attempts
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
                error=message[:ERROR_MAX_LENGTH],
                lease_until=None,
                retry_after=min(
                    settings.max_retry_delay_seconds,
                    settings.retry_backoff_seconds * job["attempts"],
                ),
            )
        finally:
            heartbeat.cancel()
            with suppress(asyncio.CancelledError):
                await heartbeat

    async def run_once(self):
        settings = self._settings()
        job = await self.claim(settings=settings)
        if job is None:
            return False
        processing = asyncio.create_task(self.process(job, settings=settings))
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
            interval = self._settings().poll_seconds
            try:
                if await self.run_once():
                    continue
                self._wake.clear()
                with suppress(asyncio.TimeoutError):
                    await asyncio.wait_for(self._wake.wait(), timeout=interval)
            except asyncio.CancelledError:
                raise
            except Exception:
                log.exception("Skill import worker could not poll PostgreSQL")
                await asyncio.sleep(interval)

    async def start(self):
        if self._worker is None:
            self._worker = asyncio.create_task(self._run(), name="skill-import-worker")

    async def close(self):
        if self._worker:
            self._worker.cancel()
            with suppress(asyncio.CancelledError):
                await self._worker
            self._worker = None
