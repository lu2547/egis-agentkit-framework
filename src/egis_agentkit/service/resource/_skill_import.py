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
from hashlib import sha256
from uuid import uuid4

from egis_agentkit.client._logging import exception_origin
from egis_agentkit.client.nacos._settings import (
    SkillImportConfig,
)
from egis_agentkit.core.skill._archive import read_archive_files
from egis_agentkit.core.skill._avatar import AUTO_AVATAR
from egis_agentkit.errors import (
    AgentKitError,
    BadRequest,
    Conflict,
    Forbidden,
    InvalidRequest,
    NotFound,
)
from egis_agentkit.service._user import UserService, require_admin
from egis_agentkit.service._virtual_user import VirtualUserService
from egis_agentkit.service.resource._catalog import normalize_skill_avatar
from egis_agentkit.storage.project._files import ProjectFileStorage
from egis_agentkit.storage.skill._upload import SkillUploadStorage

from ._skill import decode_skill_markdown

log = logging.getLogger(__name__)
VALIDATION_PROGRESS = 10
COMPLETION_PROGRESS = 100
CATEGORY_MAX_LENGTH = 80
FILENAME_MAX_LENGTH = 255
ERROR_MAX_LENGTH = 1000
HEARTBEATS_PER_LEASE = 3


class SkillImportService:
    def __init__(self, storage, resources, *, virtual_users=None):
        self.resources = resources
        self.jobs = storage.skill_imports
        self.users = UserService(storage)
        self.virtual_users = virtual_users or VirtualUserService(storage.virtual_users)
        self.uploads = SkillUploadStorage.from_config(
            resources.workspace_root, resources.config
        )
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

    async def install_from_project(self, scope, path):
        """读取当前项目上传区，复用技能包的校验、发布和启用操作。"""
        if not scope.project_id or not path.lower().endswith(".zip"):
            raise InvalidRequest("请选择当前项目上传区中的 ZIP 技能包")
        user = await self.users.get(scope.user_id)
        files = ProjectFileStorage(
            self.resources.workspace_root, config_provider=self.resources.config
        )
        content, _ = await asyncio.to_thread(
            files.read, user.user_id, scope.project_id, "uploads", path
        )
        # 重试同一包时恢复此人的同一技能，再幂等保存安装开关。
        digest = sha256(user.user_id.encode() + b"\0" + content).hexdigest()
        resource_id = f"skill-{digest}"
        await self.resources.upload_skill(
            user,
            content,
            resource_id=resource_id,
            import_job_id=f"chat-{digest}",
            owner_type="user",
            allow_unversioned=True,
        )
        return await self.resources.install(user, "skill", resource_id)

    async def create_for_user(self, scope, skill_id, markdown):
        """发布当前用户的 SKILL.md；直接创建功能仅接受新技能身份。"""
        user = await self.users.get(scope.user_id)
        if await self.resources.skill_registry.list(resource_ids=[skill_id]):
            raise Conflict("技能 ID 已存在，请使用新 ID")
        return await self.resources.save_skill(
            user, {"resource_id": skill_id, "markdown": markdown, "owner_type": "user"}
        )

    async def enqueue(
        self,
        user,
        stream,
        *,
        filename,
        category="other",
        avatar=AUTO_AVATAR,
        owner_type="user",
        virtual=False,
        resource_id=None,
    ):
        if not user.is_enabled:
            raise Forbidden()
        if virtual:
            owner_type = "system"
        elif owner_type == "system":
            require_admin(user)
        elif owner_type != "user":
            raise InvalidRequest("无效的技能归属类型")
        if resource_id:
            await self.resources.require_skill_upload_target(
                user, resource_id, owner_type=owner_type, virtual=virtual
            )
        uploader_type = (
            "virtual" if virtual else "admin" if user.role_type == "admin" else "user"
        )
        category = str(category).strip() or "other"
        if len(category) > CATEGORY_MAX_LENGTH:
            raise BadRequest("Skill category exceeds limit")
        avatar = normalize_skill_avatar(avatar)
        # 1. 有界读取上传包；归档格式和内容规则仍交给异步任务校验。
        maximum = self.resources.max_skill_archive_bytes
        content = await asyncio.to_thread(stream.read, maximum + 1)
        if len(content) > maximum:
            raise BadRequest("Skill archive exceeds upload limit")
        # 2. NAS 文件完整可读后才能入队；其他 API 实例可从同一挂载目录恢复。
        job_id = uuid4().hex
        location = await asyncio.to_thread(self.uploads.write, job_id, content)
        row = await self.jobs.enqueue(
            job_id=job_id,
            uploader_id=user.user_id,
            owner_type=owner_type,
            uploader_type=uploader_type,
            filename=str(filename or "skill.zip")[:FILENAME_MAX_LENGTH],
            category=category,
            avatar=avatar,
            archive_location=location,
            resource_id=resource_id or ("skill-" + job_id),
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

    async def list_reviews(self, admin, *, page=None):
        require_admin(admin)
        if page is None:
            return await self.jobs.list_reviews(limit=self._settings().list_limit)
        rows = await self.jobs.list_reviews(limit=page.fetch_limit, offset=page.offset)
        return page.response(rows)

    async def download_review(self, admin, job_id):
        require_admin(admin)
        job = await self.jobs.pending_review(job_id)
        if job is None:
            raise NotFound("待审批技能包不存在")
        content = await asyncio.to_thread(
            self.uploads.read, job_id, job["archive_location"]
        )
        return job["filename"], content

    async def decide_batch(self, admin, job_ids, *, approved, reason):
        require_admin(admin)
        if not job_ids or len(job_ids) > self._settings().approval_batch_limit:
            raise InvalidRequest("技能导入审批数量超出限制")
        if len(set(job_ids)) != len(job_ids):
            raise InvalidRequest("技能导入审批编号重复")
        if not approved and not reason.strip():
            raise InvalidRequest("拒绝时请填写原因")
        results = []
        for job_id in job_ids:
            try:
                result = await self.jobs.decide(
                    job_id, approved=approved, actor=admin.user_id, reason=reason
                )
                if result is None:
                    raise Conflict("技能导入不在待审批状态")
                if approved:
                    self._wake.set()
                else:
                    await self._remove_upload(job_id)
                results.append({"id": job_id, "ok": True})
            except AgentKitError as exc:
                system = exc.status_code >= 500
                log.log(
                    logging.ERROR if system else logging.WARNING,
                    "Skill import review failed" if system else "Skill import review rejected",
                    extra={
                        "component": "SkillImportService.decide_batch",
                        "context": {
                            "job_id": job_id,
                            "category": "system" if system else "business",
                            "error_type": type(exc).__name__,
                            "error_description": str(exc),
                            "origin": exception_origin(exc),
                        },
                    },
                    exc_info=exc if system else None,
                )
                results.append({"id": job_id, "ok": False, "detail": str(exc)})
        return results

    async def _remove_upload(self, job_id):
        try:
            await asyncio.to_thread(self.uploads.remove, job_id)
        except (OSError, AgentKitError) as exc:
            log.warning(
                "Could not remove skill upload %s (%s)", job_id, type(exc).__name__
            )

    def _validate_submission(self, archive):
        files = read_archive_files(archive, self.resources.archive_limits)
        return self.resources._skill_data(
            decode_skill_markdown(files["SKILL.md"]), {}, require_version=True
        )

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
        context = {"job_id": job["job_id"], "attempt": job["attempts"]}
        log.info(
            "Skill import started",
            extra={"component": "SkillImportService.process", "context": context},
        )
        heartbeat = asyncio.create_task(
            self._heartbeat(job, asyncio.current_task(), settings)
        )
        try:
            uploader = (
                await self.virtual_users.get(job["uploader_id"])
                if job["uploader_type"] == "virtual"
                else await self.users.get(job["uploader_id"])
            )

            async def progress(stage, percent, **extra):
                if not await self._update(job, stage=stage, progress=percent, **extra):
                    raise asyncio.CancelledError

            # 1. 读取受管 NAS 包，任务表不保存上传字节。
            archive = await asyncio.to_thread(
                self.uploads.read, job["job_id"], job["archive_location"]
            )
            system_owned = job["owner_type"] == "system"
            if system_owned and not job["reviewed_by"]:
                skill = await asyncio.to_thread(self._validate_submission, archive)
                submitted = await self._update(
                    job,
                    status="pending_approval",
                    stage="pending_approval",
                    resource_version=skill["resource_version"],
                    progress=COMPLETION_PROGRESS,
                    result={
                        "display_name": skill["display_name"],
                        "description": skill["description"],
                    },
                    lease_until=None,
                )
                if submitted:
                    log.info(
                        "Skill import is ready for approval",
                        extra={"component": "SkillImportService.process", "context": context},
                    )
                return
            publisher = (
                await self.users.get(job["reviewed_by"]) if system_owned else uploader
            )
            result = await self.resources.upload_skill(
                publisher,
                archive,
                category=job["category"],
                avatar=job["avatar"],
                resource_id=job["resource_id"],
                import_job_id=job["job_id"],
                progress=progress,
                owner_type=job["owner_type"],
                owner_id="system" if system_owned else uploader.user_id,
                uploader_id=uploader.user_id,
                uploader_type=job["uploader_type"],
            )
            # 2. 注册完成后释放任务引用；失败任务保留引用供重试。
            completed = await self._update(
                job,
                status="succeeded",
                stage="completed",
                progress=COMPLETION_PROGRESS,
                resource_version=result["resource_version"],
                result={
                    key: result[key]
                    for key in ("resource_id", "resource_version", "display_name")
                },
                error=None,
                archive_location=None,
                lease_until=None,
            )
            if completed:
                await self._remove_upload(job["job_id"])
                log.info(
                    "Skill import completed",
                    extra={"component": "SkillImportService.process", "context": context},
                )
        except asyncio.CancelledError:
            # Graceful shutdown releases our claim; SIGKILL recovers via expiry.
            await asyncio.shield(
                self._update(job, status="queued", stage="queued", lease_until=None)
            )
            raise
        except Exception as exc:
            business = isinstance(exc, AgentKitError) and exc.status_code < 500
            retry = not business and job["attempts"] < settings.max_attempts
            # Domain/validation messages are safe; raw provider failures may contain
            # connection strings or credentials and stay in operational logs only.
            message = (
                str(exc)
                if business
                else "Skill registration failed; retrying"
                if retry
                else "Skill registration failed; please retry"
            )
            details = {
                **context,
                "category": "business" if business else "system",
                "error_type": type(exc).__name__,
                "will_retry": retry,
            }
            if business:
                details["origin"] = exception_origin(exc)
                details["error_description"] = message
            log.log(
                logging.WARNING if business else logging.ERROR,
                "Skill import rejected" if business else "Skill import failed",
                extra={"component": "SkillImportService.process", "context": details},
                exc_info=not business,
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
