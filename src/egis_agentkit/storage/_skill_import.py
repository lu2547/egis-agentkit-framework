"""技能导入任务、领取锁和租约状态。"""

from datetime import timedelta
from uuid import uuid4

from sqlalchemy import and_, func, or_, select, update

from egis_agentkit.storage import tables as t

_IMPORTS = t.skill_imports
_STATUS_COLUMNS = tuple(
    column
    for column in _IMPORTS.c
    if column.name
    not in {
        "archive_location",
        "lease_token",
        "lease_until",
        "created_by",
        "updated_by",
        "uploader_id",
        "available_at",
    }
)


class SkillImportStorage:
    """用数据库时钟和条件更新保护跨进程任务归属。"""

    def __init__(self, engine):
        self.engine = engine

    async def enqueue(self, **values):
        statement = _IMPORTS.insert().values(**values).returning(*_STATUS_COLUMNS)
        async with self.engine.begin() as conn:
            return dict((await conn.execute(statement)).mappings().one())

    async def get(self, uploader_id, job_id):
        statement = select(*_STATUS_COLUMNS).where(
            _IMPORTS.c.job_id == job_id, _IMPORTS.c.uploader_id == uploader_id
        )
        async with self.engine.connect() as conn:
            row = (await conn.execute(statement)).mappings().first()
            return dict(row) if row is not None else None

    async def list(self, uploader_id, *, limit):
        statement = (
            select(*_STATUS_COLUMNS)
            .where(_IMPORTS.c.uploader_id == uploader_id)
            .order_by(_IMPORTS.c.created_at.desc())
            .limit(limit)
        )
        async with self.engine.connect() as conn:
            return [dict(row) for row in (await conn.execute(statement)).mappings()]

    async def list_reviews(self, *, limit):
        statement = (
            select(*_STATUS_COLUMNS, _IMPORTS.c.uploader_id.label("uploaded_by"))
            .where(_IMPORTS.c.status == "pending_approval")
            .order_by(_IMPORTS.c.created_at, _IMPORTS.c.job_id)
            .limit(limit)
        )
        async with self.engine.connect() as conn:
            return [dict(row) for row in (await conn.execute(statement)).mappings()]

    async def pending_review(self, job_id):
        statement = select(_IMPORTS.c.filename, _IMPORTS.c.archive_location).where(
            _IMPORTS.c.job_id == job_id, _IMPORTS.c.status == "pending_approval"
        )
        async with self.engine.connect() as conn:
            row = (await conn.execute(statement)).mappings().one_or_none()
            return dict(row) if row is not None else None

    async def decide(self, job_id, *, approved, actor, reason):
        now = func.now()
        values = {
            "status": "queued" if approved else "rejected",
            "stage": "approved" if approved else "rejected",
            "reviewed_by": actor,
            "reviewed_at": now,
            "review_reason": reason,
            "updated_by": actor,
            "updated_at": now,
            "available_at": now,
            "attempts": 0,
        }
        if not approved:
            values["archive_location"] = None
        statement = (
            update(_IMPORTS)
            .where(_IMPORTS.c.job_id == job_id, _IMPORTS.c.status == "pending_approval")
            .values(**values)
            .returning(*_STATUS_COLUMNS)
        )
        async with self.engine.begin() as conn:
            row = (await conn.execute(statement)).mappings().one_or_none()
            return dict(row) if row is not None else None

    async def retry(self, uploader_id, job_id):
        now = func.now()
        statement = (
            update(_IMPORTS)
            .where(
                _IMPORTS.c.job_id == job_id,
                _IMPORTS.c.uploader_id == uploader_id,
                _IMPORTS.c.status == "failed",
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
            .returning(*_STATUS_COLUMNS)
        )
        async with self.engine.begin() as conn:
            row = (await conn.execute(statement)).mappings().first()
            return dict(row) if row is not None else None

    async def claim(self, *, lease_seconds, validation_progress):
        now = func.now()
        query = (
            select(_IMPORTS.c.job_id)
            .where(
                or_(
                    and_(_IMPORTS.c.status == "queued", _IMPORTS.c.available_at <= now),
                    and_(_IMPORTS.c.status == "running", _IMPORTS.c.lease_until < now),
                )
            )
            .order_by(_IMPORTS.c.created_at)
            .with_for_update(skip_locked=True)
            .limit(1)
        )
        async with self.engine.begin() as conn:
            job_id = await conn.scalar(query)
            if job_id is None:
                return None
            statement = (
                update(_IMPORTS)
                .where(_IMPORTS.c.job_id == job_id)
                .values(
                    status="running",
                    stage="validating",
                    progress=validation_progress,
                    attempts=_IMPORTS.c.attempts + 1,
                    lease_token=uuid4().hex,
                    lease_until=now + timedelta(seconds=lease_seconds),
                    updated_at=now,
                )
                .returning(_IMPORTS)
            )
            return dict((await conn.execute(statement)).mappings().one())

    async def update_claim(
        self, job_id, lease_token, *, renew_after=None, retry_after=None, **values
    ):
        now = func.now()
        if renew_after is not None:
            values["lease_until"] = now + timedelta(seconds=renew_after)
        if retry_after is not None:
            values["available_at"] = now + timedelta(seconds=retry_after)
        statement = (
            update(_IMPORTS)
            .where(
                _IMPORTS.c.job_id == job_id,
                _IMPORTS.c.lease_token == lease_token,
                _IMPORTS.c.status == "running",
            )
            .values(**values, updated_at=now)
        )
        async with self.engine.begin() as conn:
            result = await conn.execute(statement)
            return bool(result.rowcount)
