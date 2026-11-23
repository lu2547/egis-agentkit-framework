"""Persist managed schedules and arbitrate dispatch and execution in PostgreSQL."""

from __future__ import annotations

from collections.abc import Callable
from datetime import datetime, timedelta, timezone
from uuid import uuid4

from agentscope.app.storage._sql._tables import ScheduleRow
from pydantic_core import to_jsonable_python
from sqlalchemy import (
    JSON,
    CheckConstraint,
    Column,
    DateTime,
    ForeignKey,
    Index,
    MetaData,
    String,
    Table,
    Text,
    cast,
    func,
    literal,
    or_,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB, array, insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.client._postgres import PostgresTransaction
from egis_agentkit.errors import Conflict

from .model._tables import metadata

# Keep the upstream ORM mapping unchanged for its own schedule records.
schedules = ScheduleRow.__table__.to_metadata(MetaData())
schedules.append_column(Column("project_id", String(64)))
schedules.append_column(Column("next_run_at", DateTime(timezone=True)))
schedules.append_column(Column("deleted_at", DateTime(timezone=True)))

schedule_runs = Table(
    "ak_schedule_runs",
    metadata,
    Column("id", String(64), primary_key=True),
    Column("schedule_id", String(255), ForeignKey(schedules.c.id), nullable=False),
    Column("schedule_name", Text, nullable=False),
    Column("project_id", String(64), nullable=False),
    Column("trigger_type", String(16), nullable=False),
    Column("scheduled_for", DateTime(timezone=True)),
    Column("status", String(16), nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False),
    Column("started_at", DateTime(timezone=True)),
    Column("finished_at", DateTime(timezone=True)),
    Column("input_snapshot", JSONB, nullable=False),
    Column("session_id", String(255)),
    Column("reply_id", String(255)),
    Column("summary", Text),
    Column("error_code", String(64)),
    Column("error_message", Text),
    Column("archived_at", DateTime(timezone=True)),
    Column("deleted_at", DateTime(timezone=True)),
    CheckConstraint(
        "trigger_type IN ('scheduled', 'backfill', 'test')",
        name="ak_schedule_runs_trigger_type_check",
    ),
    CheckConstraint(
        "(trigger_type = 'test') = (scheduled_for IS NULL)",
        name="ak_schedule_runs_scheduled_for_check",
    ),
    CheckConstraint(
        "status IN ('queued', 'running', 'succeeded', 'failed')",
        name="ak_schedule_runs_status_check",
    ),
)
schedule_columns = schedules.c
run_columns = schedule_runs.c
Index(
    "uq_ak_schedule_runs_automatic_time",
    run_columns.schedule_id,
    run_columns.scheduled_for,
    unique=True,
    postgresql_where=run_columns.scheduled_for.is_not(None),
)
Index(
    "ix_ak_schedule_runs_history",
    run_columns.schedule_id,
    run_columns.created_at.desc(),
    run_columns.id,
)
Index(
    "ix_ak_schedule_runs_queue",
    run_columns.created_at,
    run_columns.id,
    postgresql_where=text("status = 'queued'"),
)
Index(
    "ix_ak_schedule_runs_running",
    run_columns.started_at,
    postgresql_where=text("status = 'running'"),
)

_INPUT_FIELDS = ("description", "model_key", "permission_mode", "stateful", "files")
_PAYLOAD_FIELDS = (
    *_INPUT_FIELDS,
    "name",
    "timezone",
    "enabled",
    "starts_on",
    "ends_on",
    "rule",
    "cron_expression",
)
_FINISHED = ("succeeded", "failed")
_ACTIVE = (
    schedule_columns.project_id.is_not(None),
    schedule_columns.deleted_at.is_(None),
)
_ENABLED = schedule_columns.payload["data"]["enabled"].as_boolean().is_(True)
_RUNS_WITH_OWNER = select(schedule_runs, schedule_columns.user_id).join_from(
    schedule_runs, schedules, schedule_columns.id == run_columns.schedule_id
)
_RUN_ORDER = (run_columns.created_at.desc(), run_columns.id)


async def create_schedule_columns(conn: AsyncConnection) -> None:
    """Install managed columns and indexes during explicit schema provisioning."""
    await conn.execute(
        text("""
        ALTER TABLE schedules
            ADD COLUMN IF NOT EXISTS project_id varchar(64),
            ADD COLUMN IF NOT EXISTS next_run_at timestamptz,
            ADD COLUMN IF NOT EXISTS deleted_at timestamptz
    """)
    )
    await conn.execute(
        text("""
        CREATE INDEX IF NOT EXISTS ix_schedules_user_project
        ON schedules (user_id, project_id)
        WHERE project_id IS NOT NULL AND deleted_at IS NULL
    """)
    )
    await conn.execute(
        text("""
        CREATE INDEX IF NOT EXISTS ix_schedules_due
        ON schedules (next_run_at, id)
        WHERE deleted_at IS NULL AND next_run_at IS NOT NULL
    """)
    )


def _naive_utc(value: datetime) -> datetime:
    if value.tzinfo is None:
        return value
    return value.astimezone(timezone.utc).replace(tzinfo=None)


def _task(row) -> dict:
    result = {**row["payload"]["data"], **row}
    del result["payload"]
    for field in ("created_at", "updated_at"):
        result[field] = result[field].replace(tzinfo=timezone.utc)
    return result


def _run(row) -> dict:
    return {**row, "agent_id": row["input_snapshot"]["agent_id"]}


async def _first(conn, query):
    return (await conn.execute(query)).mappings().first()


class ScheduleStorage:
    """Store Egis definitions and their immutable execution inputs."""

    def __init__(self, engine: AsyncEngine | PostgresTransaction):
        self.engine = engine

    async def create(self, task: dict) -> dict:
        created_at = task.get("created_at") or datetime.now(timezone.utc)
        payload = to_jsonable_python(
            {field: task[field] for field in _PAYLOAD_FIELDS if field in task}
        )
        async with self.engine.begin() as conn:
            row = await _first(
                conn,
                insert(schedules)
                .values(
                    id=task["id"],
                    user_id=task["user_id"],
                    agent_id=task["agent_id"],
                    project_id=task["project_id"],
                    next_run_at=task.get("next_run_at"),
                    created_at=_naive_utc(created_at),
                    updated_at=_naive_utc(task.get("updated_at") or created_at),
                    payload={"data": payload},
                )
                .returning(schedules),
            )
        return _task(row)

    async def get(self, user_id: str, schedule_id: str) -> dict | None:
        async with self.engine.connect() as conn:
            row = await self._get_owned(conn, user_id, schedule_id)
            return _task(row) if row is not None else None

    async def _get_owned(self, conn, user_id, schedule_id, *, lock=False):
        query = select(schedules).where(
            *_ACTIVE,
            schedule_columns.user_id == user_id,
            schedule_columns.id == schedule_id,
        )
        return await _first(conn, query.with_for_update() if lock else query)

    async def list(self, user_id: str) -> list[dict]:
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                select(schedules)
                .where(*_ACTIVE, schedule_columns.user_id == user_id)
                .order_by(schedule_columns.created_at.desc(), schedule_columns.id)
            )
            return [_task(row) for row in rows.mappings()]

    async def update(
        self, user_id: str, schedule_id: str, changes: dict, *, expected_updated_at=None
    ) -> dict | None:
        async with self.engine.begin() as conn:
            row = await self._get_owned(conn, user_id, schedule_id, lock=True)
            if row is None:
                return None
            if expected_updated_at is not None and row["updated_at"] != _naive_utc(
                expected_updated_at
            ):
                raise Conflict("定时任务已更新，请刷新后重试")
            payload = dict(row["payload"])
            payload["data"] = {
                **payload["data"],
                **to_jsonable_python(
                    {
                        field: changes[field]
                        for field in _PAYLOAD_FIELDS
                        if field in changes
                    }
                ),
            }
            values = {
                field: changes[field]
                for field in ("agent_id", "project_id", "next_run_at")
                if field in changes
            }
            row = await _first(
                conn,
                update(schedules)
                .where(schedule_columns.id == schedule_id)
                .values(
                    **values,
                    payload=payload,
                    updated_at=_naive_utc(
                        changes.get("updated_at") or datetime.now(timezone.utc)
                    ),
                )
                .returning(schedules),
            )
            return _task(row)

    async def delete(
        self, user_id: str, ids: list[str], now: datetime, *, project_id=None
    ) -> int:
        requested = set(ids)
        if not requested:
            return 0
        project_filter = (
            () if project_id is None else (schedule_columns.project_id == project_id,)
        )
        async with self.engine.begin() as conn:
            owned = await conn.scalars(
                select(schedule_columns.id)
                .where(
                    *_ACTIVE,
                    schedule_columns.user_id == user_id,
                    schedule_columns.id.in_(requested),
                    *project_filter,
                )
                .order_by(schedule_columns.id)
                .with_for_update()
            )
            if set(owned) != requested:
                return 0
            result = await conn.execute(
                update(schedules)
                .where(schedule_columns.id.in_(requested))
                .values(next_run_at=None, deleted_at=now, updated_at=_naive_utc(now))
            )
            return result.rowcount

    async def disable_project(
        self, user_id: str, project_id: str, now: datetime
    ) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                update(schedules)
                .where(
                    *_ACTIVE,
                    schedule_columns.user_id == user_id,
                    schedule_columns.project_id == project_id,
                    or_(_ENABLED, schedule_columns.next_run_at.is_not(None)),
                )
                .values(
                    next_run_at=None,
                    updated_at=_naive_utc(now),
                    payload=cast(
                        func.jsonb_set(
                            cast(schedule_columns.payload, JSONB),
                            array(["data", "enabled"]),
                            literal(False, type_=JSONB),
                        ),
                        JSON,
                    ),
                )
            )

    async def enqueue_test(
        self, user_id: str, schedule_id: str, now: datetime, *, project_id=None
    ) -> dict | None:
        async with self.engine.begin() as conn:
            row = await self._get_owned(conn, user_id, schedule_id, lock=True)
            if (
                row is None
                or project_id is not None
                and row["project_id"] != project_id
            ):
                return None
            return await self._enqueue(conn, row, now, "test", None)

    async def due_ids(self, now: datetime, limit: int = 100) -> list[str]:
        async with self.engine.connect() as conn:
            ids = await conn.scalars(
                select(schedule_columns.id)
                .where(*_ACTIVE, _ENABLED, schedule_columns.next_run_at <= now)
                .order_by(schedule_columns.next_run_at, schedule_columns.id)
                .limit(limit)
            )
            return list(ids)

    async def enqueue_due(
        self,
        schedule_id: str,
        now: datetime,
        plan: Callable[[dict], tuple[datetime | None, datetime | None]],
    ) -> dict | None:
        async with self.engine.begin() as conn:
            row = await _first(
                conn,
                select(schedules)
                .where(
                    *_ACTIVE,
                    _ENABLED,
                    schedule_columns.id == schedule_id,
                    schedule_columns.next_run_at <= now,
                )
                .with_for_update(skip_locked=True),
            )
            if row is None:
                return None
            scheduled_for, next_run_at = plan(_task(row))
            await conn.execute(
                update(schedules)
                .where(schedule_columns.id == schedule_id)
                .values(next_run_at=next_run_at, updated_at=_naive_utc(now))
            )
            if scheduled_for is None:
                return None
            kind = (
                "backfill"
                if now - scheduled_for > timedelta(minutes=5)
                else "scheduled"
            )
            return await self._enqueue(conn, row, now, kind, scheduled_for)

    async def _enqueue(self, conn, schedule, now, trigger_type, scheduled_for):
        task = _task(schedule)
        snapshot = {field: task[field] for field in _INPUT_FIELDS if field in task}
        snapshot["agent_id"] = task["agent_id"]
        query = (
            insert(schedule_runs)
            .values(
                id=uuid4().hex,
                schedule_id=task["id"],
                schedule_name=task["name"],
                project_id=task["project_id"],
                trigger_type=trigger_type,
                scheduled_for=scheduled_for,
                input_snapshot=snapshot,
                created_at=now,
                status="queued",
            )
            .on_conflict_do_nothing(
                index_elements=[run_columns.schedule_id, run_columns.scheduled_for],
                index_where=run_columns.scheduled_for.is_not(None),
            )
            .returning(schedule_runs)
        )
        row = await _first(conn, query)
        return _run({**row, "user_id": task["user_id"]}) if row is not None else None

    async def list_runs(
        self, user_id: str, *, schedule_id=None, status=None, q=None, archived=False
    ) -> list[dict]:
        query = _RUNS_WITH_OWNER.where(
            schedule_columns.user_id == user_id,
            run_columns.deleted_at.is_(None),
            run_columns.archived_at.is_not(None)
            if archived
            else run_columns.archived_at.is_(None),
        )
        if schedule_id is not None:
            query = query.where(run_columns.schedule_id == schedule_id)
        if status is not None:
            query = query.where(run_columns.status == status)
        if q:
            query = query.where(
                or_(
                    run_columns.schedule_name.icontains(q, autoescape=True),
                    run_columns.summary.icontains(q, autoescape=True),
                )
            )
        async with self.engine.connect() as conn:
            rows = await conn.execute(query.order_by(*_RUN_ORDER))
            return [_run(row) for row in rows.mappings()]

    async def _get_run(self, conn, run_id, *conditions):
        row = await _first(
            conn, _RUNS_WITH_OWNER.where(run_columns.id == run_id, *conditions)
        )
        return _run(row) if row is not None else None

    async def get_run(self, user_id: str, run_id: str) -> dict | None:
        async with self.engine.connect() as conn:
            return await self._get_run(
                conn,
                run_id,
                schedule_columns.user_id == user_id,
                run_columns.deleted_at.is_(None),
            )

    async def archive(
        self, user_id: str, run_id: str, archived: bool, now: datetime
    ) -> dict | None:
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(schedule_runs)
                .where(
                    run_columns.id == run_id,
                    run_columns.deleted_at.is_(None),
                    run_columns.status.in_(_FINISHED),
                    run_columns.schedule_id == schedule_columns.id,
                    schedule_columns.user_id == user_id,
                )
                .values(archived_at=now if archived else None)
            )
            return await self._get_run(conn, run_id) if result.rowcount else None

    async def delete_run(self, user_id: str, run_id: str, now: datetime) -> bool:
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(schedule_runs)
                .where(
                    run_columns.id == run_id,
                    run_columns.deleted_at.is_(None),
                    run_columns.status.in_(_FINISHED),
                    run_columns.schedule_id == schedule_columns.id,
                    schedule_columns.user_id == user_id,
                )
                .values(deleted_at=now)
            )
            return bool(result.rowcount)

    async def list_queued(self, limit: int = 100) -> list[dict]:
        active = schedule_runs.alias("active_runs")
        schedule_is_running = (
            select(active.c.id)
            .where(
                active.c.schedule_id == run_columns.schedule_id,
                active.c.status == "running",
            )
            .exists()
        )
        async with self.engine.connect() as conn:
            rows = await conn.execute(
                _RUNS_WITH_OWNER.where(
                    run_columns.status == "queued", ~schedule_is_running
                )
                .order_by(run_columns.created_at, run_columns.id)
                .limit(limit)
            )
            return [_run(row) for row in rows.mappings()]

    async def claim_run(self, run_id: str, now: datetime) -> dict | None:
        async with self.engine.begin() as conn:
            # Serialize attempts for one schedule, including workers in other processes.
            queued = await _first(
                conn,
                _RUNS_WITH_OWNER.where(
                    run_columns.id == run_id, run_columns.status == "queued"
                ).with_for_update(of=schedules, skip_locked=True),
            )
            if queued is None:
                return None
            active = await conn.scalar(
                select(run_columns.id)
                .where(
                    run_columns.schedule_id == queued["schedule_id"],
                    run_columns.status == "running",
                )
                .limit(1)
            )
            if active is not None:
                return None
            result = await conn.execute(
                update(schedule_runs)
                .where(run_columns.id == run_id, run_columns.status == "queued")
                .values(status="running", started_at=now)
            )
            return await self._get_run(conn, run_id) if result.rowcount else None

    async def bind_run(
        self, run_id: str, session_id: str, reply_id: str | None = None
    ) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                update(schedule_runs)
                .where(run_columns.id == run_id, run_columns.status == "running")
                .values(
                    session_id=session_id,
                    reply_id=func.coalesce(reply_id, run_columns.reply_id),
                )
            )

    async def finish_run(
        self,
        run_id: str,
        now: datetime,
        *,
        status: str,
        reply_id=None,
        summary=None,
        error_code=None,
        error_message=None,
    ) -> None:
        if status not in _FINISHED:
            raise ValueError("Expected a terminal schedule run status")
        async with self.engine.begin() as conn:
            await conn.execute(
                update(schedule_runs)
                .where(run_columns.id == run_id, run_columns.status == "running")
                .values(
                    status=status,
                    finished_at=now,
                    reply_id=func.coalesce(reply_id, run_columns.reply_id),
                    summary=summary,
                    error_code=error_code,
                    error_message=error_message,
                )
            )

    async def expire_running(self, cutoff: datetime, now: datetime) -> int:
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(schedule_runs)
                .where(run_columns.status == "running", run_columns.started_at < cutoff)
                .values(
                    status="failed",
                    finished_at=now,
                    error_code="interrupted",
                    error_message="运行中断，未重新执行",
                )
            )
            return result.rowcount

    async def last_stateful_run(self, schedule_id: str, project_id: str) -> dict | None:
        async with self.engine.connect() as conn:
            row = await _first(
                conn,
                _RUNS_WITH_OWNER.where(
                    run_columns.schedule_id == schedule_id,
                    run_columns.project_id == project_id,
                    run_columns.trigger_type != "test",
                    run_columns.status.in_(_FINISHED),
                    run_columns.session_id.is_not(None),
                    run_columns.input_snapshot["stateful"].as_boolean().is_(True),
                )
                .order_by(*_RUN_ORDER)
                .limit(1),
            )
            return _run(row) if row is not None else None
