"""Store original messages in a NAS journal and publish its length in PostgreSQL."""

import asyncio
import json
import logging
from contextlib import nullcontext, suppress
from itertools import chain

from agentscope.app.storage._sql._tables import SessionRow
from agentscope.message import Msg
from sqlalchemy import delete, func, select, text, tuple_, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.errors import AgentKitError, Forbidden, NotFound

from ._files import HISTORY_CHUNK_BYTES, FileStorage
from .model._tables import (
    session_history,
    session_history_gc,
    session_history_write_fence,
)

logger = logging.getLogger(__name__)
_CLEANUP_BATCH = 100


def _records(messages, *, replace):
    if replace:
        yield b'{"op":"replace"}\n'
    for msg in messages:
        yield b'{"op":"upsert","message":' + msg.model_dump_json().encode() + b"}\n"


def _write_chunks(records):
    pending = bytearray()
    for record in records:
        rest = memoryview(record)
        while rest:
            count = min(len(rest), HISTORY_CHUNK_BYTES - len(pending))
            pending.extend(rest[:count])
            rest = rest[count:]
            if len(pending) == HISTORY_CHUNK_BYTES:
                yield bytes(pending)
                pending.clear()
    if pending:
        yield bytes(pending)


def _fold_line(messages, line):
    try:
        record = json.loads(line)
        if record["op"] == "replace":
            messages.clear()
        elif record["op"] == "upsert":
            message = Msg.model_validate(record["message"])
            messages[message.id] = message
        else:
            raise ValueError("unknown journal operation")
    except (ValueError, KeyError, TypeError) as exc:
        raise AgentKitError("会话历史日志损坏", 503) from exc


class SessionHistoryStorage:
    def __init__(self, engine: AsyncEngine, files: FileStorage):
        self.engine = engine
        self.files = files
        self._cleanup_task = None

    async def read(
        self, user_id: str, session_id: str, *, conn: AsyncConnection | None = None
    ) -> list[Msg]:
        statement = select(
            session_history.c.user_id, session_history.c.committed_bytes
        ).where(session_history.c.session_id == session_id)
        async with self.engine.connect() if conn is None else nullcontext(conn) as db:
            row = (await db.execute(statement)).one_or_none()
            if row is None:
                owner = await db.scalar(
                    select(SessionRow.user_id).where(SessionRow.id == session_id)
                )
                if owner is not None and owner != user_id:
                    raise Forbidden("会话归属不匹配")
                return []
        if row.user_id != user_id:
            raise Forbidden("会话归属不匹配")
        length = row.committed_bytes
        messages = {}
        pending = bytearray()
        for offset in range(0, length, HISTORY_CHUNK_BYTES):
            size = min(HISTORY_CHUNK_BYTES, length - offset)
            data = await asyncio.to_thread(
                self.files.read_history_chunk, user_id, session_id, offset, size
            )
            if len(data) != size:
                raise AgentKitError("会话历史短于已提交长度", 503)
            lines = data.split(b"\n")
            pending.extend(lines[0])
            if len(lines) > 1:
                for line in chain((pending,), lines[1:-1]):
                    _fold_line(messages, line)
                pending = bytearray(lines[-1])
        if pending:
            raise AgentKitError("已提交的会话历史日志不完整", 503)
        return list(messages.values())

    async def upsert(self, user_id, session_id, msg, *, conn=None):
        await self._append(user_id, session_id, _records([msg], replace=False), conn)

    async def replace(self, user_id, session_id, messages, *, conn=None):
        await self._append(user_id, session_id, _records(messages, replace=True), conn)

    async def _append(self, user_id, session_id, records, conn):
        async with self.engine.begin() if conn is None else nullcontext(conn) as db:
            owner = await db.scalar(
                select(SessionRow.user_id)
                .where(SessionRow.id == session_id)
                .with_for_update()
            )
            if owner is None:
                raise NotFound("会话不存在")
            if owner != user_id:
                raise Forbidden("会话归属不匹配")

            # Read length *after* acquiring the lock: a writer we waited for
            # may have just committed a new offset. nextval survives rollback.
            offset_query = select(session_history.c.committed_bytes).where(
                session_history.c.session_id == session_id
            )
            offset, fence = (
                await db.execute(
                    select(
                        offset_query.scalar_subquery(),
                        session_history_write_fence.next_value(),
                    )
                )
            ).one()
            offset = offset or 0
            for chunk in _write_chunks(records):
                await asyncio.to_thread(
                    self.files.append_history_chunk,
                    user_id,
                    session_id,
                    offset,
                    chunk,
                    fence,
                )
                offset += len(chunk)
            save = insert(session_history).values(
                session_id=session_id, user_id=user_id, committed_bytes=offset
            )
            await db.execute(
                save.on_conflict_do_update(
                    index_elements=[session_history.c.session_id],
                    set_={"committed_bytes": offset},
                )
            )

    async def queue_deleted(self, conn, sessions):
        """Reserve a deletion fence after preceding writers, in the owning SQL delete transaction."""
        if not sessions:
            return
        # Lock before nextval(), even for a session whose first append aborted
        # before it could publish metadata. Such a directory can hold raw bytes.
        locked = (
            select(SessionRow.user_id, SessionRow.id)
            .where(tuple_(SessionRow.user_id, SessionRow.id).in_(sessions))
            .order_by(SessionRow.id)
            .with_for_update()
            .cte("deleted_sessions")
        )
        await conn.execute(
            insert(session_history_gc).from_select(
                ["user_id", "session_id", "fence"],
                select(
                    locked.c.user_id,
                    locked.c.id,
                    session_history_write_fence.next_value(),
                ),
            )
        )

    async def sweep_deleted(self, limit=_CLEANUP_BATCH):
        """Finish committed deletions; failed file operations remain available for retry."""
        if limit < 1:
            raise ValueError("cleanup limit must be positive")
        table = session_history_gc
        async with self.engine.begin() as conn:
            pending = (
                await conn.execute(
                    select(table)
                    .where(table.c.retry_at <= func.now())
                    .order_by(table.c.fence)
                    .limit(limit)
                    .with_for_update(skip_locked=True)
                )
            ).all()
            removed = 0
            for row in pending:
                reused = await conn.scalar(
                    select(SessionRow.id).where(
                        SessionRow.id == row.session_id,
                        SessionRow.user_id == row.user_id,
                    )
                )
                if reused is None:
                    try:
                        await asyncio.to_thread(
                            self.files.remove_history,
                            row.user_id,
                            row.session_id,
                            row.fence,
                        )
                    except Exception:
                        logger.exception("NAS session history cleanup will retry")
                        await conn.execute(
                            update(table)
                            .where(table.c.fence == row.fence)
                            .values(
                                retry_at=func.clock_timestamp()
                                + text("interval '60 seconds'")
                            )
                        )
                        continue
                await conn.execute(delete(table).where(table.c.fence == row.fence))
                removed += 1
        return removed

    async def start_cleanup(self):
        if self._cleanup_task is None:
            self._cleanup_task = asyncio.create_task(
                self._cleanup_loop(), name="nas-history-cleanup"
            )

    async def _cleanup_loop(self):
        while True:
            try:
                if await self.sweep_deleted() == _CLEANUP_BATCH:
                    continue
            except Exception:
                logger.exception("NAS session history cleanup sweep failed")
            await asyncio.sleep(60)

    async def aclose(self):
        if self._cleanup_task is not None:
            self._cleanup_task.cancel()
            with suppress(asyncio.CancelledError):
                await self._cleanup_task
            self._cleanup_task = None
