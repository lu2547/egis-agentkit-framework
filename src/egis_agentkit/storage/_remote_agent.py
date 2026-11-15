"""Persist remote runs and arbitrate concurrent provider callbacks."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Literal

from sqlalchemy import Boolean, Column, DateTime, String, Table, select, update
from sqlalchemy.dialects.postgresql import JSONB, insert
from sqlalchemy.engine import RowMapping
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.errors import Conflict
from egis_agentkit.storage.model.agent._remote_config import RemoteAgentConfig

from .model._remote_agent import RemoteRunRequest, RemoteRunResult
from .model._tables import metadata

remote_agent_runs = Table(
    "ak_agent_remote_runs",
    metadata,
    Column("run_id", String(64), primary_key=True),
    Column("conversation_id", String(128), nullable=False, index=True),
    Column("connection_id", String(128), nullable=False),
    Column("connection_config", JSONB, nullable=False),
    Column("request_digest", String(64), nullable=False),
    Column("deadline", DateTime(timezone=True), nullable=False),
    Column("is_submitted", Boolean, nullable=False, server_default="false"),
    Column("result", JSONB(none_as_null=True)),
    Column("reported_at", DateTime(timezone=True)),
)


class RemoteRunStorage:
    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def add_request(
        self,
        request: RemoteRunRequest,
        connection_id: str,
        config: RemoteAgentConfig,
        request_digest: str,
    ) -> RowMapping:
        run_id = request.run_id
        async with self.engine.begin() as conn:
            await conn.execute(
                insert(remote_agent_runs)
                .values(
                    run_id=run_id,
                    conversation_id=request.conversation_id,
                    connection_id=connection_id,
                    connection_config=config.model_dump(mode="json"),
                    request_digest=request_digest,
                    deadline=datetime.now(timezone.utc)
                    + timedelta(seconds=config.timeout_seconds),
                )
                .on_conflict_do_nothing(index_elements=["run_id"])
            )
            row = await self._get_request(conn, run_id)
            if (
                row["connection_id"] != connection_id
                or row["request_digest"] != request_digest
            ):
                raise Conflict("远程任务标识已用于其他请求")
            return row

    async def get_request(self, run_id: str) -> RowMapping | None:
        async with self.engine.connect() as conn:
            return await self._get_request(conn, run_id)

    async def _get_request(
        self, conn: AsyncConnection, run_id: str, *, for_update: bool = False
    ) -> RowMapping | None:
        query = select(remote_agent_runs).where(remote_agent_runs.c.run_id == run_id)
        if for_update:
            query = query.with_for_update()
        return (await conn.execute(query)).mappings().one_or_none()

    async def update_submitted(self, run_id: str) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                update(remote_agent_runs)
                .where(remote_agent_runs.c.run_id == run_id)
                .values(is_submitted=True)
            )

    async def save_result(
        self, result: RemoteRunResult
    ) -> tuple[
        Literal["missing", "duplicate", "conflict", "accepted", "expired"],
        RemoteRunResult | None,
    ]:
        result_fields = result.model_dump(mode="json", exclude_none=True)
        async with self.engine.begin() as conn:
            row = await self._get_request(conn, result.run_id, for_update=True)
            if row is None:
                return "missing", None
            if row["result"] is not None:
                outcome = "duplicate" if row["result"] == result_fields else "conflict"
                return outcome, RemoteRunResult.model_validate(row["result"])
            outcome = "accepted"
            if (
                result.status != "timed_out"
                and datetime.now(timezone.utc) >= row["deadline"]
            ):
                outcome = "expired"
                result = RemoteRunResult(
                    run_id=result.run_id,
                    status="timed_out",
                    error={"code": "timeout", "message": "远程专家未在配置时限内返回"},
                )
                result_fields = result.model_dump(mode="json", exclude_none=True)
            await conn.execute(
                update(remote_agent_runs)
                .where(remote_agent_runs.c.run_id == result.run_id)
                .values(result=result_fields)
            )
            return outcome, result

    async def update_reported(self, run_id: str) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(
                update(remote_agent_runs)
                .where(
                    remote_agent_runs.c.run_id == run_id,
                    remote_agent_runs.c.reported_at.is_(None),
                    remote_agent_runs.c.result["status"].astext == "completed",
                )
                .values(reported_at=datetime.now(timezone.utc))
            )
