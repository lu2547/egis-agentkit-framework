"""在停写窗口核对或复制旧 PostgreSQL 原始消息到 NAS；默认只核对。"""

import argparse
import asyncio
import json
import sys
from pathlib import Path

from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.storage._sql._tables import MessageRow, SessionRow
from agentscope.message import Msg
from dotenv import load_dotenv
from sqlalchemy import func, select, text

from egis_agentkit.client._postgres import (
    create_postgres_engine_from_config,
    postgres_url,
)
from egis_agentkit.client.nacos import NacosClient, NacosConfigProvider, nacos_bootstrap
from egis_agentkit.storage import AgentKitStorage
from egis_agentkit.storage.model._tables import session_history
from egis_agentkit.storage.project._files import ProjectFileStorage


class ExportError(RuntimeError):
    """Safe diagnostic containing no legacy message data."""


async def _table_exists(conn, name):
    return await conn.scalar(
        text(
            "SELECT to_regclass(format('%I.%I', current_schema(), CAST(:name AS text))) IS NOT NULL"
        ),
        {"name": name},
    )


async def _guard_offline_export(conn):
    # The operator stops API/workers first. Refuse an in-flight source write,
    # and prevent a newly acquired run lease while this offline copy is active.
    await conn.execute(text("LOCK TABLE messages IN SHARE MODE NOWAIT"))
    if await _table_exists(conn, "ak_bus_leases"):
        await conn.execute(text("LOCK TABLE ak_bus_leases IN SHARE MODE NOWAIT"))
        active = await conn.scalar(
            text(
                "SELECT EXISTS (SELECT 1 FROM ak_bus_leases "
                "WHERE starts_with(key, :prefix) AND expires_at > clock_timestamp())"
            ),
            {"prefix": MessageBusKeys.session_lock("")},
        )
        if active:
            raise ExportError(
                "active session lease: stop API/workers and wait for expiry"
            )


def _identical(left, right):
    # Python equality treats False as 0. Preserve JSON types and array order,
    # while accepting differences in how object keys were ordered by a writer.
    def canonical(msg):
        return json.dumps(
            msg.model_dump(mode="json"), sort_keys=True, ensure_ascii=False
        )

    return len(left) == len(right) and all(
        canonical(a) == canonical(b) for a, b in zip(left, right)
    )


async def export_history(storage, *, apply=False):
    counts = dict(
        source_sessions=0,
        source_messages=0,
        orphan_messages=0,
        pending_sessions=0,
        copied_sessions=0,
        identical_sessions=0,
        conflicting_sessions=0,
    )
    async with storage.engine.begin() as conn:
        if not await _table_exists(conn, "messages"):
            return counts
        if apply:
            await _guard_offline_export(conn)
        history = storage.message_history()
        counts["orphan_messages"] = await conn.scalar(
            select(func.count())
            .select_from(MessageRow)
            .outerjoin(SessionRow, SessionRow.id == MessageRow.session_id)
            .where(SessionRow.id.is_(None))
        )
        owned = (
            select(SessionRow.user_id, SessionRow.id)
            .where(
                select(MessageRow.msg_id)
                .where(MessageRow.session_id == SessionRow.id)
                .exists()
            )
            .order_by(SessionRow.id)
        )
        if apply:
            owned = owned.with_for_update(nowait=True)
        # Bound memory to one conversation's messages while using the same
        # transaction for the owner lock, comparison, copy and verification.
        async with conn.stream(owned) as sessions:
            async for user_id, sid in sessions:
                rows = await conn.execute(
                    select(MessageRow.msg_id, MessageRow.payload)
                    .where(MessageRow.session_id == sid)
                    .order_by(MessageRow.created_at, MessageRow.msg_id)
                )
                source = []
                for msg_id, payload in rows:
                    msg = Msg.model_validate(payload)
                    if msg_id != msg.id:
                        raise ExportError("legacy message ID mismatch")
                    source.append(msg)
                counts["source_sessions"] += 1
                counts["source_messages"] += len(source)
                existing = await history.read(user_id, sid, conn=conn)
                if _identical(source, existing):
                    counts["identical_sessions"] += 1
                    continue
                # A committed reset to an empty list is also new NAS history.
                committed = await conn.scalar(
                    select(session_history.c.committed_bytes).where(
                        session_history.c.session_id == sid
                    )
                )
                if committed:
                    counts["conflicting_sessions"] += 1
                    if apply:
                        raise ExportError("NAS history conflict: export rolled back")
                    continue
                if not apply:
                    counts["pending_sessions"] += 1
                    continue
                await history.replace(user_id, sid, source, conn=conn)
                saved = await history.read(user_id, sid, conn=conn)
                if not _identical(source, saved):
                    raise ExportError("NAS verification failed: export rolled back")
                counts["copied_sessions"] += 1
    return counts


async def _run(args):
    load_dotenv(args.env_file)
    async with NacosClient(nacos_bootstrap()) as client:
        provider = NacosConfigProvider(client)
        snapshot = await provider.refresh()
        postgres = snapshot.client.postgres
        files = ProjectFileStorage(
            snapshot.workspace.runtime.root, config_provider=provider
        )
        engine = create_postgres_engine_from_config(postgres)
        try:
            storage = AgentKitStorage(postgres_url(postgres), engine, files=files)
            return await export_history(storage, apply=args.apply)
        finally:
            await engine.dispose()


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--env-file",
        type=Path,
        default=Path(__file__).resolve().parents[4] / "egis-agentkit-app/.env",
        help="App 部署环境文件，默认使用相邻 App 的 .env",
    )
    parser.add_argument(
        "--apply", action="store_true", help="所有 API/Worker 停止后执行复制"
    )
    args = parser.parse_args()
    try:
        counts = asyncio.run(_run(args))
    except ExportError as exc:
        print(str(exc), file=sys.stderr)
        return 1
    except Exception as exc:
        # Validation and driver exceptions may contain entire message payloads.
        print(f"history export failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    print(json.dumps(counts))
    return int(bool(counts["conflicting_sessions"]))


if __name__ == "__main__":
    raise SystemExit(main())
