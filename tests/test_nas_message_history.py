import asyncio

import pytest
from agentscope.message import Msg
from sqlalchemy import text
from test_collaboration import setup_task
from test_nas_file_service import loopback_config as loopback_config
from test_nas_file_service import settings as settings

from egis_agentkit.errors import AgentKitError, Conflict, Forbidden
from egis_agentkit.storage.project._files import ProjectFileStorage


def message(id, text):
    return Msg(
        id=id,
        name="用户",
        role="user",
        content=[{"type": "text", "text": text}],
        metadata={"reply_id": "reply-2", "nested": {"original": [1, False, "中"]}},
        structured_output={"notes": ["计划"]},
    )


@pytest.fixture
async def journal(storage, tmp_path):
    from egis_agentkit.storage._session_history import SessionHistoryStorage

    alice, _, _, task = await setup_task(storage, tmp_path)
    sid = task["session_id"]
    files = ProjectFileStorage(tmp_path)
    path = (
        tmp_path / "user" / alice.user_id / "session/history" / sid / "messages.jsonl"
    )
    return SessionHistoryStorage(storage.engine, files), sid, path


async def test_upsert_replays_exact_messages_in_original_order(storage, journal):
    history, sid, path = journal
    first, second = message("first", "原始内容\n" * 1000), message("second", "答复")
    await history.upsert("alice", sid, first)
    prefix = path.read_bytes()
    await history.upsert("alice", sid, second)
    assert path.read_bytes().startswith(prefix)
    assert len(path.read_bytes()) - len(prefix) < len(prefix)
    edited = first.model_copy(update={"metadata": {"edited": True}})
    await history.upsert("alice", sid, edited)
    assert await history.read("alice", sid) == [edited, second]
    async with storage.engine.connect() as conn:
        rows = (
            (await conn.execute(text("SELECT * FROM ak_session_history")))
            .mappings()
            .all()
        )
        assert rows == [
            {
                "session_id": sid,
                "user_id": "alice",
                "committed_bytes": path.stat().st_size,
            }
        ]


async def test_rollback_partial_tail_and_truncated_committed_prefix(storage, journal):
    history, sid, path = journal
    old, new = message("old", "之前"), message("new", "替换")
    await history.upsert("alice", sid, old)
    committed = path.read_bytes()
    async with storage.engine.connect() as conn:
        async with conn.begin() as tx:
            await history.replace("alice", sid, [new], conn=conn)
            assert await history.read("alice", sid, conn=conn) == [new]
            assert await history.read("alice", sid) == [old]
            await tx.rollback()
    with path.open("ab") as stream:
        stream.write(b"incomplete uncommitted tail")
    assert await history.read("alice", sid) == [old]
    await history.upsert("alice", sid, new)
    assert await history.read("alice", sid) == [old, new]
    assert path.read_bytes().startswith(committed)
    assert b"incomplete uncommitted tail" not in path.read_bytes()
    path.write_bytes(committed[:12])
    with pytest.raises(AgentKitError):
        await history.upsert("alice", sid, new)
    with pytest.raises(AgentKitError):
        await history.read("alice", sid)
    assert path.stat().st_size == 12


async def test_concurrent_writers_and_foreign_owner(journal):
    history, sid, _ = journal
    messages = [message(str(i), str(i)) for i in range(12)]
    await asyncio.gather(*(history.upsert("alice", sid, msg) for msg in messages))
    assert sorted(m.id for m in await history.read("alice", sid)) == sorted(
        m.id for m in messages
    )
    for operation in [
        history.read("bob", sid),
        history.replace("bob", sid, []),
        history.upsert("bob", sid, messages[0]),
    ]:
        with pytest.raises(Forbidden):
            await operation
    assert len(await history.read("alice", sid)) == len(messages)


async def test_rpc_chunks_replace_and_reject_stale_writes(
    storage, journal, loopback_config, tmp_path
):
    from egis_agentkit.storage._session_history import SessionHistoryStorage

    _, sid, path = journal
    files = ProjectFileStorage(str(tmp_path), config_provider=loopback_config)
    history = SessionHistoryStorage(storage.engine, files)
    original = message("large", "大消息" * 200_000)
    await history.replace("alice", sid, [original])
    assert await history.read("alice", sid) == [original]
    prefix = path.read_bytes()
    # A delayed RPC from an earlier SQL transaction cannot overwrite a newer commit.
    await asyncio.to_thread(
        files.append_history_chunk, "alice", sid, len(prefix), b"new", 100_000
    )
    with pytest.raises(Conflict):
        await asyncio.to_thread(
            files.append_history_chunk, "alice", sid, 0, b"old", 99_999
        )
    assert path.read_bytes() == prefix + b"new"
    with pytest.raises(Forbidden):
        await asyncio.to_thread(files.read_history_chunk, "../escape", sid, 0, 1)
    with pytest.raises(AgentKitError):
        await asyncio.to_thread(files.read_history_chunk, "alice", sid, 0, 1 << 30)


def test_history_refuses_symlink_in_directory_tree(tmp_path):
    files = ProjectFileStorage(tmp_path)
    directory = tmp_path / "user/alice/session"
    directory.mkdir(parents=True)
    outside = tmp_path / "outside"
    outside.mkdir()
    (directory / "history").symlink_to(outside)
    with pytest.raises(Forbidden):
        files.append_history_chunk("alice", "sid", 0, b"message", 1)
    assert list(outside.iterdir()) == []


async def test_failed_multi_chunk_write_and_commit_leave_previous_history(
    storage, journal, monkeypatch
):
    from sqlalchemy.exc import IntegrityError

    history, sid, path = journal
    old, new = message("old", "before"), message("new", "after")
    await history.upsert("alice", sid, old)
    length = path.stat().st_size
    real_append = history.files.append_history_chunk

    def fail_second_chunk(user_id, session_id, offset, content, fence):
        if offset > length:
            raise OSError("lost NAS connection")
        return real_append(user_id, session_id, offset, content, fence)

    with monkeypatch.context() as patch:
        patch.setattr(history.files, "append_history_chunk", fail_second_chunk)
        with pytest.raises(OSError, match="lost NAS connection"):
            await history.replace("alice", sid, [message("large", "x" * 900_000)])
    assert path.stat().st_size > length
    assert await history.read("alice", sid) == [old]

    # A genuinely failed SQL COMMIT (deferred FK), after a successful NAS append.
    with pytest.raises(IntegrityError):
        async with storage.engine.begin() as conn:
            await conn.execute(
                text(
                    "CREATE TEMP TABLE deferred_failure "
                    "(id INT PRIMARY KEY, parent INT REFERENCES deferred_failure(id) "
                    "DEFERRABLE INITIALLY DEFERRED)"
                )
            )
            await conn.execute(text("INSERT INTO deferred_failure VALUES (1, 2)"))
            await history.replace("alice", sid, [new], conn=conn)
    assert await history.read("alice", sid) == [old]
    await history.replace("alice", sid, [new])
    assert await history.read("alice", sid) == [new]


async def test_gc_rollback_retries_and_fences_old_writes(storage, journal, monkeypatch):
    history, sid, path = journal
    original = message("first", "删除后不可恢复")
    await history.upsert("alice", sid, original)
    old_fence = int.from_bytes((path.parent / ".write-fence").read_bytes(), "big")
    async with storage.engine.begin() as conn:
        await history.queue_deleted(conn, [("alice", sid)])
        await conn.execute(text("DELETE FROM sessions WHERE id=:sid"), {"sid": sid})
        await conn.rollback()
    assert await history.sweep_deleted() == 0
    assert await history.read("alice", sid) == [original]
    async with storage.engine.begin() as conn:
        await history.queue_deleted(conn, [("alice", sid)])
        await conn.execute(text("DELETE FROM sessions WHERE id=:sid"), {"sid": sid})
    with monkeypatch.context() as patch:

        def unavailable(*args):
            raise OSError("unavailable")

        patch.setattr(history.files, "remove_history", unavailable)
        assert await history.sweep_deleted() == 0
        assert path.exists()
    async with storage.engine.begin() as conn:
        assert (
            await conn.scalar(text("SELECT count(*) FROM ak_session_history_gc")) == 1
        )
        await conn.execute(text("UPDATE ak_session_history_gc SET retry_at=now()"))
    assert await history.sweep_deleted() == 1
    assert not path.exists()
    with pytest.raises(Conflict):
        history.files.append_history_chunk("alice", sid, 0, b"delayed", old_fence)
    assert not path.exists()


async def test_gc_preserves_reused_session_and_removes_unpublished_first_write(
    storage, journal
):
    from agentscope.app.storage._sql._tables import SessionRow
    from sqlalchemy import insert, select

    history, sid, path = journal
    async with storage.engine.begin() as conn:
        row = (
            (
                await conn.execute(
                    select(SessionRow.__table__).where(SessionRow.id == sid)
                )
            )
            .mappings()
            .one()
        )
        await history.upsert("alice", sid, message("orphan", "pending"), conn=conn)
        await conn.rollback()
    assert path.exists() and await history.read("alice", sid) == []
    async with storage.engine.begin() as conn:
        await history.queue_deleted(conn, [("alice", sid)])
        await conn.execute(text("DELETE FROM sessions WHERE id=:sid"), {"sid": sid})
    assert await history.sweep_deleted() == 1
    assert not path.exists()

    async with storage.engine.begin() as conn:
        await conn.execute(insert(SessionRow), dict(row))
    await history.upsert("alice", sid, message("old", "old"))
    async with storage.engine.begin() as conn:
        await history.queue_deleted(conn, [("alice", sid)])
        await conn.execute(text("DELETE FROM sessions WHERE id=:sid"), {"sid": sid})
    async with storage.engine.begin() as conn:
        await conn.execute(insert(SessionRow), dict(row))
    new = message("fresh", "new")
    await history.upsert("alice", sid, new)
    assert await history.sweep_deleted() == 1
    assert await history.read("alice", sid) == [new]


async def test_committed_delete_swept_at_startup_through_nas_rpc(
    storage, journal, loopback_config, tmp_path
):
    from egis_agentkit.storage._session_history import SessionHistoryStorage

    _, sid, path = journal
    files = ProjectFileStorage(str(tmp_path), config_provider=loopback_config)
    history = SessionHistoryStorage(storage.engine, files)
    await history.upsert("alice", sid, message("id", "delete"))
    old = int.from_bytes((path.parent / ".write-fence").read_bytes(), "big")
    async with storage.engine.begin() as conn:
        await history.queue_deleted(conn, [("alice", sid)])
        await conn.execute(text("DELETE FROM sessions WHERE id=:sid"), {"sid": sid})
    await history.start_cleanup()
    try:
        async with asyncio.timeout(5):
            while path.exists():
                await asyncio.sleep(0.02)
        with pytest.raises(Conflict):
            await asyncio.to_thread(
                files.append_history_chunk, "alice", sid, 0, b"late", old
            )
        assert not path.exists()
    finally:
        await history.aclose()
