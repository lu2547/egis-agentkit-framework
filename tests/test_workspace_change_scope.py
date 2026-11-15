"""变更归属与最终产物汇总：并行会话不能互相领取文件。"""

import asyncio
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from agentscope.app.middleware import ToolOffloadMiddleware
from agentscope.event import ReplyEndEvent
from agentscope.message import TextBlock
from agentscope.tool import ToolResponse
from sqlalchemy import select, text
from test_domain import expert, identities
from test_workspace_service import make_workspace

from runtime_helpers import file_middleware
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.storage.model import _tables as tables


@pytest.mark.asyncio
async def test_parallel_sessions_record_scoped_changes_and_collect_only_at_reply_end(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentManager(storage).add_definition(alice, expert())
    one = await svc.tasks.add_session(alice, definition["id"], "private")
    two = await svc.tasks.add_session(
        alice, definition["id"], "private", project_id=one["project_id"]
    )
    pid = one["project_id"]
    svc.files.write("alice", pid, "unrelated.md", b"old", area="artifacts")
    first_entered, second_waiting = asyncio.Event(), asyncio.Event()

    async def first():
        async with svc.changes.track_changes(
            "alice", pid, one["session_id"], "reply-one"
        ):
            first_entered.set()
            await second_waiting.wait()
            svc.files.write("alice", pid, "one.pptx", b"v1", area="artifacts")
            svc.files.write("alice", pid, "one.svg", b"svg", area="artifacts")

    async def second():
        await first_entered.wait()
        second_waiting.set()
        async with svc.changes.track_changes(
            "alice", pid, two["session_id"], "reply-two"
        ):
            svc.files.write("alice", pid, "two.pptx", b"two", area="artifacts")

    await asyncio.gather(first(), second())
    # 普通页面扫描不能覆盖已经记录的会话归属。
    await svc.changes.observe("alice", pid)
    async with storage.engine.connect() as conn:
        rows = (
            (
                await conn.execute(
                    select(tables.file_changes).where(
                        tables.file_changes.c.project_id == pid
                    )
                )
            )
            .mappings()
            .all()
        )
    owned = {
        (r["path"], r["session_id"], r["reply_id"]) for r in rows if r["session_id"]
    }
    assert owned == {
        ("one.pptx", one["session_id"], "reply-one"),
        ("one.svg", one["session_id"], "reply-one"),
        ("two.pptx", two["session_id"], "reply-two"),
    }
    assert (await svc.workspace.session_artifacts(alice, one["session_id"]))[
        "items"
    ] == []
    async with svc.changes.track_changes("alice", pid, one["session_id"], "reply-one"):
        svc.files.write(
            "alice", pid, "one.pptx", b"v2", area="artifacts", overwrite=True
        )
        svc.files.write("alice", pid, "gone.md", b"gone", area="artifacts")
    async with svc.changes.track_changes("alice", pid, one["session_id"], "reply-one"):
        svc.files.remove("alice", pid, "artifacts", "gone.md")
    await svc.changes.finalize_reply("alice", pid, one["session_id"], "reply-one")
    await svc.changes.finalize_reply("alice", pid, one["session_id"], "reply-one")
    items = (await svc.workspace.session_artifacts(alice, one["session_id"]))["items"]
    assert {(r["path"], r["reply_id"]) for r in items} == {
        ("one.pptx", "reply-one"),
        ("one.svg", "reply-one"),
    }
    assert (await svc.workspace.session_artifacts(alice, two["session_id"]))[
        "items"
    ] == []
    await svc.changes.finalize_reply("alice", pid, two["session_id"], "reply-two")
    assert [
        r["path"]
        for r in (await svc.workspace.session_artifacts(alice, two["session_id"]))[
            "items"
        ]
    ] == ["two.pptx"]


@pytest.mark.asyncio
async def test_native_tool_offload_cannot_cancel_change_recording_after_terminal_result():
    """原生卸载层取消尾部任务时，文件变更归属必须已经写入。"""
    completed = []

    class Tracker:
        async def observe_safe(self, *args, **kwargs):
            pass

        @asynccontextmanager
        async def track_changes(self, *args):
            try:
                yield
            finally:
                await asyncio.sleep(0.02)
                completed.append(args)

    class Toolkit:
        async def get_tool(self, name):
            return SimpleNamespace(is_state_injected=False, is_external_tool=False)

    agent = SimpleNamespace(
        state=SimpleNamespace(reply_id="reply", session_id="session"), toolkit=Toolkit()
    )
    scope = SimpleNamespace(user_id="alice", project_id="project", session_id="session")
    tracker = Tracker()
    middleware = file_middleware(tracker, scope)

    async def tool(**kwargs):
        yield ToolResponse(content=[TextBlock(text="done")])

    async def tracked(**kwargs):
        async for item in middleware.on_acting(agent, kwargs, tool):
            yield item

    offload = ToolOffloadMiddleware(None, None, "alice", "agent")
    result = [
        item
        async for item in offload.on_acting(
            agent, {"tool_call": SimpleNamespace(name="Bash")}, tracked
        )
    ]
    assert result[0].content[0].text == "done"
    assert completed == [("alice", "project", "session", "reply")]


@pytest.mark.asyncio
async def test_final_collection_finishes_before_reply_end_is_visible():
    observed = []

    class Tracker:
        async def observe_safe(self, *args, **kwargs):
            pass

        async def finalize_reply(self, *args):
            observed.append(args)

    scope = SimpleNamespace(user_id="alice", project_id="project", session_id="session")
    middleware = file_middleware(Tracker(), scope)

    async def reply(**kwargs):
        yield ReplyEndEvent(reply_id="reply", session_id="session")

    async for event in middleware.on_reply(None, {}, reply):
        assert event.reply_id == "reply"
        assert observed == [("alice", "project", "session", "reply")]


@pytest.mark.asyncio
async def test_explicit_upgrade_adds_change_ownership_without_guessing_old_rows(
    storage,
):
    from egis_agentkit.storage._schema import upgrade_schema

    async with storage.engine.begin() as conn:
        await conn.execute(
            text(
                "ALTER TABLE ak_project_file_changes DROP COLUMN IF EXISTS session_id CASCADE"
            )
        )
        await conn.execute(
            text(
                "ALTER TABLE ak_project_file_changes DROP COLUMN IF EXISTS reply_id CASCADE"
            )
        )
        await conn.execute(
            text(
                "INSERT INTO ak_project_file_changes(project_id,area,path,change_type) VALUES('project','artifacts','old.md','created')"
            )
        )
    with pytest.raises(ValueError, match="upgrade-db"):
        await storage.init_schema()
    preview = await upgrade_schema(storage.engine)
    assert preview.tables["工作区变更归属"] == "pending"
    applied = await upgrade_schema(
        storage.engine, database=preview.database, apply=True
    )
    assert applied.tables["工作区变更归属"] == "upgraded"
    async with storage.engine.connect() as conn:
        row = (
            await conn.execute(
                text(
                    "SELECT session_id,reply_id FROM ak_project_file_changes WHERE path='old.md'"
                )
            )
        ).one()
    assert row == (None, None)
    assert (await upgrade_schema(storage.engine)).tables["工作区变更归属"] == "current"


@pytest.mark.asyncio
@pytest.mark.parametrize("writer", ["other-session", "other-reply", "manual"])
async def test_late_finalization_does_not_claim_a_newer_writers_file(
    storage, tmp_path, writer
):
    _, _, alice, _ = await identities(storage)
    svc = make_workspace(storage, tmp_path)
    definition = await AgentManager(storage).add_definition(alice, expert())
    one = await svc.tasks.add_session(alice, definition["id"], "private")
    two = await svc.tasks.add_session(
        alice, definition["id"], "private", project_id=one["project_id"]
    )
    pid, sid = one["project_id"], one["session_id"]
    async with svc.changes.track_changes("alice", pid, sid, "first"):
        svc.files.write("alice", pid, "shared.md", b"first", area="artifacts")
    if writer == "manual":
        svc.files.write(
            "alice", pid, "shared.md", b"newer", area="artifacts", overwrite=True
        )
        await svc.changes.observe("alice", pid)
    else:
        writer_sid = two["session_id"] if writer == "other-session" else sid
        async with svc.changes.track_changes("alice", pid, writer_sid, "second"):
            svc.files.remove("alice", pid, "artifacts", "shared.md")
            svc.files.write("alice", pid, "shared.md", b"newer", area="artifacts")
    await svc.changes.finalize_reply("alice", pid, sid, "first")
    assert (await svc.workspace.session_artifacts(alice, sid))["items"] == []
    if writer != "manual":
        await svc.changes.finalize_reply("alice", pid, writer_sid, "second")
        assert [
            r["reply_id"]
            for r in (await svc.workspace.session_artifacts(alice, writer_sid))["items"]
        ] == ["second"]
