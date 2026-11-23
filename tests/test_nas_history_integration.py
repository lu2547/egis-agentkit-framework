"""原生消息接口改用 NAS，PG 只保存运行状态和会话元数据。"""

from agentscope.app.storage._sql._storage import _utcnow
from agentscope.app.storage._sql._tables import MessageRow
from agentscope.message import UserMsg
from sqlalchemy import insert, text
from test_message_actions import history


async def test_native_history_is_on_nas_and_survives_context_compression(
    storage, tmp_path
):
    _, _, alice, _, tasks, task, messages = await history(storage, tmp_path)
    async with storage.engine.connect() as conn:
        table = await conn.scalar(
            text("SELECT to_regclass(current_schema() || '.messages')")
        )
    assert table is None, "新 schema 不应创建 PostgreSQL messages 表"
    record = await tasks.get_session_record(alice, task["session_id"])
    record.state.context = messages[-1:]
    record.state.summary = "早期对话摘要"
    await storage.update_session_state(
        alice.user_id, record.agent_id, record.id, record.state
    )
    saved, has_more = await storage.list_messages(alice.user_id, record.id)
    assert saved == messages and not has_more
    assert (
        await storage.get_message(alice.user_id, record.id, messages[0].id)
        == messages[0]
    )
    assert list(tmp_path.glob("user/alice/session/history/*/messages.jsonl"))


async def test_history_does_not_read_old_pg_message_rows(storage, tmp_path):
    _, _, alice, _, _, task, messages = await history(storage, tmp_path)
    old = UserMsg("user", "仅旧 PG 表中的消息")
    async with storage.engine.begin() as conn:
        await conn.run_sync(MessageRow.__table__.create)
        await conn.execute(
            insert(MessageRow).values(
                session_id=task["session_id"],
                msg_id=old.id,
                created_at=_utcnow(),
                payload=old.model_dump(mode="json"),
            )
        )
    saved, _ = await storage.list_messages(alice.user_id, task["session_id"])
    assert saved == messages
    assert await storage.get_message(alice.user_id, task["session_id"], old.id) is None
