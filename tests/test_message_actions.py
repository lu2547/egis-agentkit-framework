"""回复反馈和会话分支使用真实原生历史及独立 PostgreSQL schema。"""

from types import SimpleNamespace

import pytest
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from agentscope.message import AssistantMsg, UserMsg
from test_domain import expert, identities

from egis_agentkit.errors import Conflict, Forbidden, InvalidRequest, NotFound
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.session._session import SessionService
from egis_agentkit.storage.model import utcnow


async def history(storage, tmp_path):
    users, admin, alice, bob = await identities(storage)
    definition = await AgentManager(storage).add_definition(alice, expert())
    service = SessionService(storage, tmp_path)
    task = await service.add_session(alice, definition["id"], "private", name="原对话")
    messages = [
        UserMsg("user", "第一个问题"),
        AssistantMsg("writer", "第一轮回答", finished_at=utcnow().isoformat()),
        UserMsg("user", "后续问题"),
        AssistantMsg("writer", "后续回答", finished_at=utcnow().isoformat()),
    ]
    for msg in messages:
        await storage.upsert_message(alice.user_id, task["session_id"], msg)
    record = await service.get_session_record(alice, task["session_id"])
    record.state.context = messages
    record.state.summary = "包含后续答案的摘要，不能带入旧节点分支"
    await storage.update_session_state(
        alice.user_id, record.agent_id, record.id, record.state
    )
    return users, admin, alice, bob, service, task, messages


async def test_branch_copies_only_through_selected_reply_and_is_independent(
    storage, tmp_path
):
    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    async with InMemoryMessageBus() as bus:
        branch = await service.add_session_branch(
            alice, task["session_id"], messages[1].id, bus
        )
    assert branch["session_id"] != task["session_id"]
    assert branch["project_id"] == task["project_id"]
    assert branch["definition_id"] == task["definition_id"]
    assert branch["resource_version"] == task["resource_version"]
    saved, more = await storage.list_messages(alice.user_id, branch["session_id"])
    assert [m.id for m in saved] == [m.id for m in messages[:2]] and not more
    record = await service.get_session_record(alice, branch["session_id"])
    assert [m.id for m in record.state.context] == [m.id for m in messages[:2]]
    assert record.state.summary == ""
    assert record.team_id is None
    assert record.state.middle_context["egis_agentkit"]["branched_from"] == {
        "session_id": task["session_id"],
        "reply_id": messages[1].id,
    }
    await storage.upsert_message(
        alice.user_id, branch["session_id"], UserMsg("user", "分支继续")
    )
    original, _ = await storage.list_messages(alice.user_id, task["session_id"])
    assert [m.id for m in original] == [m.id for m in messages]


async def test_branch_rejects_foreign_missing_and_running_sources(storage, tmp_path):
    _, _, alice, bob, service, task, messages = await history(storage, tmp_path)
    async with InMemoryMessageBus() as bus:
        with pytest.raises(Forbidden):
            await service.add_session_branch(
                bob, task["session_id"], messages[1].id, bus
            )
        with pytest.raises(NotFound):
            await service.add_session_branch(alice, task["session_id"], "missing", bus)
        with pytest.raises(InvalidRequest):
            await service.add_session_branch(
                alice, task["session_id"], messages[0].id, bus
            )
        lock = MessageBusKeys.session_lock(task["session_id"])
        assert await bus.try_lock(lock)
        with pytest.raises(Conflict):
            await service.add_session_branch(
                alice, task["session_id"], messages[1].id, bus
            )
        await bus.unlock(lock)
    assert len(await service.list_sessions(alice)) == 1


async def test_feedback_is_persisted_admin_only_idempotent_and_survives_source_deletion(
    storage, tmp_path
):
    from egis_agentkit.client.nacos._settings import SessionFeedbackConfig
    from egis_agentkit.service._session_feedback import SessionFeedbackService

    users, admin, alice, bob, sessions, task, messages = await history(
        storage, tmp_path
    )
    config = SimpleNamespace(
        snapshot=SimpleNamespace(
            session=SimpleNamespace(feedback=SessionFeedbackConfig(default_page_size=1))
        )
    )
    feedback = SessionFeedbackService(storage, sessions, config)
    with pytest.raises(Forbidden):
        await feedback.save_reply_feedback(
            bob, task["session_id"], messages[1].id, "越权"
        )
    with pytest.raises(NotFound):
        await feedback.save_reply_feedback(
            alice, task["session_id"], "missing", "错误回复"
        )
    with pytest.raises(InvalidRequest):
        await feedback.save_reply_feedback(
            alice, task["session_id"], messages[1].id, "  "
        )
    first = await feedback.save_reply_feedback(
        alice, task["session_id"], messages[1].id, "回答依据有误"
    )
    again = await feedback.save_reply_feedback(
        alice, task["session_id"], messages[1].id, "补充：请检查来源"
    )
    assert first["id"] == again["id"]
    assert (
        again["reply_text"] == "第一轮回答" and again["question_text"] == "第一个问题"
    )
    assert again["user_id"] == "alice" and again["session_name"] == "原对话"
    second = await feedback.save_reply_feedback(
        alice, task["session_id"], messages[3].id, "第二条问题"
    )
    with pytest.raises(Forbidden):
        await feedback.page_reply_feedback(alice)
    page = await feedback.page_reply_feedback(admin)
    assert [item["id"] for item in page["items"]] == [second["id"]]
    older = await feedback.page_reply_feedback(admin, cursor=page["next_cursor"])
    assert [item["id"] for item in older["items"]] == [first["id"]]
    assert older["next_cursor"] is None
    async with InMemoryMessageBus() as bus:
        await sessions.delete_session(alice, task["session_id"], bus)
    assert (await feedback.page_reply_feedback(admin))["items"][0][
        "comment"
    ] == "第二条问题"


async def test_branch_keeps_native_model_and_only_copies_selected_artifact_links(
    storage, tmp_path
):
    from agentscope.app.storage import ChatModelConfig
    from sqlalchemy import insert, select

    from egis_agentkit.storage import tables as t

    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    source = await service.get_session_record(alice, task["session_id"])
    source.config.chat_model_config = ChatModelConfig(
        type="openai_credential",
        credential_id="original-model",
        model="frozen-model",
        parameters={"temperature": 0.2},
    )
    await storage.upsert_session(
        alice.user_id,
        source.agent_id,
        source.config,
        state=source.state,
        session_id=source.id,
    )
    async with storage.engine.begin() as conn:
        await conn.execute(
            insert(t.reply_artifacts),
            [
                {
                    "project_id": task["project_id"],
                    "session_id": source.id,
                    "reply_id": msg.id,
                    "path": f"{index}.svg",
                    "digest": "a" * 64,
                }
                for index, msg in enumerate([messages[1], messages[3]])
            ],
        )
    async with InMemoryMessageBus() as bus:
        branch = await service.add_session_branch(alice, source.id, messages[1].id, bus)
    record = await service.get_session_record(alice, branch["session_id"])
    assert record.config.chat_model_config == source.config.chat_model_config
    async with storage.engine.connect() as conn:
        paths = (
            await conn.scalars(
                select(t.reply_artifacts.c.path).where(
                    t.reply_artifacts.c.session_id == branch["session_id"]
                )
            )
        ).all()
    assert paths == ["0.svg"]


async def test_fixed_team_branch_creates_new_team_and_fresh_member_sessions(
    storage, tmp_path, monkeypatch
):
    from egis_agentkit.service.team._definition import TeamDefinition

    _, _, alice, _ = await identities(storage)
    expert_record = await AgentManager(storage).add_definition(alice, expert())
    definition = await TeamDefinition(storage).save(
        alice,
        {
            "team_name": "branch-team",
            "team_display_name": "分支专家团",
            "orchestrator_config": {
                "agent_name": "coordinator",
                "agent_display_name": "主理人",
                "payload": {},
            },
            "members": [
                {"definition_id": expert_record["id"], "resource_view_type": "private"}
            ],
        },
    )
    service = SessionService(storage, tmp_path)
    task = await service.add_session(alice, definition["id"], "private")
    msg = AssistantMsg("coordinator", "专家团答案", finished_at=utcnow().isoformat())
    await storage.upsert_message(alice.user_id, task["session_id"], msg)
    source = await service.get_session_record(alice, task["session_id"])
    async with InMemoryMessageBus() as bus:
        branch = await service.add_session_branch(alice, source.id, msg.id, bus)
    record = await service.get_session_record(alice, branch["session_id"])
    assert record.team_id and record.team_id != source.team_id
    old_team = await storage.get_team(alice.user_id, source.team_id)
    team = await storage.get_team(alice.user_id, record.team_id)
    assert team.data.members[0].session_id != old_team.data.members[0].session_id
    member = team.data.members[0]
    worker = await storage.get_session(
        alice.user_id, member.agent_id, member.session_id
    )
    assert worker.state.context == []
    assert worker.state.middle_context["egis_agentkit"]["is_initialized"]
    from agentscope.app.storage._sql._tables import TeamRow
    from sqlalchemy import event, select

    def fail_binding(_conn, _cursor, statement, _params, _context, _many):
        if statement.startswith("INSERT INTO teams"):
            raise RuntimeError("team binding failed")

    event.listen(storage.engine.sync_engine, "before_cursor_execute", fail_binding)
    try:
        async with InMemoryMessageBus() as bus:
            with pytest.raises(RuntimeError, match="team binding failed"):
                await service.add_session_branch(alice, source.id, msg.id, bus)
    finally:
        event.remove(storage.engine.sync_engine, "before_cursor_execute", fail_binding)
    async with storage.engine.connect() as conn:
        team_ids = set((await conn.scalars(select(TeamRow.id))).all())
    assert team_ids == {source.team_id, record.team_id}
    surviving = await storage.sessions.in_project(alice.user_id, task["project_id"])
    assert {row.id for row in surviving} == {
        source.id,
        record.id,
        old_team.data.members[0].session_id,
        member.session_id,
    }


async def test_branch_failure_cleans_new_records_and_preserves_source(
    storage, tmp_path, monkeypatch
):
    _, _, alice, _, service, task, messages = await history(storage, tmp_path)

    async def fail(*_):
        raise RuntimeError("copy failed")

    monkeypatch.setattr(storage.sessions, "save_branch_history", fail)
    async with InMemoryMessageBus() as bus:
        with pytest.raises(RuntimeError, match="copy failed"):
            await service.add_session_branch(
                alice, task["session_id"], messages[1].id, bus
            )
    assert [item["session_id"] for item in await service.list_sessions(alice)] == [
        task["session_id"]
    ]


async def test_message_actions_http_enforces_feedback_admin_access(storage, tmp_path):
    import httpx
    from egis_agentkit_app.bootstrap._router import manager_router as router
    from egis_agentkit_app.controller.base import current_user
    from fastapi import FastAPI
    from fastapi.responses import JSONResponse

    from egis_agentkit.errors import AgentKitError
    from egis_agentkit.service._session_feedback import SessionFeedbackService

    _, admin, alice, _, sessions, task, messages = await history(storage, tmp_path)
    app = FastAPI()
    app.include_router(router)
    app.state.egis_services = SimpleNamespace(
        sessions=sessions, session_feedback=SessionFeedbackService(storage, sessions)
    )

    async def domain_error(request, exc):
        return JSONResponse({"detail": str(exc)}, status_code=exc.status_code)

    app.add_exception_handler(AgentKitError, domain_error)
    app.dependency_overrides[current_user] = lambda: alice
    async with InMemoryMessageBus() as bus:
        app.state.egis_bus = bus
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            url = f"/manager/tasks/{task['session_id']}/replies/{messages[1].id}"
            saved = await client.put(url + "/feedback", json={"comment": "口径有误"})
            assert saved.status_code == 200, saved.text
            assert (
                await client.get("/manager/admin/reply-feedback")
            ).status_code == 403
            branch = await client.post(url + "/branch")
            assert branch.status_code == 201, branch.text
            assert branch.json()["session_id"] != task["session_id"]
            app.dependency_overrides[current_user] = lambda: admin
            result = await client.get("/manager/admin/reply-feedback")
            assert (
                result.status_code == 200
                and result.json()["items"][0]["comment"] == "口径有误"
            )


async def test_branch_runtime_uses_final_transcript_and_normalized_attachment_context(
    storage, tmp_path
):
    from agentscope.message import (
        DataBlock,
        TextBlock,
        ToolCallBlock,
        ToolResultBlock,
        URLSource,
    )

    _, _, alice, _, sessions, task, messages = await history(storage, tmp_path)
    user = messages[0].model_copy(
        update={
            "content": [
                TextBlock(text="阅读附件"),
                DataBlock(
                    name="report.pdf",
                    source=URLSource(
                        url="file:///workspace/report.pdf", media_type="application/pdf"
                    ),
                ),
            ]
        }
    )
    reply = messages[1].model_copy(
        update={
            "content": [
                TextBlock(text="处理中"),
                ToolCallBlock(
                    id="old-team-call", name="TeamSay", input="{}", state="finished"
                ),
                ToolResultBlock(
                    id="old-team-call",
                    name="TeamSay",
                    output="完整旧工具输出不能进新模型上下文",
                    state="success",
                ),
                TextBlock(text="最终答案"),
            ]
        }
    )
    await storage.upsert_message(alice.user_id, task["session_id"], user)
    await storage.upsert_message(alice.user_id, task["session_id"], reply)
    source = await sessions.get_session_record(alice, task["session_id"])
    normalized = user.model_copy(
        update={
            "content": [TextBlock(text="阅读附件，文件已保存到 /workspace/report.pdf")]
        }
    )
    source.state.context = [normalized, reply, *messages[2:]]
    await storage.update_session_state(
        alice.user_id, source.agent_id, source.id, source.state
    )
    async with InMemoryMessageBus() as bus:
        branch = await sessions.add_session_branch(alice, source.id, reply.id, bus)
    record = await sessions.get_session_record(alice, branch["session_id"])
    assert (
        record.state.context[0].get_text_content()
        == "阅读附件，文件已保存到 /workspace/report.pdf"
    )
    assert record.state.context[1].get_text_content() == "最终答案"
    assert all(
        block.type == "text" for msg in record.state.context for block in msg.content
    )
    ui, _ = await storage.list_messages(alice.user_id, branch["session_id"])
    assert ui[0].has_content_blocks("data")
    assert ui[1].has_content_blocks("tool_result")


async def test_branch_failure_after_copy_removes_new_artifact_links(
    storage, tmp_path, monkeypatch
):
    from sqlalchemy import insert, select

    from egis_agentkit.storage import tables as t

    _, _, alice, _, sessions, task, messages = await history(storage, tmp_path)
    async with storage.engine.begin() as conn:
        await conn.execute(
            insert(t.reply_artifacts).values(
                project_id=task["project_id"],
                session_id=task["session_id"],
                reply_id=messages[1].id,
                path="report.md",
                digest="a" * 64,
            )
        )
    original_copy = storage.sessions.save_branch_history

    async def fail_after_copy(*args, **kwargs):
        await original_copy(*args, **kwargs)
        raise RuntimeError("metadata failed")

    monkeypatch.setattr(storage.sessions, "save_branch_history", fail_after_copy)
    async with InMemoryMessageBus() as bus:
        with pytest.raises(RuntimeError, match="metadata failed"):
            await sessions.add_session_branch(
                alice, task["session_id"], messages[1].id, bus
            )
    async with storage.engine.connect() as conn:
        session_ids = (await conn.scalars(select(t.reply_artifacts.c.session_id))).all()
    assert session_ids == [task["session_id"]]
