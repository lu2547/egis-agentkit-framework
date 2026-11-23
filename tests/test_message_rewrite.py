"""编辑重发只回退会话；物理产物及其记录保持原样。"""

import time
from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest
from agentscope.app._bus_ops import enqueue_chat_input, list_chat_inputs
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from agentscope.message import ToolCallBlock, UserMsg
from pydantic import SecretStr
from test_message_actions import history
from test_session_steering import chat_api  # noqa: F401

from egis_agentkit.errors import Conflict, Forbidden, InvalidRequest, NotFound


async def replace(service, user, task, message, edited, bus):
    async with service.rewrite_message(
        user, task["session_id"], task["agent_id"], message.id, bus
    ) as receipt:
        item = await enqueue_chat_input(
            bus,
            user_id=user.user_id,
            session_id=task["session_id"],
            agent_id=task["agent_id"],
            inputs=edited,
        )
        receipt.queue_item_id = item["id"]
        receipt.replacement = edited


async def test_edit_truncates_history_and_model_context_but_keeps_files(
    storage, tmp_path
):
    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    service.files.write(
        "alice", task["project_id"], "report.md", b"do not roll back", area="artifacts"
    )
    original = messages[2]
    edited = original.model_copy(deep=True)
    edited.content = UserMsg("alice", "改后的问题").content
    async with InMemoryMessageBus() as bus:
        await bus.log_append(
            MessageBusKeys.session_events(task["session_id"]), {"old": "event"}
        )
        await replace(service, alice, task, original, edited, bus)
        saved, more = await storage.list_messages("alice", task["session_id"])
        assert not more
        assert [m.id for m in saved] == [m.id for m in messages[:2]]
        (queued,) = await list_chat_inputs(bus, task["session_id"])
        assert queued["input"]["id"] == original.id
        assert queued["input"]["content"][0]["text"] == "改后的问题"
        ((_, event),) = await bus.log_read(
            MessageBusKeys.session_events(task["session_id"])
        )
        assert event["name"] == "chat_history_rewritten"
        assert event["value"]["retained_message_ids"] == [m.id for m in messages[:2]]
        assert event["value"]["input"]["id"] == original.id
    record = await service.get_session_record(alice, task["session_id"])
    assert [m.id for m in record.state.context] == [m.id for m in messages[:2]]
    assert record.state.summary == ""
    assert record.state.reply_context.reply_id not in [m.id for m in messages]
    assert (
        service.files.read("alice", task["project_id"], "artifacts", "report.md")[0]
        == b"do not roll back"
    )


async def test_edit_of_first_message_keeps_session_and_definition(storage, tmp_path):
    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    before = await service.get_session_record(alice, task["session_id"])
    async with InMemoryMessageBus() as bus:
        await replace(service, alice, task, messages[0], messages[0], bus)
    saved, _ = await storage.list_messages("alice", task["session_id"])
    assert saved == []
    after = await service.get_session_record(alice, task["session_id"])
    assert after.id == before.id and after.agent_id == before.agent_id
    assert after.config == before.config
    assert after.state.permission_context == before.state.permission_context
    assert (
        after.state.middle_context["egis_agentkit"]
        == before.state.middle_context["egis_agentkit"]
    )


async def test_edit_rejects_foreign_missing_and_assistant_messages_without_mutation(
    storage, tmp_path
):
    _, _, alice, bob, service, task, messages = await history(storage, tmp_path)
    async with InMemoryMessageBus() as bus:
        for actor, message_id, aid, error in [
            (bob, messages[0].id, task["agent_id"], Forbidden),
            (alice, "missing", task["agent_id"], NotFound),
            (alice, messages[1].id, task["agent_id"], InvalidRequest),
            (alice, messages[0].id, "wrong-agent", Forbidden),
        ]:
            with pytest.raises(error):
                async with service.rewrite_message(
                    actor, task["session_id"], aid, message_id, bus
                ):
                    pytest.fail("invalid target must be rejected before mutation")
    saved, _ = await storage.list_messages("alice", task["session_id"])
    assert [m.id for m in saved] == [m.id for m in messages]


@pytest.mark.parametrize("busy", ["running", "queued", "claimed", "background"])
async def test_edit_rejects_unfinished_work_without_mutation(storage, tmp_path, busy):
    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    sid = task["session_id"]
    async with InMemoryMessageBus() as bus:
        if busy == "running":
            assert await bus.try_lock(MessageBusKeys.session_lock(sid))
        elif busy == "queued":
            await enqueue_chat_input(
                bus,
                user_id="alice",
                session_id=sid,
                agent_id=task["agent_id"],
                inputs=UserMsg("alice", "待执行"),
            )
        elif busy == "claimed":
            await bus.registry_set(
                MessageBusKeys.chat_input_inflight_registry(), sid, "{}"
            )
        else:
            await bus.registry_set(MessageBusKeys.bg_tasks(sid), "background", "{}")
        with pytest.raises(Conflict):
            await replace(service, alice, task, messages[0], messages[0], bus)
    saved, _ = await storage.list_messages("alice", sid)
    assert [m.id for m in saved] == [m.id for m in messages]


async def test_enqueue_failure_rolls_back_history_and_runtime_state(storage, tmp_path):
    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    before = await service.get_session_record(alice, task["session_id"])
    async with InMemoryMessageBus() as bus:
        sid = task["session_id"]
        await bus.queue_push(MessageBusKeys.inbox(sid), {"message": "保留未消费报告"})
        await bus.log_append(MessageBusKeys.session_events(sid), {"old": "event"})
        await bus.registry_set(
            MessageBusKeys.projection_namespace(sid), "progress", "keep"
        )
        previous_inbox = await bus.queue_read(MessageBusKeys.inbox(sid))
        previous_events = await bus.log_read(MessageBusKeys.session_events(sid))
        with pytest.raises(RuntimeError, match="enqueue failed"):
            async with service.rewrite_message(
                alice, task["session_id"], task["agent_id"], messages[2].id, bus
            ):
                raise RuntimeError("enqueue failed")
        assert not await bus.is_locked(MessageBusKeys.session_lock(task["session_id"]))
        assert await bus.queue_read(MessageBusKeys.inbox(sid)) == previous_inbox
        assert await bus.log_read(MessageBusKeys.session_events(sid)) == previous_events
        assert (
            await bus.registry_get(MessageBusKeys.projection_namespace(sid), "progress")
            == "keep"
        )
    saved, _ = await storage.list_messages("alice", task["session_id"])
    after = await service.get_session_record(alice, task["session_id"])
    assert [m.id for m in saved] == [m.id for m in messages]
    assert after.state == before.state


async def test_successful_edit_retains_verified_authorization(storage, tmp_path):
    from egis_agentkit.core.session._context import VerifiedChatAuthorization
    from egis_agentkit.service.session._authorization import ChatAuthorizationStore
    from egis_agentkit.service.session._message_bus_override import SessionMessageBus

    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    async with SessionMessageBus(deployment_mode="standalone") as bus:
        authorizations = ChatAuthorizationStore(bus)
        await authorizations.remember(
            VerifiedChatAuthorization(
                "alice",
                task["agent_id"],
                task["session_id"],
                SecretStr("verified-test-token"),
                expires_at=time.time() + 300,
            )
        )
        await replace(service, alice, task, messages[0], messages[0], bus)
        restored = await authorizations.load(
            "alice", task["agent_id"], task["session_id"]
        )
        assert restored is not None
        assert restored.access_token.get_secret_value() == "verified-test-token"


@pytest.mark.parametrize("state", ["asking", "submitted"])
async def test_parked_tool_reply_is_not_editable(storage, tmp_path, state):
    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    record = await service.get_session_record(alice, task["session_id"])
    record.state.context[-1].content = [
        ToolCallBlock(id="parked", name="Bash", input="{}", state=state)
    ]
    record.state.reply_id = messages[-1].id
    await storage.update_session_state(
        "alice", task["agent_id"], task["session_id"], record.state
    )
    async with InMemoryMessageBus() as bus:
        with pytest.raises(Conflict):
            await replace(service, alice, task, messages[0], messages[0], bus)
    saved, _ = await storage.list_messages("alice", task["session_id"])
    assert [message.id for message in saved] == [message.id for message in messages]


async def test_http_replaces_a_message_and_queues_preserved_input(chat_api, storage):
    client, bus, task, alice, _ = chat_api
    messages = [UserMsg("alice", "原问题"), UserMsg("alice", "后续问题")]
    for message in messages:
        await storage.upsert_message("alice", task["session_id"], message)
    edited = messages[0].model_copy(deep=True)
    edited.content = UserMsg("spoofed-name", "新问题").content
    edited.metadata = {"preview_prompt": "新问题", "references": [{"keep": True}]}
    response = await client.post(
        "/chat/",
        json={
            "session_id": task["session_id"],
            "agent_id": task["agent_id"],
            "replace_message_id": edited.id,
            "input": edited.model_dump(mode="json"),
        },
    )
    assert response.status_code == 200, response.text
    assert response.json()["status"] == "queued"
    assert response.json()["queue_item_id"]
    assert (await storage.list_messages("alice", task["session_id"]))[0] == []
    (queued,) = await list_chat_inputs(bus, task["session_id"])
    assert queued["input"][0]["name"] == alice.username
    assert queued["input"][0]["metadata"] == edited.metadata


async def test_http_rejects_wrong_replacement_id_batch_and_foreign_owner(
    chat_api, storage
):
    client, bus, task, _, _ = chat_api
    original = UserMsg("alice", "原问题")
    await storage.upsert_message("alice", task["session_id"], original)
    target = {
        "session_id": task["session_id"],
        "agent_id": task["agent_id"],
        "replace_message_id": original.id,
    }
    for input_value in [
        None,
        UserMsg("alice", "不同 ID").model_dump(mode="json"),
        [
            original.model_dump(mode="json"),
            UserMsg("alice", "第二条").model_dump(mode="json"),
        ],
    ]:
        response = await client.post("/chat/", json={**target, "input": input_value})
        assert response.status_code == 422, response.text
    response = await client.post(
        "/chat/",
        json={**target, "input": original.model_dump(mode="json")},
        headers={"Authorization": "Bearer bob"},
    )
    assert response.status_code == 403
    saved, _ = await storage.list_messages("alice", task["session_id"])
    assert [message.id for message in saved] == [original.id]
    assert await list_chat_inputs(bus, task["session_id"]) == []


async def test_http_queue_quota_rejection_keeps_original_bus_and_history(
    chat_api, storage, monkeypatch
):
    client, bus, task, _, _ = chat_api
    sid = task["session_id"]
    original = UserMsg("alice", "原问题")
    await storage.upsert_message("alice", sid, original)
    await bus.queue_push(MessageBusKeys.inbox(sid), {"report": "不得因拒绝重发而丢失"})
    await bus.log_append(MessageBusKeys.session_events(sid), {"old": "event"})
    await bus.registry_set(MessageBusKeys.projection_namespace(sid), "progress", "keep")
    before_events = await bus.log_read(MessageBusKeys.session_events(sid))
    before_inbox = await bus.queue_read(MessageBusKeys.inbox(sid))
    monkeypatch.setattr(MessageBusKeys, "CHAT_INPUT_USER_MAX_LEN", 1)
    await enqueue_chat_input(
        bus,
        "alice",
        "other-session",
        "other-agent",
        UserMsg("alice", "另一对话的排队消息"),
    )
    response = await client.post(
        "/chat/",
        json={
            "session_id": sid,
            "agent_id": task["agent_id"],
            "replace_message_id": original.id,
            "input": original.model_dump(mode="json"),
        },
    )
    assert response.status_code == 429, response.text
    assert await bus.log_read(MessageBusKeys.session_events(sid)) == before_events
    assert await bus.queue_read(MessageBusKeys.inbox(sid)) == before_inbox
    assert (
        await bus.registry_get(MessageBusKeys.projection_namespace(sid), "progress")
        == "keep"
    )
    assert [
        message.id for message in (await storage.list_messages("alice", sid))[0]
    ] == [original.id]


async def test_database_failure_removes_accepted_replacement_and_restores_history(
    storage, tmp_path, monkeypatch
):
    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    engine = storage.sessions.engine

    @asynccontextmanager
    async def failing_transaction():
        async with engine.begin() as conn:
            yield conn
            raise RuntimeError("commit failed")

    monkeypatch.setattr(
        storage.sessions,
        "engine",
        SimpleNamespace(
            begin=failing_transaction,
            connect=engine.connect,
        ),
    )
    async with InMemoryMessageBus() as bus:
        with pytest.raises(RuntimeError, match="commit failed"):
            await replace(service, alice, task, messages[2], messages[2], bus)
        assert await list_chat_inputs(bus, task["session_id"]) == []
    saved, _ = await storage.list_messages("alice", task["session_id"])
    assert [message.id for message in saved] == [message.id for message in messages]


@pytest.mark.parametrize("worker_running", [False, True, "parked"])
async def test_edit_resets_invited_worker_context_and_refuses_running_worker(
    storage, tmp_path, worker_running
):
    from agentscope.app._tool._team_create import TeamCreate
    from test_collaboration import ref, setup_task

    from egis_agentkit.service.access._permission import PermissionService
    from egis_agentkit.service.session._preparation import SessionPreparationService
    from egis_agentkit.service.team._collaboration import TeamCollaborationService
    from egis_agentkit.service.team._invitation import apply_invitations

    alice, writer, tasks, task = await setup_task(storage, tmp_path)
    original = UserMsg("alice", "原问题")
    async with InMemoryMessageBus() as bus:
        await TeamCollaborationService.stage_collaborators(
            tasks, alice, task["session_id"], bus, "request", [ref(writer)]
        )
        async with SessionPreparationService(storage, tmp_path).bind(
            task["session_id"]
        ) as scope:
            await apply_invitations(
                alice,
                scope,
                scope.session_record.state,
                TeamCreate(
                    storage,
                    bus,
                    SimpleNamespace(),
                    "alice",
                    task["session_id"],
                    task["agent_id"],
                ),
                original,
                PermissionService(storage),
            )
            await storage.update_session_state(
                "alice",
                task["agent_id"],
                task["session_id"],
                scope.session_record.state,
            )
            team_id = scope.session_record.team_id
        team = await storage.get_team("alice", team_id)
        (member,) = team.data.members
        worker = await storage.get_session("alice", member.agent_id, member.session_id)
        stale = UserMsg("alice", "后续的专家任务")
        worker.state.context = [stale]
        worker.state.summary = "后续摘要"
        if worker_running == "parked":
            from agentscope.message import AssistantMsg

            parked = AssistantMsg(
                "writer",
                [
                    ToolCallBlock(
                        id="parked-worker", name="Bash", input="{}", state="asking"
                    )
                ],
            )
            worker.state.context.append(parked)
            worker.state.reply_id = parked.id
        await storage.update_session_state(
            "alice", worker.agent_id, worker.id, worker.state
        )
        await storage.upsert_message("alice", worker.id, stale)
        await storage.upsert_message("alice", task["session_id"], original)
        if worker_running:
            if worker_running is True:
                assert await bus.try_lock(MessageBusKeys.session_lock(worker.id))
            with pytest.raises(Conflict):
                await replace(tasks, alice, task, original, original, bus)
            assert (await storage.list_messages("alice", worker.id))[0]
            assert (await storage.list_messages("alice", task["session_id"]))[0]
        else:
            await replace(tasks, alice, task, original, original, bus)
            worker_after = await storage.get_session(
                "alice", worker.agent_id, worker.id
            )
            assert worker_after.state.context == []
            assert worker_after.state.summary == ""
            assert (await storage.list_messages("alice", worker.id))[0] == []
            assert await bus.queue_read(MessageBusKeys.inbox(worker.id)) == []
            parent = await tasks.get_session_record(alice, task["session_id"])
            assert parent.team_id == team_id
            assert (
                await storage.get_team("alice", team_id)
            ).data.members == team.data.members


@pytest.mark.parametrize("failure", [None, "enqueue", "commit"])
async def test_cluster_rewrite_cleans_only_committed_runtime_and_retains_credentials(
    storage, tmp_path, monkeypatch, failure
):
    from unittest.mock import AsyncMock

    from egis_agentkit.core.message_bus import EgisMessageBus
    from egis_agentkit.core.session._context import VerifiedChatAuthorization
    from egis_agentkit.service.session._authorization import ChatAuthorizationStore

    _, _, alice, _, service, task, messages = await history(storage, tmp_path)
    sid = task["session_id"]
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus.init_schema()
    monkeypatch.setattr(bus, "publish", AsyncMock())
    original_push = bus.queue_push

    async def push(key, payload, *, ttl_secs=None):
        if key == MessageBusKeys.wakeup_queue():
            return "test-wakeup"
        return await original_push(key, payload, ttl_secs=ttl_secs)

    monkeypatch.setattr(bus, "queue_push", push)
    store = ChatAuthorizationStore(bus)
    await store.remember(
        VerifiedChatAuthorization(
            "alice",
            task["agent_id"],
            sid,
            SecretStr("verified-test-token"),
            expires_at=time.time() + 300,
        )
    )
    await bus.queue_push(MessageBusKeys.inbox(sid), {"report": "保留旧报告直到提交"})
    await bus.log_append(MessageBusKeys.session_events(sid), {"old": "event"})
    await bus.registry_set(
        MessageBusKeys.projection_namespace(sid), "progress", "old-progress"
    )
    before_inbox = await bus.queue_read(MessageBusKeys.inbox(sid))
    before_events = await bus.log_read(MessageBusKeys.session_events(sid))
    if failure == "commit":
        engine = storage.sessions.engine

        @asynccontextmanager
        async def failing_transaction():
            async with engine.begin() as conn:
                yield conn
                raise RuntimeError("commit failed")

        monkeypatch.setattr(
            storage.sessions,
            "engine",
            SimpleNamespace(begin=failing_transaction, connect=engine.connect),
        )
    try:
        if failure:
            with pytest.raises(RuntimeError):
                async with service.rewrite_message(
                    alice, sid, task["agent_id"], messages[2].id, bus
                ) as receipt:
                    if failure == "enqueue":
                        raise RuntimeError("enqueue failed")
                    item = await enqueue_chat_input(
                        bus, "alice", sid, task["agent_id"], messages[2]
                    )
                    receipt.queue_item_id = item["id"]
                    receipt.replacement = messages[2]
            assert await bus.queue_read(MessageBusKeys.inbox(sid)) == before_inbox
            assert (
                await bus.log_read(MessageBusKeys.session_events(sid)) == before_events
            )
            assert (
                await bus.registry_get(
                    MessageBusKeys.projection_namespace(sid), "progress"
                )
                == "old-progress"
            )
            assert await list_chat_inputs(bus, sid) == []
            assert [m.id for m in (await storage.list_messages("alice", sid))[0]] == [
                m.id for m in messages
            ]
        else:
            await replace(service, alice, task, messages[2], messages[2], bus)
            assert await bus.queue_read(MessageBusKeys.inbox(sid)) == []
            assert (
                await bus.registry_get(
                    MessageBusKeys.projection_namespace(sid), "progress"
                )
                is None
            )
            assert len(await list_chat_inputs(bus, sid)) == 1
            assert [
                event["name"]
                for _, event in await bus.log_read(MessageBusKeys.session_events(sid))
            ] == ["chat_history_rewritten"]
        assert (
            await store.load("alice", task["agent_id"], sid)
        ).access_token.get_secret_value() == "verified-test-token"
    finally:
        await bus.aclose()
