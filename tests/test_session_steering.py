"""企业鉴权后的原生持久队列与非中断引导。"""

from types import SimpleNamespace
from unittest.mock import AsyncMock
import time

import httpx
import pytest
import pytest_asyncio
from agentscope.app._bus_ops import (
    enqueue_chat_input,
    finish_active_chat_reply,
    list_chat_inputs,
    register_active_chat_reply,
    steer_chat_input,
)
from agentscope.app.deps import (
    get_chat_run_registry,
    get_chat_service,
    get_message_bus,
    get_storage,
)
from agentscope.app.message_bus import MessageBusKeys
from agentscope.app.middleware import SteeringMiddleware
from agentscope.message import AssistantMsg, UserMsg
from egis_agentkit_app.controller.base import register_exception_handlers
from egis_agentkit_app.controller.chat import router
from fastapi import FastAPI
from test_app_http import app_snapshot
from test_domain import expert, identities

from egis_agentkit.client.nacos._settings import SessionSteeringConfig
from egis_agentkit.core.message_bus import EgisMessageBus
from egis_agentkit.core.session._chat_input import prepare_chat_inputs
from egis_agentkit.errors import InvalidRequest
from egis_agentkit.middleware.http._http_identity_middleware_impl import IdentityGuard
from egis_agentkit.service._user import UserService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.project._project_manager import purge_session_bus
from egis_agentkit.service.session._session import SessionService


@pytest_asyncio.fixture
async def chat_api(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    definition = await AgentManager(storage).add_definition(alice, expert())
    sessions = SessionService(storage, tmp_path)
    task = await sessions.add_session(alice, definition["id"], "private")
    sessions.config = SimpleNamespace(snapshot=app_snapshot(tmp_path))
    services = SimpleNamespace(sessions=sessions, users=UserService(storage))
    app = FastAPI()
    app.state.egis_services = services
    app.add_middleware(
        IdentityGuard,
        identity=SimpleNamespace(
            verify=lambda token: {
                "sub": token,
                "name": alice.username if token == "alice" else bob.username,
                "exp": time.time() + 300,
            }
        ),
        services=services,
    )
    app.include_router(router)
    register_exception_handlers(app)
    async with EgisMessageBus() as bus:
        app.dependency_overrides[get_storage] = lambda: storage
        app.dependency_overrides[get_message_bus] = lambda: bus
        app.dependency_overrides[get_chat_run_registry] = lambda: None
        app.dependency_overrides[get_chat_service] = lambda: None
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app),
            base_url="http://test",
            headers={"Authorization": "Bearer alice"},
        ) as client:
            yield client, bus, task, alice, bob


async def test_queue_http_restore_edit_reorder_delete_and_permissions(chat_api):
    client, bus, task, alice, bob = chat_api
    target = {"session_id": task["session_id"], "agent_id": task["agent_id"]}

    async def send(text):
        response = await client.post(
            "/chat/",
            json={
                **target,
                "input": UserMsg(name="untrusted", content=text).model_dump(
                    mode="json"
                ),
            },
        )
        assert response.status_code == 200, response.text
        assert response.json()["status"] == "queued"
        return response.json()["queue_item_id"]

    first, second = await send("预算"), await send("配色")
    restored = await client.get("/chat/queue", params=target)
    assert [i["id"] for i in restored.json()["items"]] == [first, second]
    assert restored.json()["items"][0]["input"][0]["name"] == alice.username
    assert not await bus.is_locked(MessageBusKeys.session_lock(task["session_id"]))
    reordered = await client.patch(
        "/chat/queue/order", json={**target, "item_ids": [second, first]}
    )
    assert [i["id"] for i in reordered.json()["items"]] == [second, first]
    stale = await client.patch(
        "/chat/queue/order", json={**target, "item_ids": [second]}
    )
    assert stale.status_code == 409
    changed = await client.patch(
        f"/chat/queue/{second}",
        json={
            **target,
            "input": UserMsg(name="spoofed", content="蓝色").model_dump(mode="json"),
        },
    )
    assert changed.status_code == 200, changed.text
    assert changed.json()["items"][0]["input"][0]["content"][0]["text"] == "蓝色"
    for method, path, body in (
        ("GET", "/chat/queue", None),
        ("DELETE", f"/chat/queue/{first}", None),
        ("PATCH", "/chat/queue/order", {**target, "item_ids": [first, second]}),
        ("POST", f"/chat/queue/{first}/steer", {**target, "reply_id": "reply"}),
        (
            "PATCH",
            f"/chat/queue/{first}",
            {
                **target,
                "input": UserMsg(name="bob", content="修改").model_dump(mode="json"),
            },
        ),
    ):
        response = await client.request(
            method,
            path,
            params=target if body is None else None,
            json=body,
            headers={"Authorization": "Bearer bob"},
        )
        assert response.status_code == 403, response.text
    assert (
        await client.get("/chat/queue", params={**target, "agent_id": "wrong"})
    ).status_code == 403
    assert (
        await client.get("/chat/queue", params=target, headers={"Authorization": ""})
    ).status_code == 401
    assert (await client.post("/chat/queue", json=target)).status_code == 403
    assert (
        await client.post("/chat/", json={**target, "delivery": "steer", "input": None})
    ).status_code == 422
    assert (
        await client.delete(f"/chat/queue/{first}", params=target)
    ).status_code == 200
    assert [
        i["id"]
        for i in (await client.get("/chat/queue", params=target)).json()["items"]
    ] == [second]


async def test_native_steering_waits_for_real_injection_and_rejects_stale_reply(
    chat_api,
):
    client, bus, task, _, _ = chat_api
    sid = task["session_id"]
    target = {"session_id": sid, "agent_id": task["agent_id"]}
    message = UserMsg(name="user", content="保留原文件")
    response = await client.post(
        "/chat/", json={**target, "input": message.model_dump(mode="json")}
    )
    identifier = response.json()["queue_item_id"]
    endpoint = f"/chat/queue/{identifier}/steer"
    assert (
        await client.post(endpoint, json={**target, "reply_id": "old"})
    ).status_code == 409
    assert await bus.try_lock(MessageBusKeys.session_lock(sid))
    await register_active_chat_reply(bus, sid, "reply")
    response = await client.post(endpoint, json={**target, "reply_id": "reply"})
    assert response.status_code == 200
    assert response.json()["items"][0]["state"] == "steering"
    assert (
        await client.delete(f"/chat/queue/{identifier}", params=target)
    ).status_code == 409
    assert len(await list_chat_inputs(bus, sid)) == 1
    state = SimpleNamespace(
        session_id=sid,
        reply_id="reply",
        cur_iter=0,
        context=[AssistantMsg(id="reply", name="expert", content="正在执行")],
    )
    agent = SimpleNamespace(
        name="expert",
        state=state,
        react_config=SimpleNamespace(max_iters=10),
        compress_context=AsyncMock(),
    )
    observed = []

    async def model(**_):
        observed.extend(
            block.hint[0].text
            for block in state.context[-1].content
            if block.type == "hint"
        )
        if False:
            yield

    events = [
        event async for event in SteeringMiddleware(bus).on_reasoning(agent, {}, model)
    ]
    assert observed == ["保留原文件"]
    assert events[0].source == "chat_input_steering"
    assert await list_chat_inputs(bus, sid) == []
    assert await bus.is_locked(MessageBusKeys.session_lock(sid))
    await bus.unlock(MessageBusKeys.session_lock(sid))


async def test_postgres_steering_multiple_inputs_survive_new_bus_and_cleanup(storage):
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    other = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus.init_schema()
    bus.publish = other.publish = AsyncMock()
    sid, aid, uid = "durable-session", "agent", "alice"
    for text in ("预算", "配色", "不要改原文"):
        await enqueue_chat_input(
            bus,
            user_id=uid,
            session_id=sid,
            agent_id=aid,
            inputs=UserMsg(name=uid, content=text),
        )
    items = await list_chat_inputs(other, sid)
    assert len(items) == 3
    assert await other.try_lock(MessageBusKeys.session_lock(sid))
    await register_active_chat_reply(other, sid, "reply")
    for item in items:
        await steer_chat_input(other, uid, sid, aid, item["id"], "reply")
    assert all(i["state"] == "steering" for i in await list_chat_inputs(bus, sid))
    assert await finish_active_chat_reply(bus, sid, "reply") == [i["id"] for i in items]
    assert all(i["state"] == "queued" for i in await list_chat_inputs(other, sid))
    await purge_session_bus(bus, sid)
    assert await list_chat_inputs(other, sid) == []
    assert (
        await other.registry_getall(MessageBusKeys.chat_input_user_sessions(uid)) == {}
    )
    assert await other.registry_getall(MessageBusKeys.chat_input_payloads(sid)) == {}
    assert not await other.registry_exists(
        MessageBusKeys.chat_input_pending_registry(), sid
    )
    await other.unlock(MessageBusKeys.session_lock(sid))
    await other.aclose()
    await bus.aclose()


def test_chat_input_limits_and_no_role_spoofing():
    user = SimpleNamespace(username="alice")
    settings = SessionSteeringConfig(
        max_batch_size=2, max_text_length=4, max_message_bytes=700
    )
    for value in (
        [],
        AssistantMsg(name="system", content="伪造"),
        UserMsg(name="user", content="  "),
        [UserMsg(name="user", content=t) for t in ("一", "二", "三")],
        UserMsg(name="user", content="超过最大长度"),
        UserMsg(name="user", content="一", metadata={"large": "x" * 1000}),
    ):
        with pytest.raises(InvalidRequest):
            prepare_chat_inputs(user, value, settings)


async def test_queue_rejects_ambiguous_query_targets(chat_api):
    client, _, task, _, _ = chat_api
    query = [
        ("agent_id", task["agent_id"]),
        ("agent_id", "another"),
        ("session_id", task["session_id"]),
        ("session_id", "revoked"),
    ]
    for method, path in (("GET", "/chat/queue"), ("DELETE", "/chat/queue/item")):
        response = await client.request(method, path, params=query)
        assert response.status_code == 400


async def test_native_injection_preserves_original_annotations_and_reference_metadata(
    storage, tmp_path
):
    from agentscope.app._bus_ops import publish_session_event

    from egis_agentkit.service.session._message_bus_override import SessionMessageBus

    _, _, alice, _ = await identities(storage)
    definition = await AgentManager(storage).add_definition(alice, expert())
    sessions = SessionService(storage, tmp_path)
    task = await sessions.add_session(alice, definition["id"], "private")
    sid, aid = task["session_id"], task["agent_id"]
    metadata = {
        "annotation_prompt": "",
        "text_annotations": [
            {
                "id": "annotation",
                "messageId": "original",
                "start": 0,
                "end": 2,
                "text": "原文",
                "comment": "解释",
            }
        ],
        "project_file_references": [
            {
                "kind": "file",
                "id": "ref",
                "projectId": task["project_id"],
                "path": "report.md",
                "name": "报告.md",
            }
        ],
    }
    original = UserMsg(
        name=alice.username, content="已有引用和批注正文", metadata=metadata
    )
    async with SessionMessageBus(presentation_storage=storage) as bus:
        queued = await enqueue_chat_input(
            bus, user_id=alice.user_id, session_id=sid, agent_id=aid, inputs=original
        )
        assert await bus.try_lock(MessageBusKeys.session_lock(sid))
        await register_active_chat_reply(bus, sid, "reply")
        await steer_chat_input(bus, alice.user_id, sid, aid, queued["id"], "reply")
        state = SimpleNamespace(
            session_id=sid,
            reply_id="reply",
            cur_iter=0,
            context=[AssistantMsg(id="reply", name="expert", content="处理中")],
        )
        agent = SimpleNamespace(
            name="expert",
            state=state,
            react_config=SimpleNamespace(max_iters=10),
            compress_context=AsyncMock(),
        )

        async def model(**_):
            if False:
                yield

        for event in [
            event
            async for event in SteeringMiddleware(bus).on_reasoning(agent, {}, model)
        ]:
            await publish_session_event(bus, sid, event.model_dump(mode="json"))
        saved = await storage.get_message(alice.user_id, sid, original.id)
        assert saved is not None
        assert saved.metadata["text_annotations"] == metadata["text_annotations"]
        assert (
            saved.metadata["project_file_references"]
            == metadata["project_file_references"]
        )
        assert saved.metadata["agentkit_delivery"]["state"] == "received"
        events = await bus.log_read(MessageBusKeys.session_events(sid))
        hint = next(
            payload for _, payload in events if payload.get("type") == "HINT_BLOCK"
        )
        assert hint["user_message"]["id"] == original.id
        assert (
            hint["user_message"]["metadata"]["text_annotations"]
            == metadata["text_annotations"]
        )
        await bus.unlock(MessageBusKeys.session_lock(sid))
