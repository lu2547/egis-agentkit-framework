"""Scheduled main conversations share the sidebar and retain completion watermarks."""

from datetime import timedelta

import pytest
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from agentscope.app.storage._model._session import TeamOrigin
from agentscope.app.storage._sql._tables import SessionRow
from test_domain import expert, identities
from test_schedule_storage import NOW, task
from test_session_listing_queries import select_queries

from egis_agentkit.errors import Forbidden
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.session._session import SessionService


async def scheduled_task(storage, service, user, definition, schedule_id="morning"):
    project = await service.projects.default(user)
    await storage.schedules.create(
        task(
            schedule_id,
            user_id=user.user_id,
            agent_id=definition["id"],
            project_id=project["id"],
        )
    )
    return await service.add_session(
        user, definition["id"], "private", project_id=project["id"],
        schedule_id=schedule_id,
    )


async def run_for(
    storage, session, *, schedule_id="morning", status="succeeded", offset=0
):
    at = NOW + timedelta(seconds=offset)
    run = await storage.schedules.enqueue_test("alice", schedule_id, at)
    if status != "queued":
        await storage.schedules.claim_run(run["id"], at)
        await storage.schedules.bind_run(run["id"], session["session_id"])
    if status in {"succeeded", "failed"}:
        await storage.schedules.finish_run(
            run["id"], at + timedelta(seconds=1), status=status
        )
    return run


async def test_scheduled_mains_share_order_permissions_and_running_state(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    agents, service = AgentManager(storage), SessionService(storage, tmp_path)
    definition = await agents.add_definition(alice, expert())
    ordinary = await service.add_session(alice, definition["id"], "private")
    scheduled = await scheduled_task(storage, service, alice, definition)
    hidden = await service.add_session(
        alice, definition["id"], "private", schedule_id="morning"
    )
    record = await service.get_session_record(alice, hidden["session_id"])
    record.state.middle_context["egis_agentkit"]["is_initialized"] = False
    await storage.sessions.save_state(record)
    worker = await service.add_session(alice, definition["id"], "private")
    record = await service.get_session_record(alice, worker["session_id"])
    record.origin = TeamOrigin()
    await storage._write_row(SessionRow, record)
    foreign = await agents.add_definition(bob, expert(owner="bob", name="bob_writer"))
    await scheduled_task(storage, service, bob, foreign, "bob-schedule")
    async with InMemoryMessageBus() as bus:
        await bus.try_lock(MessageBusKeys.session_lock(scheduled["session_id"]))
        with select_queries(storage) as queries:
            listed = await service.list_sessions(alice, bus)
        assert [item["session_id"] for item in listed] == [
            scheduled["session_id"], ordinary["session_id"]
        ]
        assert [item["source"] for item in listed] == ["schedule", "user"]
        assert [item["latest_schedule_run_id"] for item in listed] == [None, None]
        assert [item["is_running"] for item in listed] == [True, False]
        assert len(queries) <= 6
    with pytest.raises(Forbidden):
        await service.get_session(bob, scheduled["session_id"])
    await agents.delete_definition(alice, definition["id"])
    assert await service.list_sessions(alice) == []
    with pytest.raises(Forbidden):
        await service.get_session(alice, scheduled["session_id"])


async def test_completed_run_marker_advances_on_reused_session_and_survives_history_hiding(
    storage, tmp_path
):
    _, _, alice, _ = await identities(storage)
    service = SessionService(storage, tmp_path)
    definition = await AgentManager(storage).add_definition(alice, expert())
    scheduled = await scheduled_task(storage, service, alice, definition)
    first = await run_for(storage, scheduled)
    detail = await service.get_session(alice, scheduled["session_id"])
    assert detail["latest_schedule_run_id"] == first["id"]
    queued = await run_for(storage, scheduled, status="queued", offset=2)
    assert (await service.list_sessions(alice))[0]["latest_schedule_run_id"] == first["id"]
    await storage.schedules.claim_run(queued["id"], NOW + timedelta(seconds=2))
    await storage.schedules.bind_run(queued["id"], scheduled["session_id"])
    assert (await service.list_sessions(alice))[0]["latest_schedule_run_id"] == first["id"]
    await storage.schedules.finish_run(
        queued["id"], NOW + timedelta(seconds=3), status="failed"
    )
    await storage.schedules.archive("alice", queued["id"], True, NOW)
    await storage.schedules.delete_run("alice", queued["id"], NOW)
    await storage.schedules.delete("alice", ["morning"], NOW)
    detail = await service.get_session(alice, scheduled["session_id"])
    assert detail["source"] == "schedule"
    assert detail["latest_schedule_run_id"] == queued["id"]
    assert (await service.list_sessions(alice))[0]["latest_schedule_run_id"] == queued["id"]


async def test_task_http_exposes_scheduled_completion_without_observing_running(
    storage, tmp_path, monkeypatch
):
    import httpx
    from agentscope.message import Msg
    from egis_agentkit_app.bootstrap import build_app
    from identity_helpers import configure_test_issuer, signed_token
    from test_app_http import Config, app_snapshot

    from egis_agentkit.client.nacos import NacosBootstrap, NacosClient

    configure_test_issuer(monkeypatch)
    monkeypatch.setenv("TEST_DOWNLOAD", "test-download-secret-with-thirty-two-bytes")
    monkeypatch.setenv("TEST_MODEL_KEY", "test-only-unused-model-key")
    _, _, alice, _ = await identities(storage)
    async with NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos:
        app = await build_app(
            Config(app_snapshot(tmp_path)), nacos,
            engine=storage.engine, manage_logging=False,
        )
        async with app.router.lifespan_context(app):
            services = app.state.egis_services
            definition = await services.agents.add_definition(alice, expert())
            scheduled = await scheduled_task(
                storage, services.sessions, alice, definition
            )
            run = await run_for(storage, scheduled)
            await storage.upsert_message(
                alice.user_id, scheduled["session_id"],
                Msg(
                    name="user", role="user",
                    content=[{"type": "text", "text": "scheduled input"}],
                ),
            )
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app), base_url="http://test",
                headers={"Authorization": "Bearer " + signed_token("alice")},
            ) as client:
                response = await client.get("/manager/tasks")
                assert response.status_code == 200, response.text
                [item] = response.json()["items"]
                assert item["source"] == "schedule"
                assert item["latest_schedule_run_id"] == run["id"]
                assert item["is_running"] is False
                detail_url = "/manager/tasks/" + scheduled["session_id"]
                response = await client.get(detail_url)
                assert response.status_code == 200, response.text
                assert response.json()["latest_schedule_run_id"] == run["id"]
                response = await client.get(
                    f"/sessions/{scheduled['session_id']}/messages",
                    params={"agent_id": scheduled["agent_id"]},
                )
                assert response.status_code == 200, response.text
                assert "scheduled input" in response.text
                await assert_scheduled_stream(app, scheduled)
                client.headers["Authorization"] = "Bearer " + signed_token("bob")
                assert (await client.get("/manager/tasks")).json()["items"] == []
                assert (await client.get(detail_url)).status_code == 403


async def assert_scheduled_stream(app, scheduled):
    import asyncio

    from agentscope.app._bus_ops import publish_session_event
    from identity_helpers import signed_token

    await publish_session_event(
        app.state.egis_bus, scheduled["session_id"],
        {
            "type": "TEXT_BLOCK_DELTA", "reply_id": "scheduled-reply",
            "delta": "scheduled-output",
        },
    )
    frames, disconnect = [], asyncio.Event()

    async def receive():
        await disconnect.wait()
        return {"type": "http.disconnect"}

    async def send(message):
        frames.append(message)
        if b"scheduled-output" in message.get("body", b""):
            disconnect.set()

    path = f"/sessions/{scheduled['session_id']}/stream"
    await asyncio.wait_for(
        app(
            {
                "type": "http", "asgi": {"version": "3.0"}, "http_version": "1.1",
                "method": "GET", "scheme": "http", "path": path,
                "raw_path": path.encode(), "root_path": "",
                "query_string": f"agent_id={scheduled['agent_id']}".encode(),
                "headers": [
                    (b"authorization", ("Bearer " + signed_token("alice")).encode())
                ],
                "server": ("test", 80), "client": ("127.0.0.1", 1234),
            },
            receive, send,
        ),
        3,
    )
    assert next(
        frame["status"] for frame in frames if frame["type"] == "http.response.start"
    ) == 200
    assert disconnect.is_set()
