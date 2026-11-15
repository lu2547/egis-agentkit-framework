import asyncio
import hashlib
import hmac
import json
import time

import httpx
import pytest
from agentscope.app.message_bus import InMemoryMessageBus

from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.storage.model import AgentDefinition, User


def config(**changes):
    from egis_agentkit.storage.model.agent._remote_config import RemoteAgentConfig

    return RemoteAgentConfig.model_validate(
        {
            "transport": "http_sync",
            "endpoint": "http://finance.test/v1/runs",
            "credential_ref": "EGIS_REMOTE_AGENT_TEST_TOKEN",
            **changes,
        }
    )


def request(**changes):
    from egis_agentkit.storage.model._remote_agent import RemoteRunRequest

    return RemoteRunRequest.model_validate(
        {
            "run_id": "job",
            "conversation_id": "session",
            "input": {"text": "120、85、45"},
            **changes,
        }
    )


async def test_remote_definition_cannot_be_registered_by_an_ordinary_user(storage):
    from egis_agentkit.errors import Forbidden

    draft = AgentDefinition(
        owner_id="alice",
        agent_type="remote",
        agent_name="finance",
        agent_display_name="财务专家",
        payload={
            "remote_config": config().model_dump(mode="json"),
        },
    )
    with pytest.raises(Forbidden):
        await AgentManager(storage).add_definition(User(user_id="alice", username="Alice"), draft)


async def test_sync_transport_reuses_id_on_retry_and_reuses_saved_result(storage, monkeypatch):
    from egis_agentkit.extension._remote_agent_callback import RemoteAgentRuntime

    monkeypatch.setenv("EGIS_REMOTE_AGENT_TEST_TOKEN", "finance-token")
    attempts = []

    def provider(req):
        assert req.headers["Authorization"] == "Bearer finance-token"
        attempts.append(json.loads(req.content))
        if len(attempts) == 1:
            return httpx.Response(503)
        return httpx.Response(
            200,
            json={"run_id": "job", "status": "completed", "output": {"text": "250 元"}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        async with InMemoryMessageBus() as bus:
            service = RemoteAgentRuntime(storage.remote_runs, bus, http=http)
            agent = service.get_client("finance", config())
            result = await service.send_request(agent, request())
            assert result.output.text == "250 元"
            assert (await service.send_request(agent, request())).output.text == "250 元"
            assert attempts == [
                {
                    "run_id": "job",
                    "conversation_id": "session",
                    "input": {"text": "120、85、45"},
                    "attachments": [],
                }
            ] * 2


async def test_callback_checks_connection_signature_and_accepts_exact_replay(storage, monkeypatch):
    from egis_agentkit.errors import AgentKitError
    from egis_agentkit.extension._remote_agent_callback import RemoteAgentRuntime

    monkeypatch.setenv("EGIS_REMOTE_AGENT_TEST_TOKEN", "finance-token")
    monkeypatch.setenv("EGIS_REMOTE_AGENT_TEST_CALLBACK_SECRET", "finance-callback-secret-32-characters")
    dispatched = asyncio.Event()

    def provider(req):
        assert json.loads(req.content)["callback_url"] == (
            "https://egis.test/callbacks/remote-agent/runs/job/result"
        )
        dispatched.set()
        return httpx.Response(202)

    def sign(value, connection="finance"):
        body = json.dumps(value).encode()
        timestamp = str(int(time.time()))
        signature = hmac.new(
            b"finance-callback-secret-32-characters",
            f"{timestamp}\n{connection}\njob\n".encode() + body,
            hashlib.sha256,
        ).hexdigest()
        return {
            "x-egis-connection": connection,
            "x-egis-timestamp": timestamp,
            "x-egis-signature": f"sha256={signature}",
        }, body

    final = {"run_id": "job", "status": "completed", "output": {"text": "250 元"}}
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        async with InMemoryMessageBus() as bus:
            service = RemoteAgentRuntime(
                storage.remote_runs, bus, callback_base_url="https://egis.test", http=http
            )
            # The callback is handled by another service instance and uses the
            # same persisted run instead of sharing an in-memory request map.
            callback = RemoteAgentRuntime(storage.remote_runs, bus)
            agent = service.get_client(
                "finance", config(transport="http_callback", callback_secret_ref="EGIS_REMOTE_AGENT_TEST_CALLBACK_SECRET")
            )
            async with asyncio.TaskGroup() as tasks:
                running = tasks.create_task(service.send_request(agent, request()))
                await asyncio.wait_for(dispatched.wait(), timeout=3)
                with pytest.raises(AgentKitError) as wrong_connection:
                    await callback.save_callback_result("job", *sign(final, connection="other"))
                assert wrong_connection.value.status_code == 401
                invalid_headers, invalid_body = sign(final)
                invalid_headers["x-egis-signature"] = "sha256=invalid"
                with pytest.raises(AgentKitError) as bad_signature:
                    await callback.save_callback_result("job", invalid_headers, invalid_body)
                assert bad_signature.value.status_code == 401
                assert await callback.save_callback_result("job", *sign(final)) == "accepted"
                assert await callback.save_callback_result("job", *sign(final)) == "duplicate"
                with pytest.raises(AgentKitError) as changed_result:
                    await callback.save_callback_result(
                        "job", *sign({**final, "output": {"text": "251 元"}})
                    )
                assert changed_result.value.status_code == 409
            assert running.result().output.text == "250 元"


async def test_callback_timeout_is_terminal_and_stops_resubmission(storage, monkeypatch):
    from egis_agentkit.extension._remote_agent_callback import RemoteAgentRuntime

    monkeypatch.setenv("EGIS_REMOTE_AGENT_TEST_TOKEN", "finance-token")
    monkeypatch.setenv("EGIS_REMOTE_AGENT_TEST_CALLBACK_SECRET", "finance-callback-secret-32-characters")
    calls = []

    def provider(req):
        calls.append(req)
        return httpx.Response(202)

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        async with InMemoryMessageBus() as bus:
            service = RemoteAgentRuntime(
                storage.remote_runs, bus, callback_base_url="https://egis.test", http=http
            )
            agent = service.get_client(
                "finance",
                config(
                    transport="http_callback",
                    callback_secret_ref="EGIS_REMOTE_AGENT_TEST_CALLBACK_SECRET",
                    timeout_seconds=0.1,
                ),
            )
            for _ in range(2):
                result = await service.send_request(agent, request())
                assert result.status == "timed_out"
            assert len(calls) == 1


async def test_websocket_reconnect_preserves_id_and_cancel_is_acknowledged(storage, monkeypatch):
    from websockets.asyncio.server import serve

    from egis_agentkit.extension._remote_agent_callback import RemoteAgentRuntime

    monkeypatch.setenv("EGIS_REMOTE_AGENT_TEST_TOKEN", "finance-token")
    submitted = []
    progress = []
    started = asyncio.Event()

    async def provider(socket):
        task = json.loads(await socket.recv())
        submitted.append(task["run_id"])
        assert task["event"] == "run.start"
        if len(submitted) == 1:
            await socket.close()
            return
        await socket.send(json.dumps({
            "event": "run.progress", "run_id": "job", "message": "收到"
        }))
        started.set()
        assert json.loads(await socket.recv()) == {"event": "run.cancel", "run_id": "job"}
        await socket.send(json.dumps({
            "event": "run.cancelled",
            "run_id": "job",
            "error": {"code": "cancelled", "message": "任务已停止"},
        }))

    async def on_progress(message):
        progress.append(message)

    async with serve(provider, "127.0.0.1", 0) as server:
        port = server.sockets[0].getsockname()[1]
        async with InMemoryMessageBus() as bus:
            service = RemoteAgentRuntime(storage.remote_runs, bus)
            agent = service.get_client(
                "finance", config(transport="websocket", endpoint=f"ws://127.0.0.1:{port}")
            )
            async with asyncio.TaskGroup() as tasks:
                running = tasks.create_task(service.send_request(agent, request(), on_progress))
                await asyncio.wait_for(started.wait(), 3)
                assert await agent.send_cancel_request("job")
            assert running.result().status == "cancelled"
    assert progress == ["收到"]
    assert submitted == ["job", "job"]


def test_remote_connection_credentials_use_dedicated_environment_names():
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        config(credential_ref="AGENTKIT_DATABASE_URL")


async def test_websocket_does_not_forward_credentials_to_redirect_targets(storage, monkeypatch):
    from http import HTTPStatus

    from websockets.asyncio.server import serve

    from egis_agentkit.extension._remote_agent_callback import RemoteAgentRuntime

    monkeypatch.setenv("EGIS_REMOTE_AGENT_TEST_TOKEN", "finance-token")
    paths = []

    def redirect(socket, incoming):
        paths.append(incoming.path)
        if incoming.path == "/redirect":
            response = socket.respond(HTTPStatus.TEMPORARY_REDIRECT, "Try again")
            response.headers["Location"] = "/target"
            return response
        return None

    async def target(socket):
        await socket.close()

    async with serve(target, "127.0.0.1", 0, process_request=redirect) as server:
        port = server.sockets[0].getsockname()[1]
        async with InMemoryMessageBus() as bus:
            service = RemoteAgentRuntime(storage.remote_runs, bus)
            agent = service.get_client(
                "finance",
                config(
                    transport="websocket",
                    endpoint=f"ws://127.0.0.1:{port}/redirect",
                    retries=0,
                ),
            )
            assert (await service.send_request(agent, request())).status == "failed"
    assert paths == ["/redirect"]
