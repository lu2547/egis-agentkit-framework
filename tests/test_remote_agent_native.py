"""Exercise each independently runnable provider through Egis and native ChatService."""

import asyncio
import importlib.util
import json
import socket
from contextlib import asynccontextmanager, closing
from pathlib import Path
from uuid import uuid4

import httpx
import pytest
import uvicorn
from agentscope.app._service import _chat
from agentscope.formatter import OpenAIChatFormatter
from agentscope.message import TextBlock, ToolCallBlock
from agentscope.model import ChatModelBase, ChatResponse
from egis_agentkit_app.bootstrap import build_app
from runtime_helpers import stub_sandbox_provider
from sqlalchemy import select
from test_app_http import Config, app_snapshot
from test_domain import identities

from egis_agentkit.client.nacos import NacosBootstrap, NacosClient
from egis_agentkit.storage._remote_agent import remote_agent_runs

FINANCE = Path(__file__).resolve().parents[1] / "examples" / "remote_finance"


@asynccontextmanager
async def serve(app, sock):
    server = uvicorn.Server(
        uvicorn.Config(app, log_level="error", ws="websockets-sansio")
    )
    server.install_signal_handlers = lambda: None
    task = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                if task.done():
                    await task
                    raise AssertionError("Test ASGI server did not start")
                await asyncio.sleep(0.01)
        yield
    finally:
        server.should_exit = True
        await asyncio.wait_for(task, 5)


def socket_address(sock):
    sock.bind(("127.0.0.1", 0))
    return f"127.0.0.1:{sock.getsockname()[1]}"


def load_provider():
    spec = importlib.util.spec_from_file_location("remote_finance_demo", FINANCE / "demo_service.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


async def eventually(predicate):
    async with asyncio.timeout(5):
        while not (result := await predicate()):
            await asyncio.sleep(0.05)
    return result


@pytest.mark.parametrize("protocol", ["sync", "callback", "websocket"])
async def test_finance_through_native_chat_invite_and_teamsay(
    protocol, storage, tmp_path, monkeypatch
):
    stub_sandbox_provider(monkeypatch)
    _, admin, _, _ = await identities(storage)
    monkeypatch.setenv("TEST_DB_URL", "postgresql+asyncpg://unused")
    monkeypatch.setenv("TEST_DOWNLOAD", "download-secret-with-more-than-thirty-two-bytes")
    monkeypatch.setenv("TEST_JWT", "jwt-secret-with-more-than-thirty-two-bytes")
    monkeypatch.setenv("TEST_LOGIN", "a-test-password-with-enough-entropy")
    monkeypatch.setenv("TEST_MODEL_KEY", "test-only-unused-model-key")
    monkeypatch.setenv("EGIS_REMOTE_AGENT_FINANCE_TOKEN", "finance-token")
    monkeypatch.setenv("EGIS_REMOTE_AGENT_FINANCE_CALLBACK_SECRET", "finance-callback-secret-32-characters")
    monkeypatch.setenv("FINANCE_CALLBACK_DELIVERIES", "2")
    monkeypatch.setenv("FINANCE_WORK_DELAY", "0.05")
    recipient = None
    seen_reports = set()

    class LeaderModel(ChatModelBase):
        formatter = OpenAIChatFormatter()

        async def __call__(self, messages, tools=None, tool_choice=None):
            for message in messages:
                for hint in message.get_content_blocks("hint"):
                    if "三笔报销共 250 元。" in str(hint.hint):
                        seen_reports.add(hint.id)
            if messages[-1].get_text_content() == "请组长追问":
                return ChatResponse(
                    is_last=True,
                    content=[
                        ToolCallBlock(
                            id=uuid4().hex,
                            name="TeamSay",
                            input=json.dumps(
                                {"to": recipient, "content": "刚才的合计是多少？"}
                            ),
                        )
                    ],
                )
            return ChatResponse(is_last=True, content=[TextBlock(text="组长已处理")])

    async def get_model(*args):
        return LeaderModel(None, "leader-test", LeaderModel.Parameters(), stream=False)

    monkeypatch.setattr(_chat, "get_model", get_model)
    with closing(socket.socket()) as egis_socket, closing(socket.socket()) as demo_socket:
        egis_address, demo_address = map(socket_address, (egis_socket, demo_socket))
        egis_url = "http://" + egis_address
        monkeypatch.setenv("FINANCE_CALLBACK_BASE_URL", egis_url)
        config = Config(
            app_snapshot(
                tmp_path, agent={"remote": {"callback": {"public_base_url": egis_url}}}
            )
        )
        async with httpx.AsyncClient(
            transport=httpx.MockTransport(lambda req: httpx.Response(503))
        ) as unused_nacos:
            nacos = NacosClient(
                NacosBootstrap(server_url="http://unused.test"), http_client=unused_nacos
            )
            app = await build_app(config, nacos, engine=storage.engine)
            provider = load_provider()
            async with (
                serve(app, egis_socket),
                serve(provider.app, demo_socket),
                httpx.AsyncClient(base_url=egis_url) as client,
            ):
                login = await client.post(
                    "/auth/login",
                    json={"user_id": "admin", "password": "a-test-password-with-enough-entropy"},
                )
                assert login.status_code == 200, login.text
                client.headers["Authorization"] = "Bearer " + login.json()["access_token"]

                registration = json.loads((FINANCE / f"{protocol}.json").read_text())
                for field in ("endpoint", "cancel_endpoint"):
                    connection = registration["payload"]["remote_config"]
                    if field in connection:
                        connection[field] = connection[field].replace("127.0.0.1:8120", demo_address)
                created = await client.post("/manager/agents", json=registration)
                assert created.status_code == 201, created.text
                expert = created.json()

                async def create_task(definition_id, name):
                    response = await client.post(
                        "/manager/tasks",
                        json={
                            "definition_id": definition_id,
                            "resource_view_type": "private",
                            "name": name,
                        },
                    )
                    assert response.status_code == 201, response.text
                    return response.json()

                async def chat(task, message):
                    response = await client.post(
                        "/chat/",
                        json={
                            "session_id": task["session_id"],
                            "agent_id": task["agent_id"],
                            "input": {
                                "name": "user",
                                "role": "user",
                                "content": [{"type": "text", "text": message}],
                            },
                        },
                    )
                    assert response.status_code == 200, response.text

                async def answers(sid, expected):
                    messages, _ = await storage.list_messages(admin.user_id, sid)
                    completed = [
                        msg for msg in messages
                        if msg.role == "assistant" and msg.finished_reason
                    ]
                    assert all(msg.finished_reason == "completed" for msg in completed), completed
                    matches = [
                        msg for msg in completed
                        if "三笔报销共 250 元。" in (msg.get_text_content() or "")
                    ]
                    return matches if len(matches) >= expected else None

                user_text = "汇总三笔报销，金额为 120、85、45 元。"
                independent = await create_task(expert["id"], "独立财务测试")
                await chat(independent, user_text)
                await eventually(lambda: answers(independent["session_id"], 1))

                personal = (await client.get("/manager/assistant")).json()
                leader = await create_task(personal["id"], "协作财务测试")
                staged = await client.post(
                    f"/manager/tasks/{leader['session_id']}/collaborators",
                    json={
                        "request_id": "remote-finance",
                        "experts": [
                            {"definition_id": expert["id"], "resource_view_type": "private"}
                        ],
                    },
                )
                assert staged.status_code == 200, staged.text
                await chat(leader, user_text)

                async def team_member():
                    session = await storage.get_session(
                        admin.user_id, leader["agent_id"], leader["session_id"]
                    )
                    if session.team_id:
                        team = await storage.get_team(admin.user_id, session.team_id)
                        if team.data.members:
                            return team.data.members[0]
                    return None

                member = await eventually(team_member)
                await eventually(lambda: answers(member.session_id, 1))
                recipient = registration["agent_name"] + "@" + member.agent_id[:8]

                async def reports(expected):
                    return len(seen_reports) >= expected

                await eventually(lambda: reports(1))

                async def leader_idle():
                    # 模型已收到报告不代表回复已结束；等待原生运行状态释放后再发送下一轮。
                    response = await client.get(
                        f"/sessions/{leader['session_id']}/messages",
                        params={"agent_id": leader["agent_id"]},
                    )
                    assert response.status_code == 200, response.text
                    return not response.json()["is_running"]

                await eventually(leader_idle)
                await chat(leader, "请组长追问")
                await eventually(lambda: answers(member.session_id, 2))
                await eventually(lambda: reports(2))

                async with storage.engine.connect() as conn:
                    rows = (await conn.execute(select(remote_agent_runs))).mappings().all()
                independent_rows = [
                    row for row in rows
                    if row["conversation_id"] == independent["session_id"]
                ]
                member_rows = [
                    row for row in rows if row["conversation_id"] == member.session_id
                ]
                assert len(independent_rows) == 1 and independent_rows[0]["reported_at"] is None
                assert len(member_rows) == 2 and all(row["reported_at"] for row in member_rows)

                monkeypatch.setenv("FINANCE_WORK_DELAY", "2")
                await chat(independent, user_text)

                async def provider_running():
                    return next(
                        (
                            job for job in provider.tasks.values()
                            if job.request.conversation_id == independent["session_id"]
                            and not job.result.done()
                        ),
                        None,
                    )

                job = await eventually(provider_running)
                stopped = await client.post(
                    f"/sessions/{independent['session_id']}/interrupt",
                    params={"agent_id": independent["agent_id"]},
                )
                assert stopped.is_success, stopped.text
                provider_result = await asyncio.wait_for(asyncio.shield(job.result), 2)
                assert provider_result["status"] == "cancelled"
                persisted = await storage.remote_runs.get_request(job.request.run_id)
                assert persisted["result"]["status"] == "cancelled"

                async def interrupted():
                    messages, _ = await storage.list_messages(admin.user_id, independent["session_id"])
                    return any(msg.finished_reason == "interrupted" for msg in messages)

                await eventually(interrupted)
