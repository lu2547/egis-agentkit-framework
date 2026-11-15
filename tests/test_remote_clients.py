"""远程通信客户端可直接调用第三方，不依赖数据库或原生会话。"""

import httpx
from test_remote_agents import config, request

from egis_agentkit.client.nacos._settings import RemoteConfig


async def test_http_client_runs_without_storage_or_runtime_service(monkeypatch):
    from egis_agentkit.client.remote_agent._http_sync import SyncHTTPRemoteAgentClient

    monkeypatch.setenv("EGIS_REMOTE_AGENT_TEST_TOKEN", "finance-token")

    def provider(incoming):
        assert incoming.headers["Authorization"] == "Bearer finance-token"
        return httpx.Response(
            200,
            json={"run_id": "job", "status": "completed", "output": {"text": "250 元"}},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        client = SyncHTTPRemoteAgentClient(
            "finance", config(), RemoteConfig(), http=http
        )
        assert (await client.send_request(request())).output.text == "250 元"


async def test_callback_client_submits_without_a_database_waiter(monkeypatch):
    from egis_agentkit.client.remote_agent._http_callback import (
        CallbackHTTPRemoteAgentClient,
    )

    monkeypatch.setenv("EGIS_REMOTE_AGENT_TEST_TOKEN", "finance-token")
    monkeypatch.setenv(
        "EGIS_REMOTE_AGENT_TEST_CALLBACK_SECRET", "callback-secret-with-enough-entropy"
    )
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(202))
    ) as http:
        client = CallbackHTTPRemoteAgentClient(
            "finance",
            config(
                transport="http_callback",
                callback_secret_ref="EGIS_REMOTE_AGENT_TEST_CALLBACK_SECRET",
            ),
            RemoteConfig(),
            http=http,
            callback_base_url="https://egis.test",
        )
        assert await client.send_request(request()) is None
