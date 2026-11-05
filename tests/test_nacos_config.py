import httpx
import pytest
import yaml

from egis_agentkit.config import (
    NacosBootstrap,
    NacosClient,
    NacosConfigProvider,
    NacosError,
)


@pytest.mark.asyncio
async def test_real_v33_config_query_envelope_extracts_content_and_rejects_nested_failure():
    body = {
        "code": 0,
        "data": {
            "success": True,
            "content": "config_version: 1",
            "md5": "abc",
            "errorCode": 0,
        },
    }
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda req: httpx.Response(200, json=body))
    ) as http:
        client = NacosClient(
            NacosBootstrap(server_url="http://registry"), http_client=http
        )
        assert await client.get_config("real.yaml") == "config_version: 1"
        body["data"]["success"] = False
        body["data"]["errorCode"] = 20004
        with pytest.raises(NacosError):
            await client.get_config("real.yaml")


def documents(version=1):
    return {
        "agentkit-runtime.yaml": {
            "config_version": version,
            "workspace_root": "/tmp/work",
            "release_roots": ["/tmp/releases"],
            "auth": {
                "mode": "demo",
                "issuer": "egis",
                "audience": "agentkit",
                "algorithm": "HS256",
            },
        },
        "agentkit-models.yaml": {
            "config_version": version,
            "default_key": "company",
            "models": [
                {
                    "key": "company",
                    "display_name": "Company",
                    "model": "demo",
                    "api_key_env": "COMPANY_API_KEY",
                }
            ],
        },
        "agentkit-policies.yaml": {
            "config_version": version,
            "allow_private_self_use": True,
        },
    }


@pytest.mark.asyncio
async def test_client_config_reads_envelope_and_publishes_correct_namespace_group():
    requests = []

    def handle(req):
        requests.append(req)
        if req.method == "GET":
            assert req.url.params["groupName"] == "EGIS_AGENTKIT"
            return httpx.Response(200, json={"code": 0, "data": "config_version: 1"})
        return httpx.Response(200, json={"code": 0, "data": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = NacosClient(
            NacosBootstrap(
                server_url="http://registry:8848",
                identity_key="identity",
                identity_value="private-secret",
            ),
            http_client=http,
        )
        assert await client.get_config("a.yaml") == "config_version: 1"
        assert await client.publish_config("a.yaml", "hello") is True
    assert requests[0].headers["identity"] == "private-secret"
    assert b"groupName=EGIS_AGENTKIT" in requests[1].content
    assert "private-secret" not in repr(client.bootstrap)


@pytest.mark.asyncio
async def test_client_rejects_cross_origin_paths_and_scrubs_server_errors():
    def handle(req):
        return httpx.Response(403, json={"code": 403, "message": "private-secret"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = NacosClient(
            NacosBootstrap(server_url="http://registry", access_token="private-secret"),
            http_client=http,
        )
        with pytest.raises(ValueError):
            await client.request("GET", "https://evil.test/")
        with pytest.raises(NacosError) as exc:
            await client.get_config("a")
        assert "private-secret" not in str(exc.value)


@pytest.mark.asyncio
async def test_snapshot_refresh_atomic_invalid_or_mixed_version_keeps_last_valid():
    docs = documents()

    def handle(req):
        return httpx.Response(
            200,
            json={"code": 0, "data": yaml.safe_dump(docs[req.url.params["dataId"]])},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        provider = NacosConfigProvider(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
        )
        with pytest.raises(RuntimeError):
            _ = provider.snapshot
        first = await provider.refresh()
        assert first.config_version == 1 and first.models.models[0].key == "company"
        docs["agentkit-models.yaml"]["config_version"] = 2
        with pytest.raises(NacosError):
            await provider.refresh()
        assert provider.snapshot == first
        docs.update(documents(2))
        docs["agentkit-models.yaml"]["default_key"] = "missing"
        with pytest.raises(NacosError):
            await provider.refresh()
        assert provider.snapshot == first
        docs.update(documents(2))
        second = await provider.refresh()
        assert second.config_version == 2
        assert first.config_version == 1
        docs.update(documents(3))
        docs["agentkit-runtime.yaml"]["workspace_root"] = "/different"
        with pytest.raises(NacosError):
            await provider.refresh()
        assert provider.restart_required is True
        assert provider.snapshot == second


@pytest.mark.asyncio
async def test_snapshot_consumers_cannot_mutate_shared_provider_state():
    docs = documents()

    def handle(req):
        return httpx.Response(
            200,
            json={"code": 0, "data": yaml.safe_dump(docs[req.url.params["dataId"]])},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        provider = NacosConfigProvider(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
        )
        snapshot = await provider.refresh()
        snapshot.models.models.clear()
        assert provider.snapshot.models.default_key == "company"
        assert len(provider.snapshot.models.models) == 1


@pytest.mark.asyncio
async def test_config_batch_rejects_changed_content_without_new_version():
    docs = documents()

    def handle(req):
        return httpx.Response(
            200,
            json={"code": 0, "data": yaml.safe_dump(docs[req.url.params["dataId"]])},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        provider = NacosConfigProvider(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
        )
        first = await provider.refresh()
        docs["agentkit-policies.yaml"]["allow_private_self_use"] = False
        with pytest.raises(NacosError):
            await provider.refresh()
        assert provider.snapshot == first


@pytest.mark.asyncio
async def test_explicit_console_address_routes_only_management_not_config_reads():
    urls = []

    def handle(req):
        urls.append(str(req.url))
        return httpx.Response(200, json={"code": 0, "data": "text"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = NacosClient(
            NacosBootstrap(
                server_url="http://registry:8848", console_url="http://registry:8080"
            ),
            http_client=http,
        )
        await client.get_config("required.yaml")
        await client.request("GET", "/nacos/v3/admin/ai/skills/list")
    assert urls[0].startswith("http://registry:8848/nacos/v3/client/cs/config?")
    assert urls[1] == "http://registry:8080/v3/console/ai/skills/list"


@pytest.mark.asyncio
async def test_outage_keeps_last_configuration_without_local_fallback():
    docs = documents()
    fail = False

    def handle(req):
        if fail:
            raise httpx.ConnectError("network contains a secret", request=req)
        return httpx.Response(
            200,
            json={"code": 0, "data": yaml.safe_dump(docs[req.url.params["dataId"]])},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        provider = NacosConfigProvider(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
        )
        first = await provider.refresh()
        fail = True
        with pytest.raises(NacosError) as exc:
            await provider.refresh()
        assert "secret" not in str(exc.value)
        assert provider.snapshot == first
        assert provider.is_connected is False


@pytest.mark.asyncio
async def test_config_poller_refreshes_and_shutdown_stops_further_fetches():
    import asyncio

    docs = documents()
    fetched = 0

    def handle(req):
        nonlocal fetched
        fetched += 1
        return httpx.Response(
            200,
            json={"code": 0, "data": yaml.safe_dump(docs[req.url.params["dataId"]])},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        provider = NacosConfigProvider(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
        )
        await provider.start(interval=0.005)
        docs.update(documents(2))
        async with asyncio.timeout(1):
            while provider.snapshot.config_version != 2:
                await asyncio.sleep(0.005)
        await provider.close()
        stopped = fetched
        await asyncio.sleep(0.02)
        assert fetched == stopped
