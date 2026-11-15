import httpx
import pytest
import yaml

from egis_agentkit.client.nacos import (
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
        with pytest.raises(NacosError) as exc:
            await client.get_config("real.yaml")
        assert exc.value.operation_code == 20004


def documents(revision=1):
    return {
        "agentkit.yaml": {
            "agentkit": {
                "revision": revision,
                "workspace": {"runtime": {"root": "/tmp/work"}}, "project": {"upload": {"maxBytes": 20_971_520}},
                "user": {
                    "identity": {
                        "mode": "demo",
                        "issuer": "egis",
                        "audience": "agentkit",
                        "algorithm": "HS256",
                    }
                },
                "model": {
                    "selection": {"defaultKey": "company"},
                    "catalog": {
                        "models": [
                            {
                                "key": "company",
                                "displayName": "Company",
                                "model": "demo",
                                "apiKeyEnv": "COMPANY_API_KEY",
                            }
                        ]
                    },
                },
            }
        }
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
async def test_snapshot_reads_one_document_and_updates_typed_hot_settings():
    docs = documents()
    requests = []

    def handle(req):
        data_id = req.url.params["dataId"]
        requests.append(data_id)
        return httpx.Response(
            200,
            json={"code": 0, "data": yaml.safe_dump(docs[data_id])},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        provider = NacosConfigProvider(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
        )
        with pytest.raises(RuntimeError):
            _ = provider.snapshot
        first = await provider.refresh()
        assert first.revision == 1 and first.model.catalog.models[0].key == "company"
        docs.update(documents(2))
        current = docs["agentkit.yaml"]["agentkit"]
        current["model"]["selection"]["defaultKey"] = "missing"
        with pytest.raises(NacosError, match=r"agentkit\.model\.selection\.defaultKey"):
            await provider.refresh()
        assert provider.snapshot == first
        docs.update(documents(2))
        docs["agentkit.yaml"]["agentkit"]["project"]["upload"]["maxBytes"] = 40
        second = await provider.refresh()
        assert second.revision == 2
        assert second.project.upload.max_bytes == 40
        assert first.project.upload.max_bytes == 20_971_520
        docs.update(documents(3))
        docs["agentkit.yaml"]["agentkit"]["workspace"]["runtime"]["root"] = "/different"
        with pytest.raises(NacosError, match="restart"):
            await provider.refresh()
        assert provider.restart_required is True
        assert provider.snapshot == second
    assert requests == ["agentkit.yaml"] * 4


@pytest.mark.asyncio
@pytest.mark.parametrize("value", [None, "4096", True])
async def test_first_refresh_rejects_missing_or_invalid_key_without_exposing_value(
    value,
):
    docs = documents()
    upload = docs["agentkit.yaml"]["agentkit"]["project"]["upload"]
    if value is None:
        del upload["maxBytes"]
    else:
        upload["maxBytes"] = value

    def handle(req):
        return httpx.Response(
            200,
            json={"code": 0, "data": yaml.safe_dump(docs[req.url.params["dataId"]])},
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        provider = NacosConfigProvider(
            NacosClient(NacosBootstrap(server_url="http://registry"), http_client=http)
        )
        with pytest.raises(
            NacosError, match=r"agentkit\.project\.upload\.maxBytes"
        ) as exc:
            await provider.refresh()
        assert "4096" not in str(exc.value)
        assert not provider.is_connected
        with pytest.raises(RuntimeError):
            _ = provider.snapshot


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
        snapshot.model.catalog.models.clear()
        assert provider.snapshot.model.selection.default_key == "company"
        assert len(provider.snapshot.model.catalog.models) == 1


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
        docs["agentkit.yaml"]["agentkit"]["access"] = {"allowPrivateSelfUse": False}
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
        docs["agentkit.yaml"]["agentkit"]["client"] = {
            "nacos": {"refresh": {"intervalSeconds": 0.005}}
        }
        await provider.start()
        docs.update(documents(2))
        async with asyncio.timeout(1):
            while provider.snapshot.revision != 2:
                await asyncio.sleep(0.005)
        await provider.close()
        stopped = fetched
        await asyncio.sleep(0.02)
        assert fetched == stopped


@pytest.mark.asyncio
async def test_unregistered_platform_default_keeps_last_valid_snapshot():
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
        provider.bind_platform_tool_ids({"Read", "Write"})
        docs.update(documents(2))
        docs["agentkit.yaml"]["agentkit"]["tool"] = {
            "defaults": {"registeredIds": ["UnknownTool"]}
        }
        with pytest.raises(
            NacosError, match=r"agentkit\.tool\.defaults\.registeredIds"
        ):
            await provider.refresh()
        assert provider.snapshot == first
        assert provider.last_error is not None


def test_yaml_rejects_duplicate_agentkit_root():
    from egis_agentkit.client.nacos import load_agentkit_config

    content = yaml.safe_dump(documents()["agentkit.yaml"])
    with pytest.raises(NacosError, match="duplicate"):
        load_agentkit_config(content + content)


def test_checked_in_agentkit_example_validates():
    from pathlib import Path

    from egis_agentkit.client.nacos import load_agentkit_config

    example = Path(__file__).resolve().parents[1] / "examples/nacos/agentkit.yaml"
    settings = load_agentkit_config(example.read_text())
    assert settings.project.upload.max_bytes == 20_971_520
    assert settings.model.selection.default_key == "company"
