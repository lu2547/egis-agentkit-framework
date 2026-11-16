"""通过真实 Nacos Client 验证首次发布、重复执行及失败时的配置保护。"""

import importlib.util
from pathlib import Path
from urllib.parse import parse_qs

import httpx
import pytest
import yaml

from egis_agentkit.client.nacos import NacosBootstrap, NacosClient, NacosError

SCRIPT = Path(__file__).resolve().parents[1] / "scripts/init/nacos.py"
EXAMPLE = Path(__file__).resolve().parents[1] / "examples/nacos/agentkit.yaml"


def load_initialization():
    assert SCRIPT.is_file(), "缺少 Nacos 首次初始化脚本"
    spec = importlib.util.spec_from_file_location("initialization_nacos", SCRIPT)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


@pytest.mark.parametrize("missing_envelope", ["outer", "nested"])
async def test_missing_config_is_published_to_existing_namespace_and_read_back(
    missing_envelope,
):
    initialization = load_initialization()
    content = EXAMPLE.read_text(encoding="utf-8")
    saved = None
    methods = []

    def handle(request):
        nonlocal saved
        methods.append(request.method)
        if request.method == "GET":
            assert request.url.host == "server"
            assert request.url.path == "/nacos/v3/client/cs/config"
            assert dict(request.url.params) == {
                "namespaceId": "provided-namespace",
                "groupName": "provided-group",
                "dataId": "agentkit.yaml",
            }
            if saved is None and missing_envelope == "outer":
                return httpx.Response(200, json={"code": 20004, "data": None})
            return httpx.Response(
                200,
                json={
                    "code": 0,
                    "data": {
                        "success": saved is not None,
                        "content": saved,
                        "errorCode": 0 if saved is not None else 20004,
                    },
                },
            )
        assert request.url.host == "console"
        assert request.url.path == "/v3/console/cs/config"
        values = parse_qs(request.content.decode())
        assert values == {
            "namespaceId": ["provided-namespace"],
            "groupName": ["provided-group"],
            "dataId": ["agentkit.yaml"],
            "content": [content],
            "type": ["yaml"],
        }
        saved = values["content"][0]
        return httpx.Response(200, json={"code": 0, "data": True})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = NacosClient(
            NacosBootstrap(
                server_url="http://server:8848",
                console_url="http://console:8080",
                namespace_id="provided-namespace",
                group="provided-group",
            ),
            http_client=http,
        )
        assert await initialization.initialize(client, content) is True

    assert saved == content
    assert methods == ["GET", "POST", "GET"]


async def test_semantically_equal_existing_config_is_preserved():
    initialization = load_initialization()
    content = EXAMPLE.read_text(encoding="utf-8")
    document = yaml.safe_load(content)
    del document["agentkit"]["client"]["nacos"]
    stored = yaml.safe_dump(document, sort_keys=True)
    methods = []

    def handle(request):
        methods.append(request.method)
        return httpx.Response(200, json={"code": 0, "data": stored})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = NacosClient(NacosBootstrap(server_url="http://server"), http)
        assert await initialization.initialize(client, content) is False

    assert methods == ["GET"]


@pytest.mark.parametrize("change", ["revision", "model"])
async def test_different_existing_config_is_rejected_without_overwriting(change):
    initialization = load_initialization()
    stored = EXAMPLE.read_text(encoding="utf-8")
    original = stored
    document = yaml.safe_load(stored)
    if change == "revision":
        document["agentkit"]["revision"] = 2
    else:
        document["agentkit"]["model"]["catalog"]["models"][0]["model"] = (
            "private-deployment-marker"
        )
    content = yaml.safe_dump(document)
    methods = []

    def handle(request):
        nonlocal stored
        methods.append(request.method)
        if request.method == "POST":
            stored = parse_qs(request.content.decode())["content"][0]
            return httpx.Response(200, json={"code": 0, "data": True})
        return httpx.Response(200, json={"code": 0, "data": stored})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = NacosClient(NacosBootstrap(server_url="http://server"), http)
        with pytest.raises(NacosError, match="不同") as exc:
            await initialization.initialize(client, content)

    assert stored == original
    assert methods == ["GET"]
    assert "private-deployment-marker" not in str(exc.value)


@pytest.mark.parametrize("failure", ["transport", 403, 404, 500, "operation"])
async def test_query_errors_are_never_treated_as_missing_config(failure):
    initialization = load_initialization()
    methods = []

    def handle(request):
        methods.append(request.method)
        if failure == "transport":
            raise httpx.ConnectError("private-upstream-body", request=request)
        if failure == "operation":
            return httpx.Response(
                200, json={"code": 50001, "message": "private-upstream-body"}
            )
        return httpx.Response(
            failure, json={"code": 20004, "message": "private-upstream-body"}
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = NacosClient(NacosBootstrap(server_url="http://server"), http)
        with pytest.raises(NacosError) as exc:
            await initialization.initialize(client, EXAMPLE.read_text(encoding="utf-8"))

    assert methods == ["GET"]
    assert "private-upstream-body" not in str(exc.value)


@pytest.mark.parametrize("content", ["agentkit: [", "unexpected: private-config-body"])
async def test_invalid_config_is_rejected_before_any_network_request(content):
    initialization = load_initialization()
    requests = []

    def handle(request):
        requests.append(request)
        return httpx.Response(200, json={"code": 20004})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = NacosClient(NacosBootstrap(server_url="http://server"), http)
        with pytest.raises(NacosError) as exc:
            await initialization.initialize(client, content)

    assert requests == []
    assert "private-config-body" not in str(exc.value)


async def test_publication_readback_mismatch_fails():
    initialization = load_initialization()
    document = yaml.safe_load(EXAMPLE.read_text(encoding="utf-8"))
    document["agentkit"]["revision"] = 2
    changed = yaml.safe_dump(document)
    methods = []

    def handle(request):
        methods.append(request.method)
        if len(methods) == 1:
            return httpx.Response(200, json={"code": 20004})
        if request.method == "POST":
            return httpx.Response(200, json={"code": 0, "data": True})
        return httpx.Response(200, json={"code": 0, "data": changed})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        client = NacosClient(NacosBootstrap(server_url="http://server"), http)
        with pytest.raises(NacosError, match="读回"):
            await initialization.initialize(client, EXAMPLE.read_text(encoding="utf-8"))

    assert methods == ["GET", "POST", "GET"]


async def test_cli_uses_prepared_file_and_environment_without_loading_dotenv(
    monkeypatch, tmp_path, capsys,
):
    initialization = load_initialization()
    content = EXAMPLE.read_text(encoding="utf-8")
    config_file = tmp_path / "prepared.yaml"
    config_file.write_text(content, encoding="utf-8")
    (tmp_path / ".env").write_text("NACOS_GROUP=unexpected-dotenv-group\n")
    monkeypatch.chdir(tmp_path)
    monkeypatch.setattr("sys.argv", [str(SCRIPT), "--file", str(config_file)])
    monkeypatch.setenv("NACOS_SERVER_URL", "http://provided-server:8848")
    monkeypatch.setenv("NACOS_NAMESPACE_ID", "provided-namespace")
    monkeypatch.setenv("NACOS_IDENTITY_KEY", "test-identity")
    monkeypatch.setenv("NACOS_IDENTITY_VALUE", "private-identity")
    monkeypatch.setenv("NACOS_ACCESS_TOKEN", "private-token")
    monkeypatch.delenv("NACOS_CONSOLE_URL", raising=False)
    monkeypatch.delenv("NACOS_GROUP", raising=False)
    requests = []

    def handle(request):
        requests.append(request)
        assert request.url.host == "provided-server"
        assert request.url.params["namespaceId"] == "provided-namespace"
        assert request.url.params["groupName"] == "EGIS_AGENTKIT"
        assert request.headers["test-identity"] == "private-identity"
        assert request.headers["accessToken"] == "private-token"
        return httpx.Response(200, json={"code": 0, "data": content})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        monkeypatch.setattr(
            initialization, "NacosClient", lambda bootstrap: NacosClient(bootstrap, http)
        )
        await initialization.main()

    output = capsys.readouterr()
    assert len(requests) == 1
    assert "保留" in output.out
    assert "private-identity" not in output.out + output.err
    assert "private-token" not in output.out + output.err
    assert content not in output.out + output.err


async def test_cli_rejects_invalid_connection_without_printing_credentials(
    monkeypatch, capsys,
):
    initialization = load_initialization()
    monkeypatch.setattr("sys.argv", [str(SCRIPT), "--file", str(EXAMPLE)])
    monkeypatch.setenv("NACOS_SERVER_URL", "http://user:private-password@server")

    with pytest.raises(SystemExit) as exc:
        await initialization.main()

    output = capsys.readouterr()
    assert exc.value.code == 1
    assert "环境变量" in output.err
    assert "private-password" not in output.out + output.err
