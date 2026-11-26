from types import SimpleNamespace
from urllib.parse import parse_qs, urlsplit

import httpx
import pytest
from cryptography.fernet import Fernet

from egis_agentkit.client.nacos._settings import ConnectorOAuthConfig
from egis_agentkit.core.mcp import MCPAuthorization
from egis_agentkit.errors import AgentKitError

ASSET = {
    "resource_id": "oauth-setup",
    "checksum_sha256": "fixture",
    "payload": {
        "config": {
            "transport": "streamable_http",
            "url": "https://mcp.example/mcp",
            "auth_type": "oauth",
        }
    },
}
USER = SimpleNamespace(user_id="oauth-setup-user")
CALLBACK = "https://app.example/connectors/oauth/callback"


@pytest.fixture
def authorization(storage, monkeypatch):
    monkeypatch.setenv("AGENTKIT_MCP_CREDENTIAL_KEY", Fernet.generate_key().decode())
    monkeypatch.delenv("AGENTKIT_MCP_OAUTH_REDIRECT_URI", raising=False)

    def create(*, server_overrides=None, **settings):
        def respond(request):
            path = request.url.path
            if path == "/mcp":
                return httpx.Response(401)
            if path == "/.well-known/oauth-protected-resource/mcp":
                return httpx.Response(
                    200,
                    json={
                        "resource": "https://mcp.example/mcp",
                        "authorization_servers": ["https://auth.example"],
                    },
                )
            if path == "/.well-known/oauth-authorization-server":
                return httpx.Response(
                    200,
                    json={
                        "issuer": "https://auth.example",
                        "authorization_endpoint": "https://auth.example/authorize",
                        "token_endpoint": "https://auth.example/token",
                        "registration_endpoint": "https://auth.example/register",
                        "code_challenge_methods_supported": ["S256"],
                        **(server_overrides or {}),
                    },
                )
            if path == "/register" and request.method == "POST":
                return httpx.Response(201, json={"client_id": "public-client"})
            raise AssertionError("Unexpected OAuth provider request")

        config = SimpleNamespace(
            snapshot=SimpleNamespace(
                connector=SimpleNamespace(oauth=ConnectorOAuthConfig(**settings))
            )
        )
        return MCPAuthorization(
            storage.mcp_credentials,
            config=config,
            transport=httpx.MockTransport(respond),
        )

    return create


async def test_nacos_defaults_use_callback_environment(authorization, monkeypatch):
    monkeypatch.setenv("AGENTKIT_MCP_OAUTH_REDIRECT_URI", CALLBACK)
    result = await authorization().start(USER, ASSET)
    query = parse_qs(urlsplit(result["authorization_url"]).query)
    assert query["redirect_uri"] == [CALLBACK]
    assert query["code_challenge_method"] == ["S256"]


async def test_explicit_callback_overrides_environment(authorization, monkeypatch):
    monkeypatch.setenv(
        "AGENTKIT_MCP_OAUTH_REDIRECT_URI", "https://other.example/callback"
    )
    result = await authorization(redirect_uri=CALLBACK).start(USER, ASSET)
    assert parse_qs(urlsplit(result["authorization_url"]).query)["redirect_uri"] == [
        CALLBACK
    ]


async def test_explicit_callback_environment_mapping(authorization, monkeypatch):
    monkeypatch.setenv("TEST_OAUTH_CALLBACK", CALLBACK)
    result = await authorization(redirect_uri_env="TEST_OAUTH_CALLBACK").start(
        USER, ASSET
    )
    assert parse_qs(urlsplit(result["authorization_url"]).query)["redirect_uri"] == [
        CALLBACK
    ]


async def test_missing_key_explains_encryption_configuration(authorization, monkeypatch):
    monkeypatch.delenv("AGENTKIT_MCP_CREDENTIAL_KEY")
    with pytest.raises(AgentKitError, match="MCP 凭证加密密钥未配置") as error:
        await authorization(redirect_uri=CALLBACK).start(USER, ASSET)
    assert error.value.status_code == 503


async def test_missing_callback_explains_deployment_configuration(authorization):
    with pytest.raises(AgentKitError, match="^请联系管理员配置 OAuth 回调地址$") as error:
        await authorization().start(USER, ASSET)
    assert error.value.status_code == 503


@pytest.mark.parametrize(
    "callback",
    [
        "http://localhost:5179/connectors/oauth/callback",
        "http://127.0.0.1:5179/connectors/oauth/callback",
        "http://127.42.0.1:5179/connectors/oauth/callback",
        "http://[::1]:5179/connectors/oauth/callback",
    ],
)
async def test_explicit_loopback_http_callback_keeps_browser_origin(
    authorization, callback
):
    result = await authorization(redirect_uri=callback).start(USER, ASSET)
    assert parse_qs(urlsplit(result["authorization_url"]).query)["redirect_uri"] == [
        callback
    ]


@pytest.mark.parametrize(
    "callback",
    [
        "http://app.example/connectors/oauth/callback",
        "http://localhost.evil.example/callback",
        "http://192.168.1.1/callback",
        "http://2130706433/callback",
        "http://user@localhost/callback",
        "http://@localhost/callback",
        "http://:@localhost/callback",
        "http://localhost/callback#fragment",
        "http://local\nhost/callback",
        "http://localhost:0/callback",
    ],
)
async def test_unsafe_callback_reports_deployment_error(authorization, callback):
    with pytest.raises(AgentKitError, match="MCP OAuth 回调地址无效") as error:
        await authorization(redirect_uri=callback).start(USER, ASSET)
    assert error.value.status_code == 503


@pytest.mark.parametrize(
    "endpoint", ["authorization_endpoint", "token_endpoint", "registration_endpoint"]
)
async def test_loopback_http_exception_does_not_apply_to_provider_endpoints(
    authorization, endpoint
):
    with pytest.raises(AgentKitError, match="MCP OAuth 服务发现或注册失败") as error:
        await authorization(
            redirect_uri=CALLBACK,
            server_overrides={endpoint: "http://localhost/endpoint"},
        ).start(USER, ASSET)
    assert error.value.status_code == 502


def test_loopback_http_exception_does_not_apply_to_mcp_server():
    with pytest.raises(ValueError):
        MCPAuthorization.validate_config(
            {
                "transport": "streamable_http",
                "url": "http://localhost:8766/mcp",
                "auth_type": "oauth",
            }
        )
