"""管理连接器凭据、OAuth 授权、刷新和撤销。"""

from __future__ import annotations

import base64
import hashlib
import json
import os
import re
import secrets
from datetime import timedelta
from urllib.parse import parse_qsl, urlencode, urlsplit, urlunsplit

import httpx
from cryptography.fernet import Fernet, InvalidToken

from egis_agentkit.backend.nacos._settings import (
    ConnectorOAuthConfig,
)
from egis_agentkit.errors import AgentKitError, Forbidden
from egis_agentkit.storage.model import (
    utcnow,
)


def _https(url):
    if not isinstance(url, str) or any(ord(character) <= 32 for character in url):
        raise ValueError("MCP OAuth URL is required")
    parsed = urlsplit(url)
    if (
        parsed.scheme != "https"
        or not parsed.hostname
        or parsed.username
        or parsed.password
        or parsed.fragment
        or parsed.port == 0
    ):
        raise ValueError(
            "MCP OAuth URLs require HTTPS without user information or fragments"
        )
    return parsed


def _origin(parsed):
    return parsed.scheme, parsed.hostname, parsed.port or 443


def _digest(value):
    return hashlib.sha256(value.encode()).hexdigest()


class MCPAuthorizationService:
    def __init__(
        self, storage, *, key=None, redirect_uri=None, transport=None, config=None
    ):
        self.storage = storage.mcp_credentials
        self.config = config
        value = key or os.environ.get(self.settings.credential_key_env)
        self.cipher = Fernet(value) if value else None
        self.redirect_uri = redirect_uri
        self.transport = transport

    @property
    def settings(self) -> ConnectorOAuthConfig:
        if self.config is None:
            return ConnectorOAuthConfig(
                redirect_uri_env="AGENTKIT_MCP_OAUTH_REDIRECT_URI"
            )
        return self.config.snapshot.connector.oauth

    @staticmethod
    def validate_config(config):
        fields = {
            "transport",
            "url",
            "auth_type",
            "key_header",
            "oauth_client_id",
            "oauth_scope",
        }
        if (
            not isinstance(config, dict)
            or set(config) - fields
            or config.get("transport") != "streamable_http"
        ):
            raise ValueError("Only streamable HTTP MCP endpoints are supported")
        config = dict(config)
        method = config.get("auth_type", "none")
        if method not in ("none", "api_key", "oauth"):
            raise ValueError("Unknown MCP authentication method")
        config["auth_type"] = method
        allowed = {"transport", "url", "auth_type"}
        if method != "none":
            _https(config.get("url"))
        if method == "api_key":
            allowed.add("key_header")
            header = config.get("key_header", "Authorization")
            if (
                not isinstance(header, str)
                or not re.fullmatch(r"[A-Za-z][A-Za-z0-9-]{0,63}", header)
                or header.lower()
                in {
                    "host",
                    "connection",
                    "content-length",
                    "transfer-encoding",
                    "cookie",
                    "proxy-authorization",
                    "mcp-session-id",
                }
            ):
                raise ValueError("Invalid MCP API key header")
            config["key_header"] = header
        if method == "oauth":
            allowed.update({"oauth_client_id", "oauth_scope"})
            for field in ("oauth_client_id", "oauth_scope"):
                if field in config and (
                    not isinstance(config[field], str)
                    or len(config[field]) > 1024
                    or any(ord(c) < 32 for c in config[field])
                ):
                    raise ValueError("Invalid MCP OAuth client configuration")
        if set(config) - allowed:
            raise ValueError(
                "MCP authentication options must match the selected method"
            )
        return config

    def _config(self, asset, expected=None):
        config = asset["payload"].get("config", {})
        if expected is not None and config.get("auth_type") != expected:
            raise ValueError("MCP connection authentication method does not match")
        return config

    def _connection(self, asset):
        return _digest(
            json.dumps(
                [
                    asset["resource_id"],
                    asset["resource_version"],
                    asset["checksum_sha256"],
                ]
            )
        )

    def _encrypt(self, value):
        if self.cipher is None:
            raise AgentKitError("MCP 凭证存储未配置", 503)
        return self.cipher.encrypt(json.dumps(value).encode())

    def _decrypt(self, value):
        if self.cipher is None:
            raise AgentKitError("MCP 凭证存储未配置", 503)
        try:
            return json.loads(self.cipher.decrypt(value))
        except (InvalidToken, ValueError):
            raise AgentKitError("MCP 凭证不可读取，请重新授权", 503) from None

    def _http(self, settings):
        return httpx.AsyncClient(
            transport=self.transport,
            timeout=settings.timeout_seconds,
            follow_redirects=False,
        )

    @staticmethod
    def _header_value(value):
        if (
            not isinstance(value, str)
            or not value
            or len(value) > 8192
            or any(not 33 <= ord(c) <= 126 for c in value)
        ):
            raise ValueError("Invalid MCP credential")
        return value

    async def set_api_key(self, user, asset, *, key):
        self._config(asset, "api_key")
        credential = self._encrypt({"key": self._header_value(key)})
        identity = user.user_id, self._connection(asset)
        async with self.storage.write() as credentials:
            await credentials.lock(*identity)
            await credentials.update(*identity, credential=credential)
        return {"authorized": True}

    async def status(self, user, asset):
        settings = self.settings
        method = self._config(asset).get("auth_type", "none")
        row = await self.storage.read(user.user_id, self._connection(asset))
        authorized = method == "none"
        if row and row["credential"]:
            credential = self._decrypt(row["credential"])
            authorized = method == "api_key" or bool(
                credential.get("refresh_token") or self._fresh(credential, settings)
            )
        return {"auth_type": method, "authorized": authorized}

    async def _discover(self, http, config):
        resource = config["url"]
        server = _https(resource)
        probe = await http.get(resource)
        challenge = (
            probe.headers.get("www-authenticate", "")
            if probe.status_code == 401
            else ""
        )

        def parameter(name):
            match = re.search(r"(?:\s|,)" + name + r'="([^"]+)"', challenge, re.I)
            return match.group(1) if match else None

        root = f"{server.scheme}://{server.netloc}"
        advertised = parameter("resource_metadata")
        if advertised:
            if _origin(_https(advertised)) != _origin(server):
                raise ValueError(
                    "MCP protected-resource metadata must come from the resource origin"
                )
            candidates = [advertised]
        else:
            well_known = root + "/.well-known/oauth-protected-resource"
            candidates = list(
                dict.fromkeys([well_known + server.path.rstrip("/"), well_known])
            )
        metadata = await self._metadata(http, candidates)
        registered_resource = _https(metadata.get("resource"))
        if _origin(registered_resource) != _origin(server) or (
            registered_resource.path.rstrip("/") != server.path.rstrip("/")
            or registered_resource.query
        ):
            raise ValueError(
                "OAuth protected resource does not match the MCP connection"
            )
        issuers = metadata.get("authorization_servers", [])
        if not isinstance(issuers, list) or not issuers:
            raise ValueError("No MCP authorization server was advertised")
        issuer = issuers[0]
        auth = _https(issuer)
        if auth.query:
            raise ValueError("MCP OAuth issuer cannot have a query")
        root = f"{auth.scheme}://{auth.netloc}"
        path = auth.path.rstrip("/")
        authorization = await self._metadata(
            http,
            [
                root + "/.well-known/oauth-authorization-server" + path,
                root + "/.well-known/openid-configuration" + path,
                root + path + "/.well-known/openid-configuration",
            ],
        )
        if authorization.get("issuer") != issuer or "S256" not in authorization.get(
            "code_challenge_methods_supported", []
        ):
            raise ValueError(
                "MCP authorization server must advertise its issuer and PKCE S256"
            )
        for key in ("authorization_endpoint", "token_endpoint"):
            _https(authorization.get(key))
        scope = parameter("scope") or config.get("oauth_scope")
        if scope is None:
            scopes = metadata.get("scopes_supported", [])
            if not isinstance(scopes, list) or any(
                not isinstance(value, str) for value in scopes
            ):
                raise ValueError("MCP OAuth scope metadata is invalid")
            scope = " ".join(scopes)
        return authorization, scope

    @staticmethod
    async def _metadata(http, urls):
        for url in dict.fromkeys(urls):
            response = await http.get(url, headers={"Accept": "application/json"})
            if response.status_code == 200:
                data = response.json()
                if isinstance(data, dict):
                    return data
        raise ValueError("MCP OAuth metadata could not be discovered")

    async def start(self, user, asset):
        settings = self.settings
        config = self._config(asset, "oauth")
        redirect_uri = self.redirect_uri or settings.redirect_uri
        if not redirect_uri and settings.redirect_uri_env:
            redirect_uri = os.environ.get(settings.redirect_uri_env)
        if self.cipher is None or not redirect_uri:
            raise AgentKitError("MCP OAuth 客户端未配置", 503)
        _https(redirect_uri)
        verifier = secrets.token_urlsafe(48)
        state = secrets.token_urlsafe(32)
        code_challenge = (
            base64.urlsafe_b64encode(hashlib.sha256(verifier.encode()).digest())
            .decode()
            .rstrip("=")
        )
        try:
            async with self._http(settings) as http:
                server, scope = await self._discover(http, config)
                client_id = config.get("oauth_client_id")
                if not client_id and server.get("registration_endpoint"):
                    registration = server["registration_endpoint"]
                    _https(registration)
                    response = await http.post(
                        registration,
                        json={
                            "client_name": "Egis AgentKit",
                            "redirect_uris": [redirect_uri],
                            "grant_types": ["authorization_code", "refresh_token"],
                            "response_types": ["code"],
                            "token_endpoint_auth_method": "none",
                        },
                    )
                    response.raise_for_status()
                    client_id = response.json().get("client_id")
                if not isinstance(client_id, str) or not client_id:
                    raise ValueError("OAuth public client registration is required")
        except (httpx.HTTPError, ValueError, TypeError, AttributeError):
            raise AgentKitError("MCP OAuth 服务发现或注册失败", 502) from None
        params = {
            "response_type": "code",
            "client_id": client_id,
            "redirect_uri": redirect_uri,
            "resource": config["url"],
            "state": state,
            "code_challenge": code_challenge,
            "code_challenge_method": "S256",
        }
        if scope:
            params["scope"] = scope
        endpoint = urlsplit(server["authorization_endpoint"])
        query = [
            (key, value)
            for key, value in parse_qsl(endpoint.query)
            if key not in params
        ]
        url = urlunsplit(endpoint._replace(query=urlencode([*query, *params.items()])))
        pending = {
            "client_id": client_id,
            "token_endpoint": server["token_endpoint"],
            "revocation_endpoint": server.get("revocation_endpoint"),
            "resource": config["url"],
            "redirect_uri": redirect_uri,
            "code_verifier": verifier,
        }
        identity = user.user_id, self._connection(asset)
        async with self.storage.write() as credentials:
            await credentials.lock(*identity)
            await credentials.update(
                *identity,
                oauth_state_hash=_digest(state),
                oauth_pending=self._encrypt(pending),
                oauth_expires_at=utcnow()
                + timedelta(seconds=settings.state_lifetime_seconds),
            )
        return {"authorization_url": url, "state": state}

    async def callback(self, user, asset, *, state, code):
        settings = self.settings
        identity = user.user_id, self._connection(asset)
        self._config(asset, "oauth")
        credential = None
        async with self.storage.write() as credentials:
            row = await credentials.lock(*identity)
            if (
                not row["oauth_state_hash"]
                or not secrets.compare_digest(row["oauth_state_hash"], _digest(state))
                or row["oauth_expires_at"] <= utcnow()
            ):
                raise Forbidden("OAuth state 已失效")
            pending = self._decrypt(row["oauth_pending"])
            await credentials.update(
                *identity,
                oauth_state_hash=None,
                oauth_pending=None,
                oauth_expires_at=None,
            )
            if code:
                # 交换与写回同一凭据，防止并发撤销后重新写入令牌。
                credential = await self._exchange(
                    pending,
                    settings,
                    grant_type="authorization_code",
                    code=code,
                    redirect_uri=pending.pop("redirect_uri"),
                    code_verifier=pending.pop("code_verifier"),
                )
            if credential:
                await credentials.update(
                    *identity, credential=self._encrypt(credential)
                )
        if not credential:
            raise Forbidden("MCP OAuth 授权失败，请重新授权")
        return {"authorized": True}

    async def _exchange(self, details, settings, **params):
        try:
            async with self._http(settings) as http:
                response = await http.post(
                    details["token_endpoint"],
                    data={
                        "client_id": details["client_id"],
                        "resource": details["resource"],
                        **params,
                    },
                )
                if (
                    response.status_code == 400
                    and response.json().get("error") == "invalid_grant"
                ):
                    return None
                response.raise_for_status()
                data = response.json()
            if str(data.get("token_type", "")).lower() != "bearer":
                raise ValueError("Expected bearer token")
            access = self._header_value(data["access_token"])
            seconds = int(data.get("expires_in", settings.token_lifetime_seconds))
            if not 0 <= seconds <= settings.max_token_lifetime_seconds:
                raise ValueError("Invalid token expiry")
            refresh = data.get("refresh_token", details.get("refresh_token"))
            if refresh:
                refresh = self._header_value(refresh)
            return {
                **details,
                "access_token": access,
                "refresh_token": refresh,
                "expires_at": (utcnow() + timedelta(seconds=seconds)).timestamp(),
            }
        except (httpx.HTTPError, KeyError, TypeError, ValueError, AttributeError):
            raise AgentKitError("MCP OAuth 令牌交换失败", 502) from None

    @staticmethod
    def _fresh(credential, settings):
        return (
            credential.get("expires_at", 0)
            > utcnow().timestamp() + settings.refresh_window_seconds
        )

    async def headers(self, user, asset):
        settings = self.settings
        config = self._config(asset)
        method = config.get("auth_type", "none")
        if method == "none":
            return {}
        identity = user.user_id, self._connection(asset)
        row = await self.storage.read(*identity)
        credential = (
            self._decrypt(row["credential"]) if row and row["credential"] else None
        )
        if not credential:
            raise Forbidden("请先授权此 MCP 连接")
        if method == "api_key":
            header = config["key_header"]
            key = credential["key"]
            return {
                header: "Bearer " + key if header.lower() == "authorization" else key
            }
        if not self._fresh(credential, settings):
            async with self.storage.write() as credentials:
                row = await credentials.lock(*identity)
                credential = (
                    self._decrypt(row["credential"]) if row["credential"] else None
                )
                if credential and not self._fresh(credential, settings):
                    if credential.get("refresh_token"):
                        # 提供方可轮换刷新令牌，串行交换避免重复使用旧令牌。
                        credential = await self._exchange(
                            credential,
                            settings,
                            grant_type="refresh_token",
                            refresh_token=credential["refresh_token"],
                        )
                    else:
                        credential = None
                    await credentials.update(
                        *identity,
                        credential=self._encrypt(credential) if credential else None,
                    )
        if not credential or credential["expires_at"] <= utcnow().timestamp():
            raise Forbidden("MCP 授权已过期，请重新授权")
        return {"Authorization": "Bearer " + credential["access_token"]}

    async def revoke(self, user, asset):
        settings = self.settings
        if not user.is_enabled:
            raise Forbidden()
        identity = user.user_id, self._connection(asset)
        async with self.storage.write() as credentials:
            row = await credentials.lock(*identity)
            encrypted = row["credential"]
            await credentials.update(
                *identity,
                credential=None,
                oauth_state_hash=None,
                oauth_pending=None,
                oauth_expires_at=None,
            )
        try:
            credential = self._decrypt(encrypted) if encrypted else None
        except AgentKitError:
            return {"authorized": False, "provider_revoked": False}
        if credential and credential.get("revocation_endpoint"):
            try:
                url = credential["revocation_endpoint"]
                _https(url)
                async with self._http(settings) as http:
                    response = await http.post(
                        url,
                        data={
                            "client_id": credential["client_id"],
                            "token": credential.get("refresh_token")
                            or credential["access_token"],
                        },
                    )
                    response.raise_for_status()
            except (httpx.HTTPError, ValueError):
                return {"authorized": False, "provider_revoked": False}
        return {"authorized": False}
