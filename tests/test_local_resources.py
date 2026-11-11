import zipfile
from io import BytesIO

import pytest
from test_domain import identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.service.resource._catalog import ResourceService


async def test_connector_dependency_permits_credentials_without_marketplace_access(
    storage,
    tmp_path,
):
    from cryptography.fernet import Fernet

    from egis_agentkit.service.access._grant import GrantService
    from egis_agentkit.service.agent._expert_definition import ExpertDefinition
    from egis_agentkit.storage.model import Definition, Grant

    _, admin, _, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path, credential_key=Fernet.generate_key())
    await resources.save_mcp(
        admin,
        {
            "resource_id": "scoped-connector",
            "owner_type": "system",
            "config": {
                "transport": "streamable_http",
                "url": "https://mcp.example.com/",
                "auth_type": "api_key",
            },
        },
    )
    await resources.save_skill(
        admin,
        {
            "resource_id": "scoped-skill",
            "owner_type": "system",
            "markdown": "# Scoped",
        },
    )
    agent = await ExpertDefinition(storage).build(
        admin,
        Definition(
            owner_type="system",
            owner_id="system",
            agent_name="scoped",
            agent_display_name="Scoped",
            payload={
                "bindings": [
                    {
                        "resource_type": "skill",
                        "resource_id": "scoped-skill",
                        "resource_version": "1.0.0",
                        "resource_view_type": "system",
                    },
                    {
                        "resource_type": "mcp",
                        "resource_id": "scoped-connector",
                        "resource_version": "1.0.0",
                        "resource_view_type": "system",
                    },
                ]
            },
        ),
    )
    grants = GrantService(storage)
    root = await grants.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=bob.user_id,
            resource_type="agent",
            resource_id=agent["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="system",
        ),
    )
    assert await resources.list(bob, "mcp") == []
    assert [
        row["resource_id"] for row in await resources.mcp_dependency_authorizations(bob)
    ] == [
        "scoped-connector",
    ]
    assert (
        await resources.mcp_auth_action(
            bob,
            "scoped-connector",
            "1.0.0",
            "set_api_key",
            key="bob-only",
        )
    )["authorized"]
    with pytest.raises(Forbidden):
        await resources.installations.install(bob, "mcp", "scoped-connector", "1.0.0")
    await resources.set_asset_enabled(admin, "skill", "scoped-skill", "1.0.0", False)
    assert await resources.mcp_dependency_authorizations(bob) == []
    with pytest.raises(Forbidden):
        await resources.mcp_auth_action(bob, "scoped-connector", "1.0.0", "status")
    await grants.revoke(admin, root["id"])
    assert await resources.mcp_dependency_authorizations(bob) == []
    with pytest.raises(Forbidden):
        await resources.mcp_auth_action(bob, "scoped-connector", "1.0.0", "status")


@pytest.mark.asyncio
async def test_local_skill_archive_is_durable_and_access_checked(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_skill(
        alice,
        {
            "resource_id": "local-report",
            "resource_version": "1.0.0",
            "markdown": "---\nname: report\n---\n# Original",
        },
    )
    await resources.save_skill(
        bob,
        {
            "resource_id": "same-content",
            "resource_version": "1.0.0",
            "markdown": "---\nname: report\n---\n# Original",
        },
    )
    restarted = ResourceService(storage, tmp_path)
    for user, rid in ((alice, "local-report"), (bob, "same-content")):
        content = await restarted.download_skill(user, rid, "1.0.0")
        with zipfile.ZipFile(BytesIO(content)) as archive:
            assert archive.read(f"{rid}/SKILL.md").endswith(b"# Original")
    with pytest.raises(Forbidden):
        await restarted.download_skill(bob, "local-report", "1.0.0")
    await restarted.set_asset_enabled(alice, "skill", "local-report", "1.0.0", False)
    with pytest.raises(Forbidden):
        await restarted.download_skill(alice, "local-report", "1.0.0")


@pytest.mark.asyncio
async def test_local_mcp_install_stores_no_credentials(storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.save_mcp(
        alice,
        {
            "resource_id": "connector",
            "config": {
                "transport": "streamable_http",
                "url": "https://mcp.example.com/",
                "auth_type": "api_key",
            },
        },
    )
    await resources.installations.install(alice, "mcp", "connector", "1.0.0")
    records = await storage.list_mcps("alice")
    assert len(records) == 1
    assert not records[0].client.mcp_config.headers
    with pytest.raises(Forbidden):
        await resources.mcps_for_refs(
            alice,
            [
                {
                    "resource_type": "mcp",
                    "resource_id": "connector",
                    "resource_version": "1.0.0",
                }
            ],
        )


@pytest.mark.asyncio
async def test_oauth_state_credentials_refresh_and_revocation_are_isolated(
    storage, tmp_path
):
    import asyncio
    import base64
    import hashlib
    from urllib.parse import parse_qs

    import httpx
    from cryptography.fernet import Fernet

    from egis_agentkit.service.resource._connector_auth import MCPAuthorizationService
    from egis_agentkit.storage.model import Grant

    _, admin, alice, bob = await identities(storage)
    refreshes = []
    exchanges = []
    revocations = []

    async def request(req):
        path = req.url.path
        if path == "/mcp":
            return httpx.Response(
                401,
                headers={
                    "www-authenticate": 'Bearer resource_metadata="https://mcp.example.com/resource", scope="read"',
                },
            )
        if path == "/resource":
            return httpx.Response(
                200,
                json={
                    "resource": "https://mcp.example.com/mcp",
                    "authorization_servers": ["https://auth.example.com"],
                },
            )
        if path == "/.well-known/oauth-authorization-server":
            return httpx.Response(
                200,
                json={
                    "issuer": "https://auth.example.com",
                    "authorization_endpoint": "https://auth.example.com/authorize",
                    "token_endpoint": "https://auth.example.com/token",
                    "revocation_endpoint": "https://auth.example.com/revoke",
                    "code_challenge_methods_supported": ["S256"],
                },
            )
        data = parse_qs(req.content.decode())
        if path == "/revoke":
            revocations.append(data["token"][0])
            return httpx.Response(200)
        assert path == "/token"
        if data["grant_type"] == ["authorization_code"]:
            exchanges.append(data)
            return httpx.Response(
                200,
                json={
                    "token_type": "Bearer",
                    "access_token": "expired-alice",
                    "refresh_token": "refresh-alice",
                    "expires_in": 0,
                },
            )
        refreshes.append(data["refresh_token"][0])
        return httpx.Response(
            200,
            json={
                "token_type": "Bearer",
                "access_token": "fresh-alice",
                "expires_in": 3600,
            },
        )

    key = Fernet.generate_key()
    resources = ResourceService(storage, tmp_path, credential_key=key)
    resources.mcp_authorization = MCPAuthorizationService(
        storage,
        key=key,
        redirect_uri="https://app.example.com/connectors",
        transport=httpx.MockTransport(request),
    )
    await resources.save_mcp(
        admin,
        {
            "resource_id": "protected",
            "owner_type": "system",
            "config": {
                "transport": "streamable_http",
                "url": "https://mcp.example.com/mcp",
                "auth_type": "oauth",
                "oauth_client_id": "client-id",
            },
        },
    )
    for user in (alice, bob):
        await resources.grants.grant(
            admin,
            Grant(
                principal_type="user",
                principal_id=user.user_id,
                resource_type="mcp",
                resource_id="protected",
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="system",
            ),
        )

    async def auth(user, action, **kwargs):
        return await resources.mcp_auth_action(
            user, "protected", "1.0.0", action, **kwargs
        )

    flow = await auth(alice, "start")
    query = httpx.URL(flow["authorization_url"]).params
    assert (
        query["scope"] == "read" and query["resource"] == "https://mcp.example.com/mcp"
    )
    with pytest.raises(Forbidden):
        await auth(bob, "callback", state=flow["state"], code="code")
    await auth(alice, "callback", state=flow["state"], code="code")
    with pytest.raises(Forbidden):
        await auth(alice, "callback", state=flow["state"], code="code")
    verifier = exchanges[0]["code_verifier"][0]
    assert query["code_challenge"] == base64.urlsafe_b64encode(
        hashlib.sha256(verifier.encode()).digest()
    ).decode().rstrip("=")

    ref = [
        {
            "resource_type": "mcp",
            "resource_id": "protected",
            "resource_version": "1.0.0",
        }
    ]
    with pytest.raises(Forbidden):
        await resources.mcps_for_refs(bob, ref)
    clients = await asyncio.gather(
        *(resources.mcps_for_refs(alice, ref) for _ in range(3))
    )
    assert refreshes == ["refresh-alice"]
    assert all(
        row[0].mcp_config.headers == {"Authorization": "Bearer fresh-alice"}
        for row in clients
    )
    await auth(alice, "revoke")
    assert revocations == ["refresh-alice"]
    with pytest.raises(Forbidden):
        await resources.mcps_for_refs(alice, ref)


@pytest.mark.asyncio
async def test_system_default_workspace_tools_load_without_installation_or_grant(
    storage, tmp_path
):
    from types import SimpleNamespace

    from agentscope.tool import Bash, Read, Toolkit
    from test_read_tool_guard import execute

    from egis_agentkit.errors import Conflict
    from egis_agentkit.extension._session_lock_override import execution_scope
    from egis_agentkit.extension._workspace_manager_impl import (
        EgisLocalWorkspaceManager,
    )

    _, admin, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.seed_builtin_tools()
    manager = EgisLocalWorkspaceManager(tmp_path, resources=resources)
    workspace = await manager.get_workspace(alice.user_id, "agent", "one", "project")
    scope = SimpleNamespace(
        user_id=alice.user_id,
        session_id="one",
        project_id="project",
        definition={"agent_type": "personal"},
    )
    token = execution_scope.set(scope)
    try:
        tools = {tool.name: tool for tool in await workspace.list_tools()}
        assert "Read" in tools and ("Bash" in tools or "PowerShell" in tools)
        tool = tools["Read"]
        assert type(tool) is Read
        registry = resources.tool_registry
        assert await registry.authorize(
            alice, scope, None, tool, {"file_path": "/tmp/input.md"}
        )
        scope.definition = {"agent_type": "expert"}
        assert await registry.authorize(alice, scope, None, tool)
        with pytest.raises(Forbidden):
            await registry.authorize(alice, scope, None, tool, {"file_path": 123})
        impostor = Bash()
        impostor.name = "Read"
        with pytest.raises(Forbidden):
            await registry.authorize(alice, scope, None, impostor)

        toolkit = Toolkit()
        await toolkit.add_tool(tool)
        plain_text = tmp_path / "sample.md"
        plain_text.write_text("正常读取")
        assert (await execute(toolkit, plain_text))[-1].state == "success"
        binary = tmp_path / "sample.docx"
        binary.write_bytes(b"PK\0binary")
        assert (await execute(toolkit, binary))[-1].state == "error"

        view = await resources.detail(alice, "tool", "Read", "1.0.0")
        assert view["is_system_default"] and view["is_installed"] and view["is_enabled"]
        with pytest.raises(Forbidden):
            await resources.installations.set_installation(alice, "tool", "Read", "1.0.0", False)
        with pytest.raises(Conflict):
            await resources.set_asset_enabled(
                admin, "tool", "Read", "1.0.0", False, admin=True
            )
    finally:
        execution_scope.reset(token)
        await manager.close_all()


@pytest.mark.asyncio
async def test_native_mcp_uses_each_users_encrypted_api_key(
    storage, tmp_path, monkeypatch
):
    import asyncio
    import datetime
    import ipaddress
    import socket

    import uvicorn
    from cryptography import x509
    from cryptography.fernet import Fernet
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import rsa
    from mcp.server.mcpserver import MCPServer
    from starlette.responses import Response

    from egis_agentkit.storage.model import Grant

    key = rsa.generate_private_key(public_exponent=65537, key_size=2048)
    name = x509.Name([x509.NameAttribute(x509.oid.NameOID.COMMON_NAME, "localhost")])
    now = datetime.datetime.now(datetime.timezone.utc)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(name)
        .issuer_name(name)
        .public_key(key.public_key())
        .serial_number(x509.random_serial_number())
        .not_valid_before(now - datetime.timedelta(days=1))
        .not_valid_after(now + datetime.timedelta(days=1))
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .sign(key, hashes.SHA256())
    )
    cert_path, key_path = tmp_path / "cert.pem", tmp_path / "key.pem"
    cert_path.write_bytes(certificate.public_bytes(serialization.Encoding.PEM))
    key_path.write_bytes(
        key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    monkeypatch.setenv("SSL_CERT_FILE", str(cert_path))

    mcp = MCPServer("test-protected")

    @mcp.tool()
    def echo(text: str) -> str:
        return text

    app = mcp.streamable_http_app(stateless_http=True, json_response=True)
    calls = []

    async def require_key(scope, receive, send):
        if scope["type"] == "http":
            headers = dict(scope["headers"])
            auth = headers.get(b"x-api-key")
            if auth not in (b"key-alice", b"key-bob"):
                await Response(status_code=401)(scope, receive, send)
                return
            calls.append(auth)
        await app(scope, receive, send)

    sock = socket.socket()
    sock.bind(("127.0.0.1", 0))
    address = sock.getsockname()
    server = uvicorn.Server(
        uvicorn.Config(
            require_key,
            ssl_certfile=str(cert_path),
            ssl_keyfile=str(key_path),
            log_config=None,
        )
    )
    process = asyncio.create_task(server.serve(sockets=[sock]))
    try:
        async with asyncio.timeout(5):
            while not server.started:
                await asyncio.sleep(0.01)
        _, admin, alice, bob = await identities(storage)
        resources = ResourceService(
            storage, tmp_path, credential_key=Fernet.generate_key()
        )
        await resources.save_mcp(
            admin,
            {
                "resource_id": "actual-mcp",
                "owner_type": "system",
                "config": {
                    "transport": "streamable_http",
                    "url": f"https://{address[0]}:{address[1]}/mcp",
                    "auth_type": "api_key",
                    "key_header": "X-API-Key",
                },
            },
        )
        for user in (alice, bob):
            await resources.grants.grant(
                admin,
                Grant(
                    principal_type="user",
                    principal_id=user.user_id,
                    resource_type="mcp",
                    resource_id="actual-mcp",
                    resource_view_type="system",
                    resource_owner_type="system",
                    resource_owner_id="system",
                ),
            )
            await resources.installations.install(user, "mcp", "actual-mcp", "1.0.0")
            await resources.mcp_auth_action(
                user, "actual-mcp", "1.0.0", "set_api_key", key=f"key-{user.user_id}"
            )
            asset = await resources._asset("mcp", "actual-mcp", "1.0.0")
            auth = resources.mcp_authorization
            persisted = await auth.storage.read(user.user_id, auth._connection(asset))
            assert f"key-{user.user_id}".encode() not in persisted["credential"]
            assert not (await storage.list_mcps(user.user_id))[
                0
            ].client.mcp_config.headers
            ref = [
                {
                    "resource_type": "mcp",
                    "resource_id": "actual-mcp",
                    "resource_version": "1.0.0",
                }
            ]
            (client,) = await resources.mcps_for_refs(user, ref)
            before = len(calls)
            await client.connect()
            try:
                (tool,) = await client.list_tools()
                response = await tool(text="through-native")
                assert "through-native" in str(response)
            finally:
                await client.close()
            assert calls[before:] and set(calls[before:]) == {
                f"key-{user.user_id}".encode()
            }
    finally:
        server.should_exit = True
        await process
        sock.close()
