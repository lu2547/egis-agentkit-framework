"""通过 HTTP 边界验证 CCP 协议、会话归属及凭据脱敏。"""

import asyncio
import ipaddress
import logging
import ssl
import traceback
from datetime import datetime, timedelta, timezone

import httpx
import pytest
from pydantic import SecretStr

from egis_agentkit.client.nacos._settings import CyberArkConfig


def ccp_settings(**changes):
    """使用不含真实公司凭据的 CCP 配置。"""
    fields = dict(
        endpoint="https://ccp.test/AIMWebService/api/Accounts",
        app_id="application-private-id",
        safe="database-private-safe",
        object_name="database-private-object",
    )
    fields.update(changes)
    return CyberArkConfig(**fields)


async def test_ccp_get_password_uses_account_selectors_and_preserves_secret(caplog):
    """错误参数名称或返回明文字符串都会破坏 CCP 取密契约。"""
    from egis_agentkit.client._cyberark import CyberArkClient

    caplog.set_level(logging.DEBUG)

    def provider(request):
        assert request.method == "GET"
        assert request.url.path == "/AIMWebService/api/Accounts"
        assert dict(request.url.params) == {
            "AppID": "application-private-id",
            "Safe": "database-private-safe",
            "Folder": "Root",
            "Object": "database-private-object",
        }
        assert request.extensions["timeout"]["read"] == 2.5
        return httpx.Response(
            200,
            json={
                "Content": "  rotated-private-password  ",
                "UserName": "database-user",
                "Address": "db.test",
                "Safe": "database-private-safe",
                "PasswordChangeInProcess": "false",
            },
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        client = CyberArkClient(ccp_settings(timeout_seconds=2.5), http_client=http)
        secret = await client.get_password()
        assert isinstance(secret, SecretStr)
        assert secret.get_secret_value() == "  rotated-private-password  "
        assert "rotated-private-password" not in repr(secret)
        assert "rotated-private-password" not in caplog.text
        assert not http.is_closed


@pytest.mark.parametrize("status", [301, 403, 500])
async def test_ccp_http_failure_hides_body_query_and_exception_context(status, caplog):
    """上游错误正文、定位参数及密码不得进入公开错误或日志。"""
    from egis_agentkit.client._cyberark import CyberArkClient, CyberArkError

    caplog.set_level(logging.DEBUG, logger="egis_agentkit")
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(
                status,
                headers={"location": "https://other.test/steal"},
                json={"Content": "private-password", "ErrorMsg": "private-error-body"},
            )
        )
    ) as http:
        client = CyberArkClient(ccp_settings(), http_client=http)
        with pytest.raises(CyberArkError) as failure:
            await client.get_password()
        rendered = "".join(traceback.format_exception(failure.value)) + caplog.text
        for secret in (
            "private-password",
            "private-error-body",
            "application-private-id",
            "database-private-safe",
            "database-private-object",
            "other.test",
        ):
            assert secret not in rendered
        assert str(status) in str(failure.value)
        assert not http.is_closed


async def test_ccp_transport_failure_does_not_chain_upstream_exception():
    """网络异常可能携带完整请求 URL，必须截断异常链。"""
    from egis_agentkit.client._cyberark import CyberArkClient, CyberArkError

    def unavailable(request):
        raise httpx.ConnectError(
            "private-password " + str(request.url), request=request
        )

    async with httpx.AsyncClient(transport=httpx.MockTransport(unavailable)) as http:
        client = CyberArkClient(ccp_settings(), http_client=http)
        with pytest.raises(CyberArkError) as failure:
            await client.get_password()
        rendered = "".join(traceback.format_exception(failure.value))
        assert "private-password" not in rendered
        assert "application-private-id" not in rendered


@pytest.mark.parametrize(
    "body", [[], {}, {"Content": None}, {"Content": 1}, {"Content": ""}]
)
async def test_ccp_malformed_response_is_rejected_without_password(body):
    """缺失或非字符串密码必须失败，禁止空密码和隐式字符串转换。"""
    from egis_agentkit.client._cyberark import CyberArkClient, CyberArkError

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: httpx.Response(200, json=body))
    ) as http:
        with pytest.raises(CyberArkError):
            await CyberArkClient(ccp_settings(), http_client=http).get_password()


async def test_ccp_invalid_json_body_is_not_exposed():
    """非法 JSON 的原始正文也可能包含密码。"""
    from egis_agentkit.client._cyberark import CyberArkClient, CyberArkError

    async with httpx.AsyncClient(
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, text="private-password invalid-json")
        )
    ) as http:
        with pytest.raises(CyberArkError) as failure:
            await CyberArkClient(ccp_settings(), http_client=http).get_password()
        assert "private-password" not in "".join(
            traceback.format_exception(failure.value)
        )


async def test_ccp_owned_http_session_closes_after_every_password_request(monkeypatch):
    """默认会话逐次创建并关闭，避免把短生命周期客户端遗留到连接池之外。"""
    import egis_agentkit.client._cyberark as module

    original = httpx.AsyncClient
    sessions = []

    def session(**kwargs):
        assert isinstance(kwargs["verify"], ssl.SSLContext)
        assert kwargs["verify"].verify_mode == ssl.CERT_REQUIRED
        assert kwargs["verify"].check_hostname
        assert kwargs["trust_env"] is False
        assert kwargs["follow_redirects"] is False
        result = original(
            **kwargs,
            transport=httpx.MockTransport(
                lambda request: httpx.Response(
                    200, json={"Content": "private-password"}
                )
            ),
        )
        sessions.append(result)
        return result

    monkeypatch.setattr(module.httpx, "AsyncClient", session)
    client = module.CyberArkClient(ccp_settings())
    await client.get_password()
    await client.get_password()
    assert len(sessions) == 2
    assert all(http.is_closed for http in sessions)


async def test_ccp_invalid_ca_is_sanitized_before_request(tmp_path):
    """无效 CA 文件必须失败，不能静默关闭服务器证书验证。"""
    from egis_agentkit.client._cyberark import CyberArkClient, CyberArkError

    ca = tmp_path / "private-ca.pem"
    ca.write_text("private-password invalid certificate")
    with pytest.raises(CyberArkError) as failure:
        await CyberArkClient(ccp_settings(tls_ca_file=str(ca))).get_password()
    rendered = "".join(traceback.format_exception(failure.value))
    assert "private-password" not in rendered
    assert str(ca) not in rendered


@pytest.fixture
def tls_files(tmp_path):
    """生成本测试独占的 CA 与叶子证书，不依赖公司证书或外部网络。"""
    from cryptography import x509
    from cryptography.hazmat.primitives import hashes, serialization
    from cryptography.hazmat.primitives.asymmetric import ec
    from cryptography.x509.oid import ExtendedKeyUsageOID, NameOID

    ca_key = ec.generate_private_key(ec.SECP256R1())
    leaf_key = ec.generate_private_key(ec.SECP256R1())
    now = datetime.now(timezone.utc)
    ca_name = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Unit test CA")])

    def certificate(subject, public_key):
        return (
            x509.CertificateBuilder()
            .subject_name(subject)
            .issuer_name(ca_name)
            .public_key(public_key)
            .serial_number(x509.random_serial_number())
            .not_valid_before(now - timedelta(minutes=1))
            .not_valid_after(now + timedelta(hours=1))
        )

    ca = (
        certificate(ca_name, ca_key.public_key())
        .add_extension(
            x509.BasicConstraints(ca=True, path_length=0),
            critical=True,
        )
        .sign(ca_key, hashes.SHA256())
    )
    leaf = (
        certificate(
            x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, "Unit test CCP")]),
            leaf_key.public_key(),
        )
        .add_extension(
            x509.SubjectAlternativeName(
                [x509.IPAddress(ipaddress.ip_address("127.0.0.1"))]
            ),
            critical=False,
        )
        .add_extension(
            x509.ExtendedKeyUsage(
                [ExtendedKeyUsageOID.SERVER_AUTH, ExtendedKeyUsageOID.CLIENT_AUTH]
            ),
            critical=False,
        )
        .sign(ca_key, hashes.SHA256())
    )
    paths = [tmp_path / name for name in ("ca.pem", "leaf.pem", "leaf-key.pem")]
    paths[0].write_bytes(ca.public_bytes(serialization.Encoding.PEM))
    paths[1].write_bytes(leaf.public_bytes(serialization.Encoding.PEM))
    paths[2].write_bytes(
        leaf_key.private_bytes(
            serialization.Encoding.PEM,
            serialization.PrivateFormat.PKCS8,
            serialization.NoEncryption(),
        )
    )
    paths[2].chmod(0o600)
    return tuple(map(str, paths))


async def test_ccp_default_transport_authenticates_with_custom_ca_and_mtls(tls_files):
    """真实 TLS 握手验证自定义 CA 与客户端证书被应用到默认 HTTP 会话。"""
    from egis_agentkit.client._cyberark import CyberArkClient, CyberArkError

    ca, cert, key = tls_files
    server_tls = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)
    server_tls.load_cert_chain(cert, key)
    server_tls.load_verify_locations(cafile=ca)
    server_tls.verify_mode = ssl.CERT_REQUIRED
    peer_certificates = []

    async def handle(reader, writer):
        try:
            await reader.readuntil(b"\r\n\r\n")
            peer_certificates.append(writer.get_extra_info("peercert"))
            body = b'{"Content":"private-mtls-password"}'
            writer.write(
                b"HTTP/1.1 200 OK\r\nContent-Type: application/json\r\nConnection: close\r\n"
                + f"Content-Length: {len(body)}\r\n\r\n".encode()
                + body
            )
            await writer.drain()
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0, ssl=server_tls)
    try:
        port = server.sockets[0].getsockname()[1]
        config = dict(
            endpoint=f"https://127.0.0.1:{port}/AIMWebService/api/Accounts",
            tls_ca_file=ca,
        )
        with pytest.raises(CyberArkError):
            await CyberArkClient(ccp_settings(**config)).get_password()
        client = CyberArkClient(
            ccp_settings(
                **config,
                client_cert_file=cert,
                client_key_file=key,
            )
        )
        assert (
            await client.get_password()
        ).get_secret_value() == "private-mtls-password"
        assert len(peer_certificates) == 1
        assert peer_certificates[0]["subject"]
    finally:
        server.close()
        await server.wait_closed()


async def test_ccp_invalid_mtls_certificate_does_not_leak_key_path(tmp_path):
    """客户端证书加载失败的异常链不能暴露私钥路径或文件内容。"""
    from egis_agentkit.client._cyberark import CyberArkClient, CyberArkError

    key = tmp_path / "private-key.pem"
    cert = tmp_path / "private-cert.pem"
    key.write_text("private-password invalid key")
    cert.write_text("invalid cert")
    client = CyberArkClient(
        ccp_settings(client_cert_file=str(cert), client_key_file=str(key))
    )
    with pytest.raises(CyberArkError) as failure:
        await client.get_password()
    rendered = "".join(traceback.format_exception(failure.value))
    assert "private-password" not in rendered
    assert str(key) not in rendered
