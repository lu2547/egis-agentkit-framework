"""用一次性数据库角色验证连接池的凭据轮换及事务兼容性。"""

import logging
import os
import secrets
import ssl
import traceback
from pathlib import Path
from types import SimpleNamespace
from uuid import uuid4

import asyncpg
import httpx
import pytest
import pytest_asyncio
from dotenv import dotenv_values
from pydantic import SecretStr
from sqlalchemy import text

from egis_agentkit.client.nacos._settings import (
    CyberArkConfig,
    PostgresConfig,
    PostgresConnectionConfig,
    PostgresPoolConfig,
)


def settings(**changes):
    """构造显式组件配置，永远不接受完整数据库连接串。"""
    return PostgresConfig(
        connection=PostgresConnectionConfig(
            password=SecretStr("private-password"), **changes
        ),
        pool=PostgresPoolConfig(size=1, max_overflow=0),
    )


def test_postgres_url_has_no_password_and_escapes_components():
    """把密码放回 SQLAlchemy URL 会让错误和引擎输出重新持有凭据。"""
    from egis_agentkit.client._postgres import postgres_url

    value = postgres_url(settings(username="user@unit", database="db/unit"))
    assert value == "postgresql+asyncpg://user%40unit@127.0.0.1:5432/db/unit"
    assert "private-password" not in value


@pytest.mark.parametrize("password", [None, ""])
def test_key_engine_requires_password_before_opening_pool(password):
    """允许缺失密码的配置快照仅服务注入引擎，不能走默认数据库认证。"""
    from egis_agentkit.client._postgres import create_postgres_engine_from_config

    config = PostgresConfig(connection=PostgresConnectionConfig(password=password))
    with pytest.raises(ValueError):
        create_postgres_engine_from_config(config)


@pytest_asyncio.fixture
async def disposable_login():
    """仅创建测试独占角色，不更改部署账号密码和业务数据。"""
    path = Path(
        os.environ.get(
            "AGENTKIT_TEST_PG_ENV",
            Path(__file__).resolve().parents[3] / "deploy/agentkit-infra/pg.env",
        )
    )
    env = dotenv_values(path)
    admin = await asyncpg.connect(
        host="127.0.0.1",
        port=5433,
        database=env["POSTGRES_DB"],
        user=env["POSTGRES_USER"],
        password=env["POSTGRES_PASSWORD"],
    )
    username = "ak_credentials_" + uuid4().hex
    password = secrets.token_hex(24)
    replacement = secrets.token_hex(24)
    # 角色名和密码均为本测试生成的字母数字值；直接使用驱动避免 DDL 日志输出密码。
    await admin.execute(f"CREATE ROLE \"{username}\" LOGIN PASSWORD '{password}'")
    try:
        yield SimpleNamespace(
            admin=admin,
            username=username,
            password=SecretStr(password),
            replacement=SecretStr(replacement),
            database=env["POSTGRES_DB"],
        )
    finally:
        await admin.execute(f'DROP ROLE "{username}"')
        await admin.close()


def login_settings(login, **changes):
    """测试账号的地址参数与静态凭据来自不同字段。"""
    connection = dict(
        host="127.0.0.1",
        port=5433,
        database=login.database,
        username=login.username,
        password=login.password,
    )
    connection.update(changes)
    return PostgresConfig(
        connection=PostgresConnectionConfig(**connection),
        pool=PostgresPoolConfig(size=1, max_overflow=0),
    )


async def test_key_engine_uses_real_pool_and_keeps_transaction_contract(
    disposable_login, caplog
):
    """实际认证、连接复用和事务结束检查覆盖原有 Storage 的入口契约。"""
    from egis_agentkit.client._postgres import (
        PostgresTransaction,
        check_postgres,
        create_postgres_engine_from_config,
        transaction_connection,
    )

    caplog.set_level(logging.DEBUG)
    engine = create_postgres_engine_from_config(login_settings(disposable_login))
    try:
        await check_postgres(engine)
        async with engine.begin() as connection:
            transaction = PostgresTransaction(connection)
            assert transaction_connection(transaction, None) is connection
            async with transaction.connect() as same:
                assert same is connection
                first_pid = await same.scalar(text("SELECT pg_backend_pid()"))
                assert (
                    await same.scalar(text("SELECT current_user"))
                    == disposable_login.username
                )
        with pytest.raises(RuntimeError, match="no longer active"):
            transaction.connection()
        async with engine.connect() as connection:
            assert await connection.scalar(text("SELECT pg_backend_pid()")) == first_pid
        events = [
            record
            for record in caplog.records
            if record.name == "egis_agentkit.client._postgres"
        ]
        assert [record.getMessage() for record in events] == ["db.connected"]
        assert events[0].context["mode"] == "key"
        assert events[0].context["duration_ms"] >= 0
        assert not hasattr(events[0], "username")
        assert engine.url.password is None
        assert disposable_login.password.get_secret_value() not in caplog.text
        assert disposable_login.password.get_secret_value() not in str(engine.url)
    finally:
        await engine.dispose()


async def test_cyberark_refreshes_only_for_new_physical_connections(
    disposable_login, caplog
):
    """实际改密后复用连接不取密，dispose 后必须取到新密码才能重新认证。"""
    from egis_agentkit.client._cyberark import CyberArkClient
    from egis_agentkit.client._postgres import create_postgres_engine_from_config

    caplog.set_level(logging.DEBUG, logger="egis_agentkit")
    login = disposable_login
    current_password = login.password
    fetches = 0
    unavailable = False

    def provider(request):
        nonlocal fetches
        fetches += 1
        if unavailable:
            return httpx.Response(503, json={"Content": "private-upstream-secret"})
        return httpx.Response(
            200, json={"Content": current_password.get_secret_value()}
        )

    ccp = CyberArkConfig(
        endpoint="https://ccp.test/AIMWebService/api/Accounts",
        app_id="unit-app",
        safe="unit-safe",
        object_name="unit-object",
    )
    async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as http:
        client = CyberArkClient(ccp, http_client=http)
        config = login_settings(login, mode="cyberark", password=None, cyberark=ccp)
        engine = create_postgres_engine_from_config(config, cyberark_client=client)
        try:
            async with engine.connect() as connection:
                first_pid = await connection.scalar(text("SELECT pg_backend_pid()"))
            assert fetches == 1
            # 只变更测试独占角色；现有物理连接继续有效。
            await login.admin.execute(
                f"ALTER ROLE \"{login.username}\" PASSWORD '{login.replacement.get_secret_value()}'"
            )
            current_password = login.replacement
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(text("SELECT pg_backend_pid()"))
                    == first_pid
                )
                assert await connection.scalar(text("SELECT 1")) == 1
            assert fetches == 1
            await engine.dispose()
            async with engine.connect() as connection:
                assert (
                    await connection.scalar(text("SELECT pg_backend_pid()"))
                    != first_pid
                )
            assert fetches == 2
            await engine.dispose()
            unavailable = True
            with pytest.raises(RuntimeError) as failure:
                async with engine.connect():
                    pytest.fail("CCP 失败后不能回退到旧密码")
            assert fetches == 3
            records = [
                record
                for record in caplog.records
                if record.name == "egis_agentkit.client._postgres"
            ]
            assert [record.getMessage() for record in records] == [
                "db.connected",
                "db.connected",
                "db.credential_failed",
            ]
            assert records[-1].context["error_type"] == "CyberArkError"
            rendered = "".join(traceback.format_exception(failure.value)) + caplog.text
            for secret in (
                login.password,
                login.replacement,
                SecretStr("private-upstream-secret"),
            ):
                assert secret.get_secret_value() not in rendered
        finally:
            await engine.dispose()
        assert not http.is_closed


@pytest.mark.parametrize("mode", ["disable", "require", "verify-full"])
async def test_asyncpg_connection_receives_explicit_tls_and_timeout(
    mode, monkeypatch, caplog
):
    """驱动接收独立字段和 TLS 策略，避免密码 DSN 和驱动的环境回退。"""
    from egis_agentkit.client._postgres import create_postgres_engine_from_config

    received = {}

    async def reject(**kwargs):
        received.update(kwargs)
        raise OSError("private-password driver failure")

    monkeypatch.setattr(asyncpg, "connect", reject)
    engine = create_postgres_engine_from_config(
        settings(ssl_mode=mode, connect_timeout_seconds=1.5)
    )
    try:
        with pytest.raises(RuntimeError) as failure:
            async with engine.connect():
                pass
        assert "private-password" not in "".join(
            traceback.format_exception(failure.value)
        )
        assert "OSError" in str(failure.value)
        records = [
            record
            for record in caplog.records
            if record.name == "egis_agentkit.client._postgres"
        ]
        assert len(records) == 1
        assert records[0].getMessage() == "db.connection_failed"
        assert records[0].context["mode"] == "key"
        assert records[0].context["error_type"] == "OSError"
        assert "private-password" not in caplog.text
        assert "dsn" not in received
        assert received["password"] == "private-password"
        assert received["timeout"] == 1.5
        assert received["host"] == "127.0.0.1"
        if mode == "disable":
            assert received["ssl"] is False
        else:
            context = received["ssl"]
            assert isinstance(context, ssl.SSLContext)
            assert context.check_hostname is (mode == "verify-full")
            expected = ssl.CERT_REQUIRED if mode == "verify-full" else ssl.CERT_NONE
            assert context.verify_mode == expected
    finally:
        await engine.dispose()


def test_postgres_invalid_ca_does_not_disable_verification(tmp_path):
    """自定义 CA 无效必须明确失败，禁止忽略配置退化为非验证连接。"""
    from egis_agentkit.client._postgres import create_postgres_engine_from_config

    ca = tmp_path / "private-ca.pem"
    ca.write_text("private-password invalid certificate")
    with pytest.raises(ValueError) as failure:
        create_postgres_engine_from_config(
            settings(ssl_mode="verify-full", ssl_ca_file=str(ca))
        )
    rendered = "".join(traceback.format_exception(failure.value))
    assert "private-password" not in rendered
    assert str(ca) not in rendered
