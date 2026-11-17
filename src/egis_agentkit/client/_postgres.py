"""管理 PostgreSQL 配置、按物理连接取密及事务连接复用。

密码始终通过 asyncpg 独立参数传入，连接池和 Storage 只持有不含密码的 URL。
"""

import logging
import ssl
from contextlib import asynccontextmanager
from time import perf_counter

import asyncpg
from pydantic import SecretStr
from sqlalchemy import text
from sqlalchemy.engine import URL
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine

from egis_agentkit.client._cyberark import CyberArkClient
from egis_agentkit.client.nacos._settings import (
    PostgresConfig,
    PostgresConnectionConfig,
)

_logger = logging.getLogger(__name__)


def postgres_url(settings: PostgresConfig) -> str:
    """仅生成数据库定位信息，密码不进入 URL、引擎表示或连接池日志。"""
    connection = settings.connection
    return URL.create(
        "postgresql+asyncpg",
        username=connection.username,
        host=connection.host,
        port=connection.port,
        database=connection.database,
    ).render_as_string(hide_password=True)


def _tls_context(settings: PostgresConnectionConfig) -> ssl.SSLContext | bool:
    """把显式 TLS 策略转成驱动参数，无效 CA 必须在创建连接池前报错。"""
    if settings.ssl_mode == "disable":
        return False
    try:
        if settings.ssl_mode == "verify-full":
            return ssl.create_default_context(cafile=settings.ssl_ca_file)
        context = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        context.check_hostname = False
        context.verify_mode = ssl.CERT_NONE
        return context
    except (OSError, ValueError):
        raise ValueError("PostgreSQL TLS configuration invalid") from None


def create_postgres_engine_from_config(
    settings: PostgresConfig, *, cyberark_client: CyberArkClient | None = None
) -> AsyncEngine:
    """按 Nacos 配置创建连接池；每次新建物理连接时才解析当前密码。"""
    connection = settings.connection
    if connection.mode == "key" and (
        connection.password is None or not connection.password.get_secret_value()
    ):
        raise ValueError("PostgreSQL key mode requires a nonempty password")
    client = cyberark_client
    if connection.mode == "cyberark" and client is None:
        if connection.cyberark is None:
            raise ValueError("PostgreSQL cyberark mode requires CCP settings")
        client = CyberArkClient(connection.cyberark)
    tls = _tls_context(connection)

    async def connect():
        """取密失败时拒绝建连，绝不使用缓存密码或回退到驱动环境变量。"""
        started = perf_counter()
        try:
            secret = (
                await client.get_password()
                if connection.mode == "cyberark"
                else connection.password
            )
            if not isinstance(secret, SecretStr) or not secret.get_secret_value():
                raise ValueError("empty or invalid password")
        except Exception as exc:
            _logger.warning(
                "db.credential_failed",
                extra={
                    "context": {
                        "mode": connection.mode,
                        "error_type": type(exc).__name__,
                    }
                },
            )
            raise RuntimeError(
                f"PostgreSQL credential unavailable ({type(exc).__name__})"
            ) from None
        try:
            physical_connection = await asyncpg.connect(
                host=connection.host,
                port=connection.port,
                database=connection.database,
                user=connection.username,
                password=secret.get_secret_value(),
                timeout=connection.connect_timeout_seconds,
                ssl=tls,
            )
        except Exception as exc:
            _logger.warning(
                "db.connection_failed",
                extra={
                    "context": {
                        "mode": connection.mode,
                        "error_type": type(exc).__name__,
                    }
                },
            )
            raise RuntimeError(
                f"PostgreSQL connection unavailable ({type(exc).__name__})"
            ) from None
        _logger.info(
            "db.connected",
            extra={
                "context": {
                    "mode": connection.mode,
                    "duration_ms": round((perf_counter() - started) * 1000, 3),
                }
            },
        )
        return physical_connection

    return create_async_engine(
        postgres_url(settings),
        async_creator=connect,
        pool_pre_ping=True,
        pool_size=settings.pool.size,
        max_overflow=settings.pool.max_overflow,
        hide_parameters=True,
    )


async def check_postgres(engine: AsyncEngine) -> None:
    """临时借用池中连接完成健康检查，退出后自动归还。"""
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


class PostgresTransaction:
    """让事务内的存储对象复用连接池中同一条已借出的连接。"""

    def __init__(self, connection: AsyncConnection):
        self._connection = connection

    def connection(self) -> AsyncConnection:
        """取得仍在事务中的连接，事务结束后拒绝继续使用。"""
        if not self._connection.in_transaction():
            raise RuntimeError("PostgreSQL transaction is no longer active")
        return self._connection

    @asynccontextmanager
    async def connect(self):
        """让存储对象复用已有事务连接，不重复借用或提交。"""
        yield self.connection()

    @asynccontextmanager
    async def begin(self):
        """让组合存储复用外层事务；提交和回滚始终由最外层持有者负责。"""
        yield self.connection()


def transaction_connection(
    source: AsyncEngine | PostgresTransaction, connection: AsyncConnection | None
) -> AsyncConnection:
    """解析 Storage 已持有的事务连接，禁止隐式开启独立事务。"""
    if connection is not None:
        return connection
    if isinstance(source, PostgresTransaction):
        return source.connection()
    raise RuntimeError("This operation requires a PostgreSQL transaction")
