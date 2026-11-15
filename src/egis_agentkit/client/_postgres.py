"""管理 PostgreSQL 连接池接入与事务连接复用。

核心能力
- create_postgres_engine 创建异步连接池，连接检查与池限额统一配置。
- check_postgres 检查连接可用性，不执行业务查询。
- PostgresTransaction / transaction_connection 供 Storage 在同一事务中复用连接。
"""

from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine


def create_postgres_engine(
    url: str, *, pool_size: int, max_overflow: int
) -> AsyncEngine:
    """按部署连接串和配置限额创建池，由生命周期持有方负责释放。"""
    return create_async_engine(
        url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
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


def transaction_connection(
    source: AsyncEngine | PostgresTransaction, connection: AsyncConnection | None
) -> AsyncConnection:
    """解析 Storage 已持有的事务连接，禁止隐式开启独立事务。"""
    if connection is not None:
        return connection
    if isinstance(source, PostgresTransaction):
        return source.connection()
    raise RuntimeError("This operation requires a PostgreSQL transaction")
