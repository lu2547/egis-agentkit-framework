"""PostgreSQL 连接池与连通性检查。"""

from contextlib import asynccontextmanager

from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine, create_async_engine


def create_postgres_engine(
    url: str, *, pool_size: int, max_overflow: int
) -> AsyncEngine:
    return create_async_engine(
        url,
        pool_pre_ping=True,
        pool_size=pool_size,
        max_overflow=max_overflow,
        hide_parameters=True,
    )


async def check_postgres(engine: AsyncEngine) -> None:
    async with engine.connect() as conn:
        await conn.execute(text("SELECT 1"))


class PostgresTransaction:
    """让事务内的存储对象复用连接池中同一条已借出的连接。"""

    def __init__(self, connection: AsyncConnection):
        self._connection = connection

    def connection(self) -> AsyncConnection:
        if not self._connection.in_transaction():
            raise RuntimeError("PostgreSQL transaction is no longer active")
        return self._connection

    @asynccontextmanager
    async def connect(self):
        yield self.connection()


def transaction_connection(
    source: AsyncEngine | PostgresTransaction, connection: AsyncConnection | None
) -> AsyncConnection:
    if connection is not None:
        return connection
    if isinstance(source, PostgresTransaction):
        return source.connection()
    raise RuntimeError("This operation requires a PostgreSQL transaction")
