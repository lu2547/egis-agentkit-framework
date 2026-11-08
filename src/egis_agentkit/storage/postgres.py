from contextlib import asynccontextmanager
from typing import AsyncIterator

from agentscope.app.storage import AsyncSQLAlchemyStorage
from sqlalchemy import inspect, text
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.errors import Forbidden

from . import tables
from .access import AccessStorage
from .definition import DefinitionStorage
from .resource import ResourceAssetStorage
from .user import UserStorage


class AgentKitStorage(AsyncSQLAlchemyStorage):
    """Keep upstream storage intact, share an explicitly owned PG engine.

    Schema creation is an explicit deploy operation. Services coordinate
    enterprise transactions and reuse native persistence for runtime records.
    """

    def __init__(self, url: str, engine: AsyncEngine):
        self.engine = engine
        self.users = UserStorage(engine)
        self.definitions = DefinitionStorage(engine)
        self.access = AccessStorage(engine)
        self.assets = ResourceAssetStorage(engine)
        super().__init__(url, engine=engine, create_tables=False)

    async def migrate_enterprise(self) -> None:
        async with self.engine.begin() as conn:
            await conn.execute(text("SELECT pg_advisory_xact_lock(727140201)"))
            for table in (
                tables.file_baselines,
                tables.file_fingerprints,
                tables.file_changes,
                tables.file_scan_errors,
            ):
                columns = await conn.run_sync(
                    lambda sync, name=table.name: (
                        [column["name"] for column in inspect(sync).get_columns(name)]
                        if inspect(sync).has_table(name)
                        else []
                    )
                )
                if "session_id" in columns:
                    await conn.execute(
                        text(
                            f'ALTER TABLE "{table.name}" RENAME COLUMN session_id TO project_id'
                        )
                    )
            await conn.run_sync(tables.metadata.create_all)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncConnection]:
        async with self.engine.begin() as conn:
            yield conn

    async def get_user(self, user_id: str):
        return await self.users.get_user(user_id)

    async def get_definition(self, definition_id: str, *, conn=None):
        return await self.definitions.get_definition(definition_id, conn=conn)

    async def locate_session(self, session_id: str) -> tuple[str, str] | None:
        # The native lock identifies a session by ID alone.
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    text("SELECT user_id, agent_id FROM sessions WHERE id=:id"),
                    {"id": session_id},
                )
            ).one_or_none()
        return (row.user_id, row.agent_id) if row is not None else None

    async def read_session_unscoped(self, user_id, agent_id, session_id):
        record = await super().get_session(user_id, agent_id, session_id)
        # Native SQL implementation ignores agent_id; close that ownership gap.
        return record if record is not None and record.agent_id == agent_id else None

    async def get_session(self, user_id, agent_id, session_id):
        return await self.read_session_unscoped(user_id, agent_id, session_id)

    async def update_session_state(self, user_id, agent_id, session_id, state):
        if await self.read_session_unscoped(user_id, agent_id, session_id) is None:
            raise Forbidden("会话归属不匹配")
        return await super().update_session_state(user_id, agent_id, session_id, state)
