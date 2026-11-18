from contextlib import asynccontextmanager
from typing import AsyncIterator

from agentscope.app.storage import AsyncSQLAlchemyStorage
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from egis_agentkit.client._postgres import PostgresTransaction
from egis_agentkit.errors import Conflict, Forbidden

from ._access import AccessStorage
from ._definition import DefinitionStorage
from ._remote_agent import RemoteRunStorage
from ._resource import MCPCredentialStorage, ResourceAssetStorage
from ._schema import init_schema
from ._session import SessionStorage
from ._session_feedback import SessionFeedbackStorage
from ._session_steering import SessionSteeringStorage
from ._skill_import import SkillImportStorage
from ._user import UserStorage
from ._virtual_user import VirtualUserStorage
from .agent._resource import PersonalResourceStorage
from .project._file_records import ProjectFileRecordStorage, lock_project_files
from .project._project import ProjectStorage


class DefinitionWrite:
    """定义事务中的持久化入口，定义、资源和授权共用一个事务。"""

    def __init__(self, connection: AsyncConnection):
        self.engine = PostgresTransaction(connection)
        self.definitions = DefinitionStorage(self.engine)
        self.agents = self.definitions.agents
        self.access = AccessStorage(self.engine)
        self.assets = ResourceAssetStorage(self.engine)
        self.users = UserStorage(self.engine)

    @asynccontextmanager
    async def definition_write(self) -> AsyncIterator["DefinitionWrite"]:
        """子能力与调用方复用同一事务，由外层控制提交。"""
        self.engine.connection()
        yield self

    async def get_definition(
        self, definition_id: str, *, version: int | str | None = None, conn=None
    ):
        """在当前事务读取指定版本或当前定义。"""
        return await self.definitions.get_definition(
            definition_id, version=version, conn=conn
        )

    async def get_user(self, user_id: str):
        return await self.users.get_user(user_id)


class AgentKitStorage(AsyncSQLAlchemyStorage):
    """Keep upstream storage intact, share an explicitly owned PG engine.

    Schema creation is an explicit deploy operation. Services coordinate
    enterprise transactions and reuse native persistence for runtime records.
    """

    def __init__(self, url: str, engine: AsyncEngine):
        self.engine = engine
        self.users = UserStorage(engine)
        self.personal_resources = PersonalResourceStorage(engine)
        self.definitions = DefinitionStorage(engine)
        self.agents = self.definitions.agents
        self.access = AccessStorage(engine)
        self.assets = ResourceAssetStorage(engine)
        self.mcp_credentials = MCPCredentialStorage(engine)
        self.skill_imports = SkillImportStorage(engine)
        self.virtual_users = VirtualUserStorage(engine)
        self.remote_runs = RemoteRunStorage(engine)
        self.projects = ProjectStorage(engine)
        self.sessions = SessionStorage(engine, self)
        self.session_feedback = SessionFeedbackStorage(engine)
        self.session_steering = SessionSteeringStorage(engine)
        self.project_files = ProjectFileRecordStorage(engine)
        super().__init__(url, engine=engine, create_tables=False)

    async def init_schema(self) -> None:
        """初始化工作台原生运行表和首版企业表。"""
        await init_schema(self.engine)

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[AsyncConnection]:
        async with self.engine.begin() as conn:
            yield conn

    @asynccontextmanager
    async def definition_write(self) -> AsyncIterator[DefinitionWrite]:
        """借用池内连接，定义和依赖授权一起提交或回滚。"""
        try:
            async with self.engine.begin() as connection:
                write = DefinitionWrite(connection)
                await write.access.lock_writes()
                yield write
        except IntegrityError as exc:
            raise Conflict("英文名已存在，或配置与现有资源冲突") from exc

    async def get_user(self, user_id: str):
        return await self.users.get_user(user_id)

    async def get_definition(
        self, definition_id: str, *, version: int | str | None = None, conn=None
    ):
        """读取指定版本或当前定义。"""
        return await self.definitions.get_definition(
            definition_id, version=version, conn=conn
        )

    async def locate_session(self, session_id: str) -> tuple[str, str] | None:
        """从原生锁的会话 ID 读取归属。"""
        return await self.sessions.locate(session_id)

    @asynccontextmanager
    async def project_cleanup(self, project_id):
        """锁住文件变更，并在一个事务内清理项目、原生会话和文件记录。"""
        async with self.engine.begin() as conn:
            await lock_project_files(conn, project_id)
            transaction = PostgresTransaction(conn)
            yield (
                ProjectStorage(transaction),
                SessionStorage(transaction, self),
                ProjectFileRecordStorage(transaction),
            )

    @asynccontextmanager
    async def session_cleanup(self):
        """在同一事务清理原生会话和回复产物关系。"""
        async with self.engine.begin() as conn:
            transaction = PostgresTransaction(conn)
            yield (
                SessionStorage(transaction, self),
                ProjectFileRecordStorage(transaction),
            )

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
