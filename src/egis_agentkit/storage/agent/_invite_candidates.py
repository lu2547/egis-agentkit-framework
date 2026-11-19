"""存储用户对各专家视图的自动邀请候选选择。"""

from sqlalchemy import delete, select
from sqlalchemy.dialects.postgresql import insert

from egis_agentkit.storage.model import ViewType
from egis_agentkit.storage.model._tables import agent_invite_candidates as table


class ExpertCandidateStorage:
    def __init__(self, engine):
        self.engine = engine

    async def list(self, user_id: str) -> set[tuple[str, ViewType]]:
        async with self.engine.connect() as connection:
            result = await connection.execute(
                select(table.c.definition_id, table.c.resource_view_type).where(
                    table.c.user_id == user_id
                )
            )
            return {(identifier, view) for identifier, view in result}

    async def add(self, user_id: str, identifier: str, view: ViewType) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(
                insert(table)
                .values(
                    user_id=user_id, definition_id=identifier, resource_view_type=view
                )
                .on_conflict_do_nothing()
            )

    async def remove(self, user_id: str, identifier: str, view: ViewType) -> None:
        async with self.engine.begin() as connection:
            await connection.execute(
                delete(table).where(
                    table.c.user_id == user_id,
                    table.c.definition_id == identifier,
                    table.c.resource_view_type == view,
                )
            )
