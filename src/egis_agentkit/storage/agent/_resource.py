"""持久化个人助理资源开关，不保存资源版本或配置副本。"""

from __future__ import annotations

from typing import Any

from sqlalchemy import delete, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from egis_agentkit.errors import NotFound
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import utcnow


class PersonalResourceStorage:
    """按用户查询资源选择，保留其所属个人助理的关联。"""

    def __init__(self, engine: AsyncEngine) -> None:
        self.engine = engine

    async def list_resources(
        self, user_id: str, resource_type: str, *, resource_ids: set[str] | None = None
    ) -> dict[str, dict[str, Any]]:
        """直接按用户和资源类型读取个人选择，不需要先构建定义。"""
        table = t.agent_personal_resources
        statement = select(table).where(
            table.c.user_id == user_id, table.c.resource_type == resource_type
        )
        if resource_ids is not None:
            if not resource_ids:
                return {}
            statement = statement.where(table.c.resource_id.in_(resource_ids))
        async with self.engine.connect() as connection:
            rows = (await connection.execute(statement)).mappings()
        return {row["resource_id"]: dict(row) for row in rows}

    async def save_resource(
        self,
        user_id: str,
        agent_id: str,
        resource_type: str,
        resource_id: str,
        enabled: bool,
        *,
        actor: str,
    ) -> dict[str, Any]:
        """创建或更新唯一开关，审计时间由存储统一维护。"""
        table = t.agent_personal_resources
        now = utcnow()
        statement = insert(table).values(
            user_id=user_id,
            agent_id=agent_id,
            resource_type=resource_type,
            resource_id=resource_id,
            is_enabled=enabled,
            created_by=actor,
            updated_by=actor,
            created_at=now,
            updated_at=now,
        )
        statement = statement.on_conflict_do_update(
            index_elements=[
                table.c.agent_id,
                table.c.resource_type,
                table.c.resource_id,
            ],
            set_={"is_enabled": enabled, "updated_by": actor, "updated_at": now},
        ).returning(table)
        async with self.engine.begin() as connection:
            if resource_type == "skill":
                # 与删除取得同一行的锁，已删除的技能不能被重新选用。
                skill = t.skill_summary
                enabled_skill = await connection.scalar(
                    select(skill.c.is_enabled)
                    .where(skill.c.skill_id == resource_id)
                    .with_for_update(read=True)
                )
                if enabled_skill is False:
                    raise NotFound("技能不存在")
            return dict((await connection.execute(statement)).mappings().one())

    async def update_resource_enabled(
        self,
        user_id: str,
        resource_type: str,
        resource_id: str,
        enabled: bool,
        *,
        actor: str,
    ) -> dict[str, Any]:
        """只更新已选资源的启停状态，未选择的资源不能被更新操作隐式添加。"""
        table = t.agent_personal_resources
        statement = (
            update(table)
            .where(
                table.c.user_id == user_id,
                table.c.resource_type == resource_type,
                table.c.resource_id == resource_id,
            )
            .values(is_enabled=enabled, updated_by=actor, updated_at=utcnow())
            .returning(table)
        )
        async with self.engine.begin() as connection:
            row = (await connection.execute(statement)).mappings().one_or_none()
            if row is None:
                raise NotFound("个人助理尚未选择此资源")
            return dict(row)

    async def remove_resource(
        self, user_id: str, resource_type: str, resource_id: str
    ) -> None:
        """取消当前用户的资源选择，不修改资源或授权。"""
        table = t.agent_personal_resources
        async with self.engine.begin() as connection:
            await connection.execute(
                delete(table).where(
                    table.c.user_id == user_id,
                    table.c.resource_type == resource_type,
                    table.c.resource_id == resource_id,
                )
            )
