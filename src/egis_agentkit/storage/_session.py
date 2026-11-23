"""按所有者和项目查询原生会话，复用原生删除规则。"""

from contextlib import asynccontextmanager

from agentscope.app.storage import AgentRecord, SessionRecord, TeamRecord
from agentscope.app.storage._sql._mappers import _from_record, _to_record
from agentscope.app.storage._sql._storage import _utcnow
from agentscope.app.storage._sql._tables import (
    AgentRow,
    MessageRow,
    ScheduleRow,
    SessionRow,
    TeamRow,
)
from agentscope.message import Msg
from sqlalchemy import (
    bindparam,
    cast,
    column,
    delete,
    func,
    insert,
    literal,
    literal_column,
    or_,
    select,
    tuple_,
    update,
)
from sqlalchemy.dialects.postgresql import JSONB

from egis_agentkit.client._postgres import transaction_connection
from egis_agentkit.errors import Forbidden
from egis_agentkit.storage.model._tables import projects, reply_artifacts

# 由 AgentKit 的 PostgreSQL schema 从原生会话状态生成。
_PROJECT_ID = column("project_id")
_SESSIONS = SessionRow.__table__


class SessionStorage:
    """提供原生存储接口之外的项目查询和事务内清理。"""

    def __init__(self, engine, native):
        self.engine = engine
        self.native = native

    async def _get_message_cursor(self, conn, session_id, message_id):
        return (
            await conn.execute(
                select(MessageRow.created_at, MessageRow.msg_id).where(
                    MessageRow.session_id == session_id,
                    MessageRow.msg_id == message_id,
                )
            )
        ).one_or_none()

    async def list_messages_through(self, session_id, message_id, *, limit):
        """按数据库消息顺序取分支边界，预算外多取一条供 Service 拒绝。"""
        async with self.engine.connect() as conn:
            cursor = await self._get_message_cursor(conn, session_id, message_id)
            if cursor is None:
                return []
            rows = await conn.scalars(
                select(MessageRow.payload)
                .where(
                    MessageRow.session_id == session_id,
                    tuple_(MessageRow.created_at, MessageRow.msg_id)
                    <= tuple_(cursor.created_at, cursor.msg_id),
                )
                .order_by(MessageRow.created_at, MessageRow.msg_id)
                .limit(limit + 1)
            )
            return [Msg.model_validate(payload) for payload in rows]

    async def get_question_before_reply(self, session_id, reply_id):
        async with self.engine.connect() as conn:
            cursor = await self._get_message_cursor(conn, session_id, reply_id)
            if cursor is None:
                return None
            payload = await conn.scalar(
                select(MessageRow.payload)
                .where(
                    MessageRow.session_id == session_id,
                    MessageRow.payload["role"].as_string() == "user",
                    tuple_(MessageRow.created_at, MessageRow.msg_id)
                    < tuple_(cursor.created_at, cursor.msg_id),
                )
                .order_by(MessageRow.created_at.desc(), MessageRow.msg_id.desc())
                .limit(1)
            )
            return Msg.model_validate(payload) if payload else None

    @asynccontextmanager
    async def rewrite_before(self, user_id, record, message_id, workers):
        """截断消息及运行状态，与调用方的消息入队共用提交/回滚边界。"""
        async with self.engine.begin() as conn:
            cursor = await self._get_message_cursor(conn, record.id, message_id)
            if cursor is None:
                from egis_agentkit.errors import NotFound

                raise NotFound("消息不存在或不可访问")
            await conn.execute(
                delete(MessageRow).where(
                    MessageRow.session_id == record.id,
                    tuple_(MessageRow.created_at, MessageRow.msg_id)
                    >= tuple_(cursor.created_at, cursor.msg_id),
                )
            )
            if workers:
                await conn.execute(
                    delete(MessageRow).where(
                        MessageRow.session_id.in_([worker.id for worker in workers])
                    )
                )
            for session in [record, *workers]:
                session.updated_at = _utcnow()
                row = _from_record(SessionRow, session)
                await conn.execute(
                    update(SessionRow)
                    .where(
                        SessionRow.user_id == user_id,
                        SessionRow.id == session.id,
                        SessionRow.agent_id == session.agent_id,
                    )
                    .values(payload=row.payload, updated_at=row.updated_at)
                )
            yield conn

    async def save_branch_history(
        self, user_id, record, source_session_id, message_ids
    ):
        """原子发布分支上下文、原生消息和产物引用；保留原消息排序及 ID。"""
        ids = message_ids
        # The initial branch graph is deliberately inaccessible. Publish its
        # initialized state in the same transaction as history and artifacts.
        record.state.middle_context["egis_agentkit"]["is_initialized"] = True
        row = _from_record(SessionRow, record)
        async with self.engine.begin() as conn:
            await conn.execute(
                update(SessionRow)
                .where(
                    SessionRow.id == record.id,
                    SessionRow.user_id == user_id,
                    SessionRow.agent_id == record.agent_id,
                )
                .values(payload=row.payload)
            )
            if record.team_id:
                await conn.execute(
                    update(SessionRow)
                    .where(
                        SessionRow.user_id == user_id,
                        SessionRow.team_id == record.team_id,
                        SessionRow.id != record.id,
                    )
                    .values(
                        payload=func.jsonb_set(
                            cast(SessionRow.payload, JSONB),
                            literal_column(
                                "'{state,middle_context,egis_agentkit,is_initialized}'::text[]"
                            ),
                            literal_column("'true'::jsonb"),
                        )
                    )
                )
            messages = MessageRow.__table__
            await conn.execute(
                insert(messages).from_select(
                    ["session_id", "msg_id", "created_at", "payload"],
                    select(
                        literal(record.id),
                        messages.c.msg_id,
                        messages.c.created_at,
                        messages.c.payload,
                    ).where(
                        messages.c.session_id == source_session_id,
                        messages.c.msg_id.in_(ids),
                    ),
                )
            )
            await conn.execute(
                insert(reply_artifacts).from_select(
                    ["project_id", "session_id", "reply_id", "path", "digest"],
                    select(
                        reply_artifacts.c.project_id,
                        literal(record.id),
                        reply_artifacts.c.reply_id,
                        reply_artifacts.c.path,
                        reply_artifacts.c.digest,
                    ).where(
                        reply_artifacts.c.session_id == source_session_id,
                        reply_artifacts.c.reply_id.in_(ids),
                    ),
                )
            )

    async def locate(self, session_id):
        """用消息总线锁的会话 ID 找到原生所有者和 Agent。"""
        async with self.engine.connect() as conn:
            row = (
                await conn.execute(
                    select(_SESSIONS.c.user_id, _SESSIONS.c.agent_id).where(
                        _SESSIONS.c.id == session_id
                    )
                )
            ).one_or_none()
        return (row.user_id, row.agent_id) if row is not None else None

    async def read_with_project(self, session_id, *, user_id=None, agent_id=None):
        """先按会话主键及所有者限定，再关联唯一所属项目；执行准备可按锁 ID 定位。"""
        fields = [field for field in projects.c if not field.name.startswith("scan_")]
        query = (
            select(
                _SESSIONS, *[field.label("project_" + field.name) for field in fields]
            )
            .select_from(
                _SESSIONS.outerjoin(
                    projects,
                    (projects.c.id == literal_column("sessions.project_id"))
                    & (projects.c.owner_id == _SESSIONS.c.user_id),
                )
            )
            .where(_SESSIONS.c.id == session_id)
        )
        if user_id is not None:
            query = query.where(_SESSIONS.c.user_id == user_id)
        if agent_id is not None:
            query = query.where(_SESSIONS.c.agent_id == agent_id)
        async with self.engine.connect() as conn:
            row = (await conn.execute(query)).mappings().one_or_none()
        if row is None:
            return None, None
        record = _to_record(
            SessionRow(**{field.name: row[field.name] for field in _SESSIONS.c}),
            SessionRecord,
        )
        project = (
            {field.name: row["project_" + field.name] for field in fields}
            if row["project_id"] is not None
            else None
        )
        return record, project

    async def in_project(self, user_id, project_id):
        """按项目及所有者列出主会话和成员会话。"""
        async with self.engine.connect() as conn:
            return (
                await conn.execute(
                    select(
                        _SESSIONS.c.id,
                        _SESSIONS.c.agent_id,
                        _SESSIONS.c.payload["state"]["middle_context"][
                            "egis_agentkit"
                        ].label("binding"),
                    )
                    .where(_PROJECT_ID == project_id, _SESSIONS.c.user_id == user_id)
                    .order_by(_SESSIONS.c.id)
                )
            ).all()

    @staticmethod
    def main_records_query(user_id, *, limit):
        """先用覆盖索引取得窄分页，再按主键关联原生大记录。"""
        page = (
            select(_SESSIONS.c.id, _SESSIONS.c.updated_at)
            .where(
                _SESSIONS.c.user_id == user_id,
                _SESSIONS.c.source == literal_column("'user'"),
                _PROJECT_ID.is_not(None),
            )
            .order_by(_SESSIONS.c.updated_at.desc())
            .limit(limit)
            .cte("session_page")
            .prefix_with("MATERIALIZED", dialect="postgresql")
        )
        return (
            select(_SESSIONS)
            .select_from(page.join(_SESSIONS, _SESSIONS.c.id == page.c.id))
            .order_by(page.c.updated_at.desc())
        )

    async def list_main_records(self, user_id, *, limit):
        """一次读取列表范围内的主会话，保留原生记录的转换规则。"""
        async with self.engine.connect() as conn:
            rows = (
                await conn.execute(self.main_records_query(user_id, limit=limit))
            ).mappings()
            return [_to_record(SessionRow(**row), SessionRecord) for row in rows]

    async def invitation_records(self, user_id, agent_ids):
        """按本次邀请的投影 ID 批读已有 Agent 和每项最新主会话。"""
        if not agent_ids:
            return {}, {}
        latest = (
            select(
                SessionRow.id,
                func.row_number()
                .over(
                    partition_by=SessionRow.agent_id,
                    order_by=SessionRow.created_at.desc(),
                )
                .label("position"),
            )
            .where(SessionRow.user_id == user_id, SessionRow.agent_id.in_(agent_ids))
            .subquery("invitation_sessions")
        )
        async with self.engine.connect() as conn:
            agents = {
                row["id"]: _to_record(AgentRow(**row), AgentRecord)
                for row in (
                    await conn.execute(
                        select(AgentRow.__table__).where(
                            AgentRow.user_id == user_id,
                            AgentRow.id.in_(agent_ids),
                        )
                    )
                ).mappings()
            }
            sessions = {
                row["agent_id"]: _to_record(SessionRow(**row), SessionRecord)
                for row in (
                    await conn.execute(
                        select(_SESSIONS)
                        .join(latest, _SESSIONS.c.id == latest.c.id)
                        .where(latest.c.position == 1)
                    )
                ).mappings()
            }
        return agents, sessions

    async def publish_created(self, agents, sessions, team=None):
        """原子发布本次新分配的原生图；记录 ID 均由本次请求生成。"""

        def values(row_class, records):
            return [
                {
                    field.name: getattr(row, field.name)
                    for field in row_class.__table__.c
                }
                for row in (_from_record(row_class, record) for record in records)
            ]

        async with self.engine.begin() as conn:
            if agents:
                await conn.execute(insert(AgentRow), values(AgentRow, agents))
            if sessions:
                await conn.execute(insert(SessionRow), values(SessionRow, sessions))
            if team is not None:
                await conn.execute(insert(TeamRow), values(TeamRow, [team]))

    async def save_team(self, record):
        """锁内更新已读取的团队，保留原始创建时间且不再回查。"""
        record.updated_at = _utcnow()
        row = _from_record(TeamRow, record)
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(TeamRow)
                .where(
                    TeamRow.id == record.id,
                    TeamRow.user_id == record.user_id,
                )
                .values(
                    payload=row.payload,
                    updated_at=row.updated_at,
                    session_id=row.session_id,
                )
            )
            if not result.rowcount:
                raise Forbidden("团队不存在或不可访问")

    async def save_state(self, record):
        """锁内已读取的会话只写一次；WHERE 仍限定完整归属。"""
        record.updated_at = _utcnow()
        row = _from_record(SessionRow, record)
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(SessionRow)
                .where(
                    SessionRow.id == record.id,
                    SessionRow.user_id == record.user_id,
                    SessionRow.agent_id == record.agent_id,
                )
                .values(payload=row.payload, updated_at=row.updated_at)
            )
            if not result.rowcount:
                raise Forbidden("会话归属不匹配")

    async def detach_collaborator(self, parent, team, worker_id):
        """原子更新主会话邀请记录、原生名单及工作会话归属。"""
        parent.updated_at = _utcnow()
        parent_row = _from_record(SessionRow, parent)
        async with self.engine.begin() as conn:
            await conn.execute(
                update(SessionRow)
                .where(SessionRow.id == parent.id, SessionRow.user_id == parent.user_id)
                .values(payload=parent_row.payload, updated_at=parent_row.updated_at)
            )
            await conn.execute(
                update(SessionRow)
                .where(SessionRow.id == worker_id, SessionRow.user_id == parent.user_id)
                .values(team_id=None)
            )
            if team:
                team.updated_at = parent.updated_at
                team_row = _from_record(TeamRow, team)
                await conn.execute(
                    update(TeamRow)
                    .where(TeamRow.id == team.id, TeamRow.user_id == parent.user_id)
                    .values(payload=team_row.payload, updated_at=team_row.updated_at)
                )

    async def delete_sessions(self, user_id, sessions, owned_projections):
        """按受影响的原生图批量清理，保留 created/invited 和旧名单规则。"""
        conn = transaction_connection(self.engine, None)
        session_pairs = {
            (user_id, row.id if hasattr(row, "id") else row[0]) for row in sessions
        }
        agent_pairs = {(user_id, aid) for aid in owned_projections}
        loaded_sessions, loaded_agents, schedules, teams = {}, set(), set(), {}
        pending_sessions, pending_agents = set(session_pairs), set(agent_pairs)
        pending_schedules = set()
        while pending_sessions or pending_agents or pending_schedules:
            agents = pending_agents - loaded_agents
            loaded_agents.update(agents)
            if agents:
                rows = (
                    await conn.execute(
                        select(ScheduleRow.__table__).where(
                            tuple_(ScheduleRow.user_id, ScheduleRow.agent_id).in_(
                                agents
                            ),
                            column("project_id").is_(None),
                        )
                    )
                ).mappings()
                pending_schedules.update((row["user_id"], row["id"]) for row in rows)
                schedules.update(pending_schedules)
            conditions = []
            if pending_sessions:
                conditions.append(
                    tuple_(SessionRow.user_id, SessionRow.id).in_(pending_sessions)
                )
            if agents:
                conditions.append(
                    tuple_(SessionRow.user_id, SessionRow.agent_id).in_(agents)
                )
            if pending_schedules:
                conditions.append(
                    tuple_(SessionRow.user_id, SessionRow.source_schedule_id).in_(
                        pending_schedules
                    )
                )
            pending_sessions, pending_agents, pending_schedules = set(), set(), set()
            if not conditions:
                continue
            rows = (
                await conn.execute(select(_SESSIONS).where(or_(*conditions)))
            ).mappings()
            fresh = {}
            for row in rows:
                pair = row["user_id"], row["id"]
                if pair not in loaded_sessions:
                    fresh[pair] = _to_record(SessionRow(**row), SessionRecord)
            loaded_sessions.update(fresh)
            team_pairs = {
                (record.user_id, record.team_id)
                for record in fresh.values()
                if record.team_id
            }
            if not team_pairs:
                continue
            rows = (
                await conn.execute(
                    select(TeamRow.__table__).where(
                        tuple_(TeamRow.user_id, TeamRow.id).in_(team_pairs)
                    )
                )
            ).mappings()
            legacy_pairs = set()
            for row in rows:
                team = _to_record(TeamRow(**row), TeamRecord)
                pair = team.user_id, team.id
                if (
                    pair in teams
                    or (team.user_id, team.session_id) not in loaded_sessions
                ):
                    continue
                teams[pair] = team
                if team.data.members:
                    for member in team.data.members:
                        if member.role == "created":
                            pending_agents.add((member.owner_id, member.agent_id))
                            agent_pairs.add((member.owner_id, member.agent_id))
                        else:
                            pending_sessions.add((team.user_id, member.session_id))
                else:
                    # Legacy rosters represented only created native agents.
                    legacy = team.data.model_dump().get("member_ids", [])
                    legacy_pairs.update((team.user_id, aid) for aid in legacy)
            if legacy_pairs:
                # Native legacy migration only materializes members which still
                # have a worker session; orphan agent IDs are left untouched.
                workers = (
                    await conn.execute(
                        select(SessionRow.user_id, SessionRow.agent_id)
                        .where(
                            tuple_(SessionRow.user_id, SessionRow.agent_id).in_(
                                legacy_pairs
                            )
                        )
                        .distinct()
                    )
                ).all()
                pending_agents.update(workers)
                agent_pairs.update(workers)
        # Only teams which actually mention deleted agents are read, once per batch.
        if agent_pairs:
            roster = cast(TeamRow.payload, JSONB)
            affected = or_(
                *[
                    (TeamRow.user_id == owner)
                    & or_(
                        roster.contains({"data": {"members": [{"agent_id": aid}]}}),
                        roster.contains({"data": {"member_ids": [aid]}}),
                    )
                    for owner, aid in agent_pairs
                ]
            )
            query = select(TeamRow.__table__).where(affected)
            if teams:
                query = query.where(tuple_(TeamRow.user_id, TeamRow.id).not_in(teams))
            updates = []
            for row in (await conn.execute(query)).mappings():
                team = _to_record(TeamRow(**row), TeamRecord)
                raw = team.data.model_dump()
                if raw.get("member_ids"):
                    team.data.member_ids = [
                        aid
                        for aid in raw["member_ids"]
                        if (team.user_id, aid) not in agent_pairs
                    ]
                team.data.members = [
                    m
                    for m in team.data.members
                    if (team.user_id, m.agent_id) not in agent_pairs
                ]
                team.updated_at = _utcnow()
                native = _from_record(TeamRow, team)
                updates.append(
                    {
                        "target_id": team.id,
                        "new_payload": native.payload,
                        "new_updated": native.updated_at,
                    }
                )
            if updates:
                await conn.execute(
                    update(TeamRow.__table__)
                    .where(TeamRow.id == bindparam("target_id"))
                    .values(
                        payload=bindparam("new_payload"),
                        updated_at=bindparam("new_updated"),
                    ),
                    updates,
                )
        if teams:
            # A deleted leader is already removed below; surviving leaders are detached.
            leader_pairs = {
                (team.user_id, team.session_id) for team in teams.values()
            } - loaded_sessions.keys()
            if leader_pairs:
                await conn.execute(
                    update(SessionRow)
                    .where(tuple_(SessionRow.user_id, SessionRow.id).in_(leader_pairs))
                    .values(team_id=None, updated_at=_utcnow())
                )
            await conn.execute(
                delete(TeamRow).where(tuple_(TeamRow.user_id, TeamRow.id).in_(teams))
            )
        if loaded_sessions:
            await conn.execute(
                delete(MessageRow).where(
                    MessageRow.session_id.in_([sid for _, sid in loaded_sessions])
                )
            )
            await conn.execute(
                delete(SessionRow).where(
                    tuple_(SessionRow.user_id, SessionRow.id).in_(loaded_sessions)
                )
            )
        if schedules:
            await conn.execute(
                delete(ScheduleRow).where(
                    tuple_(ScheduleRow.user_id, ScheduleRow.id).in_(schedules)
                )
            )
        if agent_pairs:
            await conn.execute(
                delete(AgentRow).where(
                    tuple_(AgentRow.user_id, AgentRow.id).in_(agent_pairs)
                )
            )
