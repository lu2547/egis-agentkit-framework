"""Prepare native team records from resolved membership."""

from agentscope.app.storage import TeamData, TeamMember, TeamRecord


class TeamRecordFactory:
    @staticmethod
    def create(
        user_id: str,
        *,
        leader_agent_id: str,
        leader_session_id: str,
        name: str,
        description: str,
        members: list[TeamMember],
    ) -> TeamRecord:
        return TeamRecord(
            user_id=user_id,
            session_id=leader_session_id,
            leader_agent_id=leader_agent_id,
            data=TeamData(name=name, description=description, members=members),
        )
