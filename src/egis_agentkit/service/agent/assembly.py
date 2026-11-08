"""Prepare native agent records from resolved definitions."""

from typing import Literal

from agentscope.app.storage import AgentChatConfig, AgentData, AgentRecord, InviteConfig


class AgentRecordFactory:
    @staticmethod
    def create(
        user_id: str,
        *,
        name: str,
        system_prompt: str,
        source: Literal["user", "team"] = "user",
        record_id: str | None = None,
        invite_description: str | None = None,
    ) -> AgentRecord:
        return AgentRecord(
            user_id=user_id,
            source=source,
            data=AgentData(
                name=name,
                system_prompt=system_prompt,
                chat_config=AgentChatConfig(
                    invite_config=InviteConfig(
                        invitable=invite_description is not None,
                        invite_description=invite_description,
                    ),
                ),
            ),
            **({"id": record_id} if record_id is not None else {}),
        )

    @staticmethod
    def project(record: AgentRecord, *, name: str, system_prompt: str) -> AgentRecord:
        return record.model_copy(
            update={
                "data": AgentData(
                    id=record.data.id,
                    name=name,
                    system_prompt=system_prompt,
                )
            },
            deep=True,
        )
