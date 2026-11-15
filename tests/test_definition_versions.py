"""验证不可变定义和专家团的固定成员版本。"""

import pytest

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service._user import UserService
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.storage.model import AgentDefinition


def expert():
    return AgentDefinition(
        owner_id="alice",
        agent_name="writer",
        agent_display_name="写作专家",
        payload={"system_prompt": "旧版"},
    )


@pytest.mark.asyncio
async def test_agent_versions_preserve_configuration_after_an_update(storage):
    original = expert()
    async with storage.definition_write() as write:
        await write.definitions.add_definition(
            original.model_dump(mode="json"), actor="alice"
        )
    updated = original.model_copy(
        update={
            "agent_name": "editor",
            "agent_display_name": "编辑专家",
            "payload": {"system_prompt": "新版"},
        }
    )
    async with storage.definition_write() as write:
        current = await write.definitions.update_definition(
            original.id, updated.model_dump(mode="json"), actor="alice"
        )
    assert current["owner_version"] == 2
    repository = storage.definitions
    first = await repository.get_definition(original.id, version="1")
    assert first["agent_name"] == "writer"
    assert first["agent_display_name"] == "写作专家"
    assert first["payload"]["system_prompt"] == "旧版"
    assert (await repository.get_definition(original.id))["agent_name"] == "editor"
    assert (await repository.get_definition_view(original.id, "private", version=2))["payload"][
        "system_prompt"
    ] == "新版"
    with pytest.raises(NotFound):
        await repository.get_definition(original.id, version=3)
    with pytest.raises(Conflict):
        async with storage.definition_write() as write:
            await write.definitions.update_definition(
                original.id, updated.model_dump(mode="json"), actor="alice"
            )
    async with storage.definition_write() as write:
        await write.definitions.delete_definition(original.id, actor="alice")
    assert (await repository.get_definition(original.id, version=1))["payload"][
        "system_prompt"
    ] == "旧版"
    with pytest.raises(Forbidden):
        await repository.get_definition_view(original.id, "private", version=1)


@pytest.mark.asyncio
async def test_team_versions_keep_pinned_leader_and_expert_versions(storage):
    users = UserService(storage)
    await users.sync_identity("alice", "小明")
    alice = await users.get("alice")
    original = expert()
    async with storage.definition_write() as write:
        await write.definitions.add_definition(
            original.model_dump(mode="json"), actor=alice.user_id
        )
    teams = TeamDefinition(storage)
    member = {"definition_id": original.id, "resource_view_type": "private"}
    data = {
        "team_name": "delivery",
        "team_display_name": "交付专家团",
        "orchestrator_config": {"agent_name": "leader", "agent_display_name": "主理人"},
        "members": [member],
    }
    first = await teams.save(alice, data)
    async with storage.definition_write() as write:
        await write.definitions.update_definition(
            original.id,
            {**original.model_dump(mode="json"), "agent_display_name": "新版专家"},
            actor=alice.user_id,
        )
    second = await teams.save(
        alice,
        {
            **data,
            "id": first["id"],
            "owner_version": first["owner_version"],
            "members": [{**member, "resource_version": "1"}],
            "orchestrator_config": {
                "agent_name": "leader",
                "agent_display_name": "新版主理人",
            },
        },
    )
    third = await teams.save(
        alice, {**data, "id": first["id"], "owner_version": second["owner_version"]}
    )
    historical = await teams.read(alice, first["id"], version="1")
    assert not historical["is_editable"]
    assert historical["orchestrator_config"]["agent_display_name"] == "主理人"
    assert historical["payload"]["orchestrator"]["resource_version"] == "1"
    assert historical["payload"]["members"] == [{**member, "resource_version": "1"}]
    assert second["payload"]["orchestrator"]["resource_version"] == "2"
    assert second["payload"]["members"] == [{**member, "resource_version": "1"}]
    assert third["payload"]["members"] == [{**member, "resource_version": "2"}]
    assert await teams.versions(alice, first["id"]) == [
        {"resource_version": "3", "resource_view_type": "private"},
        {"resource_version": "2", "resource_view_type": "private"},
        {"resource_version": "1", "resource_view_type": "private"},
    ]


def test_core_builds_native_record():
    from egis_agentkit.core.agent._definition_base import AgentDefinitionBase

    record = AgentDefinitionBase.build_agent(
        "alice", {"agent_name": "finance", "payload": {"system_prompt": "财务专家"}}, source="team"
    )
    assert record.data.name == "finance"
    assert record.source == "team"
