"""验证 Agent 管理边界、版本读取和数据库权限分页。"""

import importlib

import pytest
from sqlalchemy import event, inspect, text
from test_domain import COMPANY, agent_changes, expert, identities
from test_resources import put_asset

from egis_agentkit.errors import Conflict, Forbidden, NotFound
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage.model import Grant, ResourceRef


def agent_manager(storage):
    module = importlib.import_module("egis_agentkit.service.agent._agent_manager")
    return module.AgentManager(storage)


@pytest.mark.asyncio
async def test_agent_manager_preserves_owner_and_shared_versions(storage):
    _, admin, alice, bob = await identities(storage)
    manager = agent_manager(storage)
    created = await manager.add_definition(alice, expert())
    assert (created["owner_version"], created["version"]) == (1, 1)
    review = await ShareService(storage).submit(
        alice, "agent", created["id"], target=COMPANY
    )
    await ShareService(storage).decide(admin, review["review_id"], True, "approved")
    changed = expert()
    changed.id = created["id"]
    changed.payload["system_prompt"] = "新版"
    latest = await manager.update_definition(alice, (changed).id, agent_changes(changed))
    assert (latest["owner_version"], latest["version"]) == (2, 2)
    assert (await manager.get_definition(bob, created["id"], "shared"))["payload"][
        "system_prompt"
    ] == "旧版"
    assert (await manager.get_definition(alice, created["id"], "private"))["payload"][
        "system_prompt"
    ] == "新版"
    with pytest.raises(Conflict):
        await manager.update_definition(alice, (changed).id, agent_changes(changed))


@pytest.mark.asyncio
async def test_agent_paging_filters_access_in_sql_without_candidate_scan(storage):
    _, _, alice, bob = await identities(storage)
    manager = agent_manager(storage)
    visible = [
        await manager.add_definition(alice, expert(name=f"visible_{index}"))
        for index in range(3)
    ]
    for index in range(40):
        await manager.add_definition(bob, expert("bob", name=f"hidden_{index}"))
    statements = []

    def record(connection, cursor, statement, parameters, context, many):
        if statement.lstrip().upper().startswith(("SELECT", "WITH")):
            statements.append(statement)

    event.listen(storage.engine.sync_engine, "before_cursor_execute", record)
    try:
        first = await manager.page_definitions(alice, limit=2)
        assert [row["id"] for row in first["items"]] == [
            visible[2]["id"],
            visible[1]["id"],
        ]
        assert len(statements) <= 4
        second = await manager.page_definitions(alice, limit=2, cursor=first["next_cursor"])
        assert [row["id"] for row in second["items"]] == [visible[0]["id"]]
        assert second["next_cursor"] is None
    finally:
        event.remove(storage.engine.sync_engine, "before_cursor_execute", record)


@pytest.mark.asyncio
async def test_agent_delete_hides_all_versions_and_preserves_history(storage):
    _, _, alice, _ = await identities(storage)
    manager = agent_manager(storage)
    created = await manager.add_definition(alice, expert())
    await manager.delete_definition(alice, created["id"])
    assert (await manager.page_definitions(alice))["items"] == []
    with pytest.raises(Forbidden):
        await manager.get_definition(alice, created["id"], "private")
    async with storage.engine.connect() as connection:
        tables = await connection.run_sync(lambda conn: inspect(conn).get_table_names())
        assert "ak_agent_definition_summary" in tables
        assert "ak_agent_definition_detail" in tables
        assert "ak_resource_permission" in tables
        columns = await connection.run_sync(
            lambda conn: inspect(conn).get_columns("ak_agent_definition_summary")
        )
        assert "owner_version" in {item["name"] for item in columns}
        assert "deleted_at" in {item["name"] for item in columns}


@pytest.mark.asyncio
async def test_agent_search_cursor_is_bound_to_the_same_query(storage):
    _, _, alice, _ = await identities(storage)
    manager = agent_manager(storage)
    for name in ("alpha_one", "alpha_two", "beta"):
        await manager.add_definition(alice, expert(name=name))
    page = await manager.page_definitions(alice, query="alpha", limit=1)
    assert page["next_cursor"]
    with pytest.raises(ValueError, match="游标"):
        await manager.page_definitions(alice, query="beta", limit=1, cursor=page["next_cursor"])


@pytest.mark.asyncio
async def test_expert_catalog_loads_ten_and_filters_visible_definitions_before_paging(
    storage,
):
    _, admin, alice, bob = await identities(storage)
    manager = agent_manager(storage)
    target = expert(name="catalog_target")
    target.payload.update(category="分析报告", tags=["月报模板"])
    await manager.add_definition(alice, target)
    for index in range(11):
        await manager.add_definition(alice, expert(name=f"catalog_item_{index}"))

    shared = expert("bob", name="catalog_shared")
    shared.payload["category"] = "跨组协作"
    await manager.add_definition(bob, shared)
    sharing = ShareService(storage)
    review = await sharing.submit(bob, "agent", shared.id, target=COMPANY)
    await sharing.decide(admin, review["review_id"], True, "已确认")
    hidden = expert("bob", name="catalog_hidden")
    hidden.payload["category"] = "仅所有者可见"
    await manager.add_definition(bob, hidden)

    first = await manager.page_definitions(alice, catalog=True, mine=True)
    assert len(first["items"]) == 10
    assert target.id not in {row["id"] for row in first["items"]}
    assert set(first["categories"]) == {"分析报告", "办公协同", "跨组协作"}
    second = await manager.page_definitions(
        alice, catalog=True, mine=True, cursor=first["next_cursor"]
    )
    assert len(second["items"]) == 2 and second["next_cursor"] is None
    assert target.id in {row["id"] for row in second["items"]}
    assert "categories" not in second
    with pytest.raises(ValueError, match="游标"):
        await manager.page_definitions(
            alice, catalog=True, cursor=first["next_cursor"], category="分析报告"
        )

    matching = await manager.page_definitions(
        alice, catalog=True, query="月报模板", category="分析报告"
    )
    assert [row["id"] for row in matching["items"]] == [target.id]
    assert matching["next_cursor"] is None
    assert set(matching["categories"]) == set(first["categories"])


@pytest.mark.asyncio
async def test_resource_admin_list_uses_active_agent_bindings(storage, tmp_path):
    _, admin, alice, _ = await identities(storage)
    await put_asset(storage, owner_type="user", owner_id=alice.user_id)
    data = expert()
    data.payload["bindings"] = [
        ResourceRef(
            resource_type="skill",
            resource_id="report",
            resource_version="1.0.0",
            resource_view_type="private",
        ).model_dump()
    ]
    manager = agent_manager(storage)
    created = await manager.add_definition(alice, data)
    resources = ResourceService(storage, tmp_path)
    assert (await resources.admin_list(admin, "skill"))[0]["bound_agents"] == [
        {"id": created["id"], "name": created["agent_display_name"]}
    ]
    await manager.delete_definition(alice, created["id"])
    assert (await resources.admin_list(admin, "skill"))[0]["bound_agents"] == []


@pytest.mark.asyncio
async def test_system_agent_requires_visibility_grant_even_for_admin(storage):
    _, admin, alice, _ = await identities(storage)
    manager = agent_manager(storage)
    created = await manager.add_definition(admin, expert("platform", "system"))
    assert (await manager.page_definitions(admin))["items"] == []
    grants = GrantService(storage)
    for recipient in (admin, alice):
        await grants.grant(
            admin,
            Grant(
                principal_type="user",
                principal_id=recipient.user_id,
                resource_type="agent",
                resource_id=created["id"],
                resource_view_type="system",
                resource_owner_type="system",
                resource_owner_id="platform",
            ),
        )
    admin_row = (await manager.page_definitions(admin))["items"][0]
    assert admin_row["is_usable"] and admin_row["is_editable"]
    user_row = (await manager.page_definitions(alice))["items"][0]
    assert user_row["is_usable"] and not user_row["is_editable"]


@pytest.mark.asyncio
async def test_grant_catalog_is_system_only_and_owner_label_matches_shared_version(storage):
    _, admin, alice, _ = await identities(storage)
    manager = agent_manager(storage)
    data = expert()
    created = await manager.add_definition(alice, data)
    shares = ShareService(storage)
    review = await shares.submit(alice, "agent", created["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "approved")
    data.payload["description"] = "尚未审核的新内容"
    await manager.update_definition(alice, data.id, agent_changes(data))
    system = await manager.add_definition(
        admin, expert("platform", "system", "system_writer")
    )
    rows = await GrantService(storage).grantable_resources(admin)
    assert [item["resource_id"] for item in rows] == [system["id"]]
    row = rows[0]
    assert row["resource_version"] == "1"
    assert row["version_label"] == "1.0"
    owner = await manager.get_definition(alice, created["id"], "private")
    assert owner["resource_version"] == "2"
    assert owner["share_version"] == "1"
    assert owner["share_version_label"] == "1.0"
    assert owner["can_share_update"]


@pytest.mark.asyncio
async def test_agent_creation_rolls_back_when_owner_access_cannot_be_saved(storage):
    _, _, alice, _ = await identities(storage)
    async with storage.transaction() as connection:
        await connection.execute(
            text(
                "ALTER TABLE ak_resource_permission ADD CONSTRAINT reject_owner_access "
                "CHECK (grant_source_type <> 'owner')"
            )
        )
    data = expert()
    with pytest.raises(Conflict):
        await agent_manager(storage).add_definition(alice, data)
    with pytest.raises(NotFound):
        await storage.agents.get_definition(data.id)
