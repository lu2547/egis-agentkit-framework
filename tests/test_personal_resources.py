"""验证个人资源开关独立于组授权及资源版本。"""

import pytest
from sqlalchemy import inspect, select, text
from test_domain import COMPANY, identities
from test_resources import put_asset

from egis_agentkit.errors import NotFound
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.migration._personal_resources import (
    migrate_personal_resources,
)


@pytest.mark.asyncio
async def test_personal_resources_are_queried_by_user_without_crossing_users(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.personal.add_resource(alice, "skill", "report")
    await resources.personal.update_resource_enabled(alice, "skill", "report", False)
    await resources.personal.add_resource(bob, "skill", "report")

    alice_resources = await storage.personal_resources.list_resources("alice", "skill")
    bob_resources = await storage.personal_resources.list_resources("bob", "skill")

    assert set(alice_resources) == {"report"}
    assert alice_resources["report"]["user_id"] == "alice"
    assert alice_resources["report"]["is_enabled"] is False
    assert bob_resources["report"]["user_id"] == "bob"
    assert bob_resources["report"]["is_enabled"] is True


@pytest.mark.asyncio
async def test_removing_personal_selection_preserves_resource_grants_and_other_users(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    await put_asset(storage, owner_type="user", owner_id="admin")
    shares = ShareService(storage)
    review = await shares.submit(admin, "skill", "report", target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "通过")
    resources = ResourceService(storage, tmp_path)
    await resources.install(alice, "skill", "report")
    await resources.install(bob, "skill", "report")
    async with storage.engine.connect() as connection:
        grants_before = (
            (await connection.execute(select(t.resource_permission))).mappings().all()
        )

    await resources.remove_installation(alice, "skill", "report")

    assert await resources.personal.list_resources(alice, "skill") == {}
    assert "report" in await resources.personal.list_resources(bob, "skill")
    with pytest.raises(NotFound):
        await resources.update_installation(alice, "skill", "report", True)
    async with storage.engine.connect() as connection:
        assert (
            await connection.execute(select(t.resource_permission))
        ).mappings().all() == grants_before
        assert await connection.scalar(select(t.skill_summary.c.skill_id)) == "report"
    await resources.install(alice, "skill", "report")
    assert (await resources.personal.list_resources(alice, "skill"))["report"][
        "is_enabled"
    ]


@pytest.mark.asyncio
async def test_personal_resource_user_upgrade_uses_owner_and_preserves_switches(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.personal.add_resource(alice, "skill", "report")
    await resources.personal.update_resource_enabled(alice, "skill", "report", False)
    await resources.personal.add_resource(bob, "skill", "report")

    async with storage.engine.begin() as connection:
        schema = await connection.scalar(text("SELECT current_schema()"))
        columns = await connection.run_sync(
            lambda conn: inspect(conn).get_columns("ak_agent_personal_resources")
        )
        if any(column["name"] == "user_id" for column in columns):
            await connection.execute(
                text("ALTER TABLE ak_agent_personal_resources DROP COLUMN user_id")
            )
        # 操作者可以与资源归属不同，回填必须取个人助理的 owner_id。
        await connection.execute(
            text("UPDATE ak_agent_personal_resources SET created_by='admin'")
        )
        assert await migrate_personal_resources(connection, schema) == "pending"
        assert (
            await migrate_personal_resources(connection, schema, apply=True)
            == "upgraded"
        )
        assert await migrate_personal_resources(connection, schema) == "current"

    alice_resources = await storage.personal_resources.list_resources("alice", "skill")
    bob_resources = await storage.personal_resources.list_resources("bob", "skill")
    assert alice_resources["report"]["user_id"] == "alice"
    assert alice_resources["report"]["is_enabled"] is False
    assert bob_resources["report"]["user_id"] == "bob"
    assert bob_resources["report"]["is_enabled"] is True


@pytest.mark.asyncio
async def test_personal_switches_are_independent_and_follow_current_shared_version(
    storage, tmp_path
):
    _, admin, alice, bob = await identities(storage)
    await put_asset(storage, owner_type="user", owner_id="admin")
    shares = ShareService(storage)
    review = await shares.submit(admin, "skill", "report", target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "通过")
    resources = ResourceService(storage, tmp_path)
    await resources.install(alice, "skill", "report")
    await resources.install(bob, "skill", "report")
    await resources.update_installation(alice, "skill", "report", False)
    assert (await resources.personal.list_resources(alice, "skill"))["report"][
        "is_enabled"
    ] is False
    assert (await resources.personal.list_resources(bob, "skill"))["report"][
        "is_enabled"
    ] is True
    await put_asset(
        storage, version="2.0.0", owner_type="user", owner_id="admin", revision=2
    )
    update = await shares.submit(admin, "skill", "report")
    await shares.decide(admin, update["review_id"], True, "通过新版")
    assert (
        await resources.selected_for_personal(alice, "skill", enabled_only=True) == {}
    )
    selected = await resources.selected_for_personal(bob, "skill", enabled_only=True)
    assert selected["report"]["resource_version"] == "2.0.0"
    async with storage.engine.connect() as connection:
        rows = (
            (await connection.execute(select(t.agent_personal_resources)))
            .mappings()
            .all()
        )
        columns = await connection.run_sync(
            lambda conn: inspect(conn).get_columns("ak_agent_personal_resources")
        )
    assert len(rows) == 2
    assert not {"version", "resource_version", "payload"} & {
        column["name"] for column in columns
    }


@pytest.mark.asyncio
async def test_personal_switch_upgrade_keeps_latest_choice_and_other_hubs(
    storage, tmp_path
):
    from agentscope.app.storage import SkillRecord

    _, _, alice, _ = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    await resources.personal.get_or_add_definition(alice)
    # 显式构造升级前的原生表，当前工作台初始化已不再创建它。
    from agentscope.app.storage._sql._tables import SkillRow

    async with storage.engine.begin() as connection:
        await connection.run_sync(lambda conn: SkillRow.__table__.create(conn))
    for identifier, hub, version, enabled in (
        ("first", "egis-local", "1.0.0", True),
        ("second", "egis-local", "2.0.0", False),
        ("external", "other-hub", "1.0.0", True),
    ):
        await storage.upsert_skill(
            "alice",
            SkillRecord(
                id=identifier,
                user_id="alice",
                name=identifier,
                hub_id=hub,
                card_id="report",
                version=version,
                enabled=enabled,
            ),
        )
    async with storage.engine.begin() as connection:
        schema = await connection.scalar(text("SELECT current_schema()"))
        await connection.run_sync(lambda conn: t.agent_personal_resources.drop(conn))
        assert await migrate_personal_resources(connection, schema) == "pending"
        assert (
            await migrate_personal_resources(connection, schema, apply=True)
            == "upgraded"
        )
        assert (
            await migrate_personal_resources(connection, schema, apply=True)
            == "current"
        )
    state = (await resources.personal.list_resources(alice, "skill"))["report"]
    assert state["is_enabled"] is False and "resource_version" not in state
    assert [record.id for record in await storage.list_skills("alice")] == ["external"]
