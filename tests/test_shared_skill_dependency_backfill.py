"""Approved historical expert/team snapshots pin private skills for NAS backfill."""

import copy
import shutil

import pytest
from sqlalchemy import event, select
from test_domain import expert, identities

from egis_agentkit.errors import AgentKitError
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.resource._catalog import ResourceService
from egis_agentkit.service.team._definition import TeamDefinition
from egis_agentkit.storage import tables as t
from egis_agentkit.storage.migration._shared_skills import backfill_shared_skills
from egis_agentkit.storage.model import ShareTarget
from egis_agentkit.storage.model.agent._update import AgentUpdate


def binding(identifier):
    return {
        "resource_type": "skill",
        "resource_id": identifier,
        "resource_version": "1.0.0",
        "resource_view_type": "private",
    }


async def rows(storage, table):
    async with storage.engine.connect() as conn:
        return [dict(row) for row in (await conn.execute(select(table))).mappings()]


@pytest.mark.parametrize("kind", ["agent", "team"])
async def test_backfill_includes_historical_approved_private_dependencies_without_pointer(
    storage, tmp_path, kind
):
    _, admin, alice, bob = await identities(storage)
    resources = ResourceService(storage, tmp_path)
    for identifier in ("old-report", "new-report", "unapproved"):
        await resources.save_skill(
            alice, {"resource_id": identifier, "markdown": f"# {identifier}"}
        )
    agents = AgentManager(storage)
    definition = expert()
    definition.payload["bindings"] = [binding("old-report")]
    member = await agents.add_definition(alice, definition)
    teams = TeamDefinition(storage)
    team_data = dict(
        team_name="reports",
        team_display_name="Reports",
        payload={},
        orchestrator_config=dict(
            agent_name="leader", agent_display_name="Leader", payload={}
        ),
        members=[dict(definition_id=member["id"], resource_view_type="private")],
    )
    root = member if kind == "agent" else await teams.save(alice, team_data)
    shares = ShareService(storage, publish_skill=resources.publish_shared_skill)
    review = await shares.submit(
        alice,
        kind,
        root["id"],
        target=ShareTarget(principal_type="user", principal_id=bob.user_id),
    )
    await shares.decide(admin, review["review_id"], True, "approved old binding")
    original_review = next(
        row
        for row in await rows(storage, t.approvals)
        if row["review_id"] == review["review_id"]
    )
    async with storage.transaction() as conn:
        # Distinct approved audience reviews force multiple bounded history pages.
        await conn.execute(
            t.approvals.insert(),
            [
                {
                    **original_review,
                    "review_id": f"history-{index:03}",
                    "is_current": False,
                }
                for index in range(101)
            ],
        )

    async def change_member(identifier, member):
        return await agents.update_definition(
            alice,
            member["id"],
            AgentUpdate(
                agent_name=member["agent_name"],
                agent_display_name=member["agent_display_name"],
                owner_version=member["owner_version"],
                payload={"bindings": [binding(identifier)]},
            ),
        )

    member = await change_member("new-report", member)
    if kind == "team":
        update = copy.deepcopy(team_data)
        update.update(id=root["id"], owner_version=root["owner_version"])
        root = await teams.save(alice, update)
    review = await shares.submit(alice, kind, root["id"])
    await shares.decide(admin, review["review_id"], True, "approved new binding")
    member = await change_member("unapproved", member)
    if kind == "team":
        update = copy.deepcopy(team_data)
        update.update(id=root["id"], owner_version=root["owner_version"])
        await teams.save(alice, update)
    await resources.save_skill(
        alice,
        dict(
            resource_id="old-report",
            resource_version="2.0.0",
            base_version="1.0.0",
            markdown="# Private newer source",
        ),
    )
    if (tmp_path / "common").exists():
        shutil.rmtree(tmp_path / "common")
    before = {
        table.name: await rows(storage, table)
        for table in (t.approvals, t.resource_permission, t.skill_summary)
    }
    queries = []

    def capture(conn, cursor, statement, parameters, context, executemany):
        if statement.startswith("SELECT") and "ak_approve" in statement:
            queries.append(statement)

    event.listen(storage.engine.sync_engine, "before_cursor_execute", capture)
    try:
        preview = await backfill_shared_skills(resources)
    finally:
        event.remove(storage.engine.sync_engine, "before_cursor_execute", capture)
    expected = {("old-report", "1.0.0"), ("new-report", "1.0.0")}
    assert {
        (row["resource_id"], row["resource_version"]) for row in preview
    } == expected
    assert len(preview) == 2
    assert queries and all("LIMIT" in statement for statement in queries)
    assert not (tmp_path / "common").exists()
    assert await backfill_shared_skills(resources, resource_ids=["unapproved"]) == []
    selected = await backfill_shared_skills(resources, resource_ids=["old-report"])
    assert [(row["resource_id"], row["resource_version"]) for row in selected] == [
        ("old-report", "1.0.0")
    ]
    assert await backfill_shared_skills(resources, apply=True) == preview
    assert await backfill_shared_skills(resources, apply=True) == preview
    assert (
        tmp_path / "common/skills/old-report/1.0.0/SKILL.md"
    ).read_text() == "# old-report"
    assert (
        tmp_path / "common/skills/new-report/1.0.0/SKILL.md"
    ).read_text() == "# new-report"
    assert not (tmp_path / "common/skills/old-report/2.0.0").exists()
    assert not (tmp_path / "common/skills/unapproved").exists()
    assert {
        table.name: await rows(storage, table)
        for table in (t.approvals, t.resource_permission, t.skill_summary)
    } == before
    assert all(row["shared_version"] is None for row in before[t.skill_summary.name])
    shutil.rmtree(tmp_path / "common")
    shutil.rmtree(tmp_path / "user/alice/skills/old-report")
    with pytest.raises(AgentKitError, match="old-report.*1.0.0.*alice"):
        await backfill_shared_skills(resources, apply=True, resource_ids=["old-report"])
