"""Small user authorization sets drive joins even with a large unrelated catalog."""

import json

import pytest
from sqlalchemy import insert, text
from test_domain import identities

from egis_agentkit.storage import tables as t
from egis_agentkit.storage.model import utcnow


def plan_nodes(plan):
    yield plan
    for child in plan.get("Plans", []):
        yield from plan_nodes(child)


@pytest.mark.parametrize("unrelated", [0, 20000, 50000])
async def test_expert_page_joins_from_scoped_eligible_ids(storage, unrelated):
    _, _, alice, _ = await identities(storage)
    identifiers = [f"a{index:05}" for index in range(unrelated)] + [
        f"z{index:02}" for index in range(10)
    ]
    async with storage.transaction() as conn:
        await conn.execute(
            insert(t.agent_definition_summary),
            [
                {
                    "id": identifier,
                    "owner_type": "system",
                    "owner_id": "platform",
                    "agent_type": "expert",
                    "agent_name": identifier,
                    "owner_version": 1,
                    "created_by": "bootstrap",
                    "updated_by": "bootstrap",
                }
                for identifier in identifiers
            ],
        )
        await conn.execute(
            insert(t.agent_definition_detail),
            [
                {
                    "definition_id": identifier,
                    "version": 1,
                    "agent_name": identifier,
                    "agent_display_name": identifier,
                    "payload": {"description": "expert", "bindings": []},
                    "created_by": "bootstrap",
                }
                for identifier in identifiers
            ],
        )
        await conn.execute(
            insert(t.resource_permission),
            [
                {
                    "id": f"grant{index:02}",
                    "principal_type": "user",
                    "principal_id": "alice",
                    "resource_type": "agent",
                    "resource_id": f"z{index:02}",
                    "resource_view_type": "system",
                    "resource_owner_type": "system",
                    "resource_owner_id": "platform",
                    "grant_source_type": "manual",
                    "origin_key": "manual",
                    "granted_by": "bootstrap",
                    "granted_at": utcnow(),
                }
                for index in range(10)
            ],
        )
        await conn.execute(text("ANALYZE ak_agent_definition_summary"))
        await conn.execute(text("ANALYZE ak_agent_definition_detail"))
        await conn.execute(text("ANALYZE ak_resource_permission"))
    rows = await storage.agents.page_visible_definitions(
        alice, ("expert",), after=None, limit=10, order_by_id=True
    )
    assert [row["id"] for row in rows] == [f"z{index:02}" for index in range(10)]
    statement = (
        storage.agents._select_visible_definitions(alice, ("expert",))
        .order_by(t.agent_definition_summary.c.id)
        .limit(10)
    )
    compiled = statement.compile(
        dialect=storage.engine.dialect, compile_kwargs={"literal_binds": True}
    )
    async with storage.engine.connect() as conn:
        raw = await conn.scalar(
            text("EXPLAIN (ANALYZE, BUFFERS, FORMAT JSON) " + str(compiled))
        )
    plan = json.loads(raw) if isinstance(raw, str) else raw
    root = plan[0]["Plan"]
    catalog_scans = [
        node
        for node in plan_nodes(root)
        if node.get("Relation Name") == "ak_agent_definition_summary"
    ]
    visited = sum(
        (node.get("Actual Rows", 0) + node.get("Rows Removed by Filter", 0))
        * node.get("Actual Loops", 1)
        for node in catalog_scans
    )
    print(
        f"AUTHORIZATION_PLAN unrelated={unrelated} summary_rows_visited={visited} execution_ms={plan[0]['Execution Time']}"
    )
    assert visited <= 30, plan
