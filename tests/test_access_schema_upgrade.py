"""Validate stable authorization migration in a disposable PostgreSQL schema."""

import pytest
from sqlalchemy import text
from test_domain import expert, identities

from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.storage.model import Grant

from test_domain import agent_changes


@pytest.mark.asyncio
async def test_access_upgrade_preserves_root_and_rejects_conflicting_versions(storage):
    from egis_agentkit.storage._access_schema import (
        check_access_upgrade,
        upgrade_access_tables,
    )

    _, admin, alice, bob = await identities(storage)
    definitions = AgentManager(storage)
    value = expert()
    first = await definitions.add_definition(alice, value)
    grant = await GrantService(storage).grant(
        admin,
        Grant(
            principal_type="user",
            principal_id=bob.user_id,
            resource_type="agent",
            resource_id=value.id,
            resource_view_type="shared",
            resource_owner_type="user",
            resource_owner_id=alice.user_id,
        ),
    )
    await definitions.update_definition(alice, (value).id, agent_changes(value))
    async with storage.engine.begin() as conn:
        schema = await conn.scalar(text("SELECT current_schema()"))
        await conn.execute(
            text("ALTER TABLE ak_agent_definition_summary DROP COLUMN shared_version")
        )
        await conn.execute(
            text("ALTER TABLE ak_team_definitions DROP COLUMN shared_version")
        )
        await conn.execute(
            text(
                "ALTER TABLE ak_resource_assets DROP COLUMN is_shared_current, DROP COLUMN publication_revision"
            )
        )
        await conn.execute(text("ALTER TABLE ak_share_reviews DROP COLUMN review_type"))
        await conn.execute(
            text(
                "ALTER TABLE ak_resource_access ADD COLUMN resource_version text NOT NULL DEFAULT '1'"
            )
        )
        await conn.execute(
            text("""
            INSERT INTO ak_resource_access (
                id, principal_type, principal_id, resource_type, resource_id,
                resource_version, resource_view_type, resource_owner_type,
                resource_owner_id, grant_source_type, origin_key, granted_by, granted_at
            ) VALUES ('legacy-conflict', 'user', 'alice', 'agent', :resource,
                '2', 'shared', 'user', 'alice', 'manual', 'manual', 'admin', now())
        """),
            {"resource": value.id},
        )
        assert await check_access_upgrade(conn, schema) == "inconsistent"
    with pytest.raises(ValueError, match=value.id):
        async with storage.engine.begin() as conn:
            await upgrade_access_tables(conn, schema)
    async with storage.engine.begin() as conn:
        assert await check_access_upgrade(conn, schema) == "inconsistent"
        await conn.execute(
            text("DELETE FROM ak_resource_access WHERE id='legacy-conflict'")
        )
        assert await check_access_upgrade(conn, schema) == "pending"
        assert await upgrade_access_tables(conn, schema) == "upgraded"
        assert await check_access_upgrade(conn, schema) == "current"
        assert await upgrade_access_tables(conn, schema) == "current"
    allowed = await PermissionService(storage).require(bob, "agent", value.id, "shared")
    assert allowed.grant_id == grant["id"]
    assert allowed.resource_version == first["resource_version"]
