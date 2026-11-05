import pytest
from test_domain import expert, identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.models import Grant, Group
from egis_agentkit.services import AgentService, PermissionService
from egis_agentkit.services.teams import TeamService


@pytest.mark.asyncio
async def test_team_roles_and_revocation_keep_independent_grants(storage):
    users, admin, alice, _ = await identities(storage)
    agents = AgentService(storage)
    first = await agents.save(admin, expert("egis-demo", "system", "research"))
    second = await agents.save(admin, expert("egis-demo", "system", "writer"))
    teams = TeamService(storage)
    team = await teams.save(
        admin,
        {
            "owner_type": "system",
            "owner_id": "egis-demo",
            "team_name": "delivery",
            "team_display_name": "交付专家团",
            "orchestrator_config": {
                "agent_name": "coordinator",
                "agent_display_name": "主理人",
                "payload": {},
            },
            "members": [
                {"definition_id": first["id"], "resource_view_type": "system"},
                {"definition_id": second["id"], "resource_view_type": "system"},
            ],
        },
    )
    permissions = PermissionService(storage)
    await users.save_group(admin, Group(group_id="engineering", group_name="研发"))
    alice.group_ids = ["engineering"]
    await users.update_user(admin, alice)
    team_grant = await permissions.grant(
        admin,
        Grant(
            principal_type="group",
            principal_id="engineering",
            resource_type="team",
            resource_id=team["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="egis-demo",
        ),
    )
    await permissions.grant(
        admin,
        Grant(
            principal_type="user",
            principal_id="alice",
            resource_type="agent",
            resource_id=first["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="egis-demo",
        ),
    )
    assert (
        len((await permissions.explain(alice, "agent", first["id"], "system")).paths)
        == 2
    )
    await permissions.revoke(admin, team_grant["id"])
    assert (await permissions.explain(alice, "agent", first["id"], "system")).is_allowed
    assert not (
        await permissions.explain(alice, "agent", second["id"], "system")
    ).is_allowed
    with pytest.raises(Forbidden):
        await teams.save(
            alice,
            {
                "team_name": "bad",
                "team_display_name": "非法团",
                "orchestrator_id": first["id"],
                "members": [],
            },
        )
