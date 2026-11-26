"""Management actions preserve review, ownership and publication boundaries."""

import copy

import pytest
from agentscope.app.message_bus import InMemoryMessageBus
from test_domain import COMPANY, expert, identities
from test_scheduled_session_listing import scheduled_task

from egis_agentkit.errors import Conflict, Forbidden
from egis_agentkit.service.access._grant import GrantService
from egis_agentkit.service.access._permission import PermissionService
from egis_agentkit.service.access._share import ShareService
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.session._session import SessionService
from egis_agentkit.storage.model import Grant, ShareTarget
from egis_agentkit.storage.model.agent._update import AgentUpdate


async def shared_agent(storage):
    users, admin, alice, bob = await identities(storage)
    agents, sharing = AgentManager(storage), ShareService(storage)
    source = await agents.add_definition(alice, expert())
    review = await sharing.submit(
        alice, "agent", source["id"], target=ShareTarget(
            principal_type="user", principal_id=bob.user_id
        )
    )
    await sharing.decide(admin, review["review_id"], True, "")
    return users, admin, alice, bob, agents, sharing, source, review


@pytest.mark.asyncio
@pytest.mark.parametrize("already_shared", [False, True])
async def test_admin_cannot_bypass_personal_resource_audience_review(storage, already_shared):
    _, admin, alice, bob = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    if already_shared:
        sharing = ShareService(storage)
        review = await sharing.submit(alice, "agent", source["id"], target=COMPANY)
        await sharing.decide(admin, review["review_id"], True, "")
    with pytest.raises(Forbidden, match="共享.*审核"):
        await GrantService(storage).grant(admin, Grant(
            principal_type="user", principal_id=bob.user_id,
            resource_type="agent", resource_id=source["id"],
            resource_view_type="shared", resource_owner_type="user",
            resource_owner_id=alice.user_id,
        ))


@pytest.mark.asyncio
@pytest.mark.parametrize("approved", [False, True])
async def test_submitter_cannot_review_own_share(storage, approved):
    _, admin, _, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(admin, expert("admin"))
    sharing = ShareService(storage)
    review = await sharing.submit(admin, "agent", source["id"], target=COMPANY)
    with pytest.raises(Forbidden, match="自己.*申请"):
        await sharing.decide(admin, review["review_id"], approved, "")
    assert (await sharing.list(admin))[0]["review_status_type"] == "pending"


@pytest.mark.asyncio
async def test_shared_owner_can_still_select_another_recipient(storage):
    _, _, alice, _, _, _, source, _ = await shared_agent(storage)
    flags = await PermissionService(storage).share_availability(
        alice, "agent", source["id"], version="1", view="private"
    )
    assert flags["can_share"]


@pytest.mark.asyncio
async def test_new_recipient_uses_approved_edition_label_not_unreviewed_latest(storage):
    _, _, alice, _, agents, _, source, _ = await shared_agent(storage)
    payload = {**source["payload"], "description": "unreviewed"}
    changed = await agents.update_definition(alice, source["id"], AgentUpdate(
        agent_name=source["agent_name"], agent_display_name=source["agent_display_name"],
        payload=payload, owner_version=source["owner_version"],
    ))
    flags = await PermissionService(storage).share_availability(
        alice, "agent", source["id"], version=changed["resource_version"], view="private"
    )
    assert flags["share_version"] == "1"
    assert flags["share_version_label"] == "1.0"
    detail = await agents.get_definition(alice, source["id"], "private")
    assert detail["share_version"] == "1"
    assert detail["share_version_label"] == "1.0"


@pytest.mark.asyncio
async def test_owner_withdraws_one_audience_and_preserves_other_grants(storage):
    _, admin, alice, bob, _, sharing, source, review = await shared_agent(storage)
    extra = await sharing.submit(alice, "agent", source["id"], target=ShareTarget(
        principal_type="user", principal_id=admin.user_id
    ))
    await sharing.decide(admin, extra["review_id"], True, "")
    withdrawal = await sharing.withdraw(alice, review["review_id"])
    await sharing.withdraw(alice, review["review_id"])
    permissions = PermissionService(storage)
    assert (await permissions.explain(bob, "agent", source["id"], "shared")).is_allowed
    await sharing.decide(admin, withdrawal["review_id"], True, "approved withdrawal")
    assert not (await permissions.explain(bob, "agent", source["id"], "shared")).is_allowed
    assert (await permissions.explain(admin, "agent", source["id"], "shared")).is_allowed
    assert (await permissions.explain(alice, "agent", source["id"], "private")).is_allowed
    reviews = {row["review_id"]: row for row in await sharing.list(alice)}
    assert reviews[review["review_id"]]["review_status_type"] == "approved"
    assert not reviews[review["review_id"]]["is_current"]


@pytest.mark.asyncio
async def test_only_owner_can_cancel_pending_review(storage):
    _, _, alice, bob = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    sharing = ShareService(storage)
    review = await sharing.submit(alice, "agent", source["id"], target=COMPANY)
    with pytest.raises(Forbidden):
        await sharing.withdraw(bob, review["review_id"])
    await sharing.withdraw(alice, review["review_id"])
    assert (await sharing.list(alice))[0]["review_status_type"] == "cancelled"


@pytest.mark.asyncio
async def test_restored_content_does_not_offer_or_accept_shared_update(storage):
    _, _, alice, _, agents, sharing, source, _ = await shared_agent(storage)
    changed = copy.deepcopy(source["payload"])
    changed["description"] = "changed description"
    updated = await agents.update_definition(alice, source["id"], AgentUpdate(
        agent_name=source["agent_name"], agent_display_name=source["agent_display_name"],
        payload=changed, owner_version=source["owner_version"],
    ))
    restored = await agents.update_definition(alice, source["id"], AgentUpdate(
        agent_name=source["agent_name"], agent_display_name=source["agent_display_name"],
        payload=source["payload"], owner_version=updated["owner_version"],
    ))
    flags = await PermissionService(storage).share_availability(
        alice, "agent", source["id"], version=restored["resource_version"], view="private"
    )
    assert not flags["can_share_update"]
    with pytest.raises(Conflict, match="内容.*变化"):
        await sharing.submit(alice, "agent", source["id"])
    assert not (await agents.get_definition(alice, source["id"], "private"))["can_share_update"]


@pytest.mark.asyncio
async def test_grant_choices_exclude_unreviewed_and_shared_personal_resources(storage):
    _, admin, _, _, _, _, source, _ = await shared_agent(storage)
    options = await GrantService(storage).grantable_resources(admin)
    assert not any(row["resource_id"] == source["id"] for row in options)


@pytest.mark.asyncio
async def test_owner_can_delete_idle_scheduled_main_conversation(storage, tmp_path):
    _, _, alice, bob = await identities(storage)
    service = SessionService(storage, tmp_path)
    definition = await AgentManager(storage).add_definition(alice, expert())
    session = await scheduled_task(storage, service, alice, definition)
    async with InMemoryMessageBus() as bus:
        with pytest.raises(Forbidden):
            await service.delete_session(bob, session["session_id"], bus)
        await service.delete_session(alice, session["session_id"], bus)
    assert await storage.get_session(
        alice.user_id, session["agent_id"], session["session_id"]
    ) is None
