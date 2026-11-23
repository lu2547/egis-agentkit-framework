"""Identity synchronization reads current access without writing on every GET."""

import asyncio
from contextlib import contextmanager

import pytest
from sqlalchemy import event, func, select
from test_domain import identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.service._user import UserService
from egis_agentkit.storage import tables as t


@contextmanager
def statements(storage):
    result = []

    def capture(_conn, _cursor, statement, _params, _context, _many):
        result.append(statement)

    event.listen(storage.engine.sync_engine, "before_cursor_execute", capture)
    try:
        yield result
    finally:
        event.remove(storage.engine.sync_engine, "before_cursor_execute", capture)


async def test_unchanged_login_is_one_read_and_observes_updated_roles(storage):
    users, admin, alice, _ = await identities(storage)
    before = await storage.get_user(alice.user_id)
    with statements(storage) as queries:
        actual = await users.sync_identity(alice.user_id, alice.username)
    assert actual == alice
    assert len(queries) == 1
    assert queries[0].lstrip().startswith("SELECT")
    assert (await storage.get_user(alice.user_id))["updated_at"] == before["updated_at"]
    changed = alice.model_copy(update={"role_type": "admin", "group_ids": []})
    await users.update_user(admin, changed)
    with statements(storage) as queries:
        refreshed = await users.sync_identity(alice.user_id, alice.username)
    assert refreshed.role_type == "admin"
    assert refreshed.group_ids == []
    assert len(queries) == 1


async def test_renamed_login_returns_current_permissions_without_refetch(storage):
    users, admin, _, _ = await identities(storage)
    with statements(storage) as queries:
        renamed = await users.sync_identity(admin.user_id, "Renamed admin")
    assert renamed.username == "Renamed admin"
    assert renamed.role_type == "admin"
    assert renamed.group_ids == admin.group_ids
    assert len(queries) == 2
    assert "RETURNING" in queries[-1]


async def test_first_concurrent_login_creates_one_company_member(storage):
    users = UserService(storage)
    with statements(storage) as queries:
        first = await users.sync_identity("first", "First")
    assert first.group_ids == [users.company_group().group_id]
    assert first.role_type == "user"
    assert len(queries) <= 3
    results = await asyncio.gather(
        *[users.sync_identity("concurrent", "Concurrent") for _ in range(12)]
    )
    assert all(user == results[0] for user in results)
    async with storage.engine.connect() as conn:
        assert await conn.scalar(select(func.count()).select_from(t.users)) == 2
        assert await conn.scalar(select(func.count()).select_from(t.groups)) == 1


async def test_disabled_login_is_rejected_without_identity_write(storage):
    users, admin, alice, _ = await identities(storage)
    await users.update_user(admin, alice.model_copy(update={"is_enabled": False}))
    with statements(storage) as queries, pytest.raises(Forbidden):
        await users.sync_identity(alice.user_id, "Different name")
    assert len(queries) == 1
    assert queries[0].lstrip().startswith("SELECT")
