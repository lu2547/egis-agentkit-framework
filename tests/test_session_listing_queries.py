"""会话列表按批读取，在查询次数固定的同时保留权限和展示契约。"""

import asyncio
import json
import statistics
import time
from contextlib import contextmanager
from contextvars import ContextVar

import pytest
from sqlalchemy import event
from test_domain import expert, identities

from egis_agentkit.core.message_bus import EgisMessageBus
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.session._session import SessionService

_query_scope = ContextVar("task_list_test_queries", default=None)


@pytest.mark.parametrize("session_count", [8, 200])
async def test_task_http_query_budget_and_latency(
    storage, tmp_path, monkeypatch, session_count
):
    """包含 JWT 中间件、真实 PostgreSQL、DTO 和 JSON 响应的完整请求。"""
    import httpx
    from agentscope.app.storage._sql._mappers import _from_record
    from agentscope.app.storage._sql._tables import SessionRow
    from egis_agentkit_app.bootstrap import build_app
    from identity_helpers import configure_test_issuer, signed_token
    from sqlalchemy.ext.asyncio import AsyncSession
    from test_app_http import Config, app_snapshot

    from egis_agentkit.client.nacos import NacosBootstrap, NacosClient
    from egis_agentkit.storage.model import new_id

    monkeypatch.setenv("TEST_DOWNLOAD", "test-download-secret-with-thirty-two-bytes")
    monkeypatch.setenv("TEST_MODEL_KEY", "test-only-unused-model-key")
    configure_test_issuer(monkeypatch)
    _, _, alice, _ = await identities(storage)
    async with NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos:
        app = await build_app(
            Config(app_snapshot(tmp_path)), nacos, engine=storage.engine
        )
        async with app.router.lifespan_context(app):
            service = app.state.egis_services.sessions
            definition = await app.state.egis_services.agents.add_definition(
                alice, expert()
            )
            task = await service.add_session(alice, definition["id"], "private")
            template = await service.get_session_record(alice, task["session_id"])
            # Seed valid independent sessions in bulk. Their shared definition and
            # project match the reported sidebar case; setup is outside timing.
            async with AsyncSession(storage.engine) as session:
                session.add_all(
                    [
                        _from_record(
                            SessionRow,
                            template.model_copy(update={"id": new_id()}, deep=True),
                        )
                        for _ in range(session_count - 1)
                    ]
                )
                await session.commit()
            samples, counts = [], []
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                headers={"Authorization": "Bearer " + signed_token("alice")},
            ) as client:
                for index in range(21):
                    with select_queries(storage) as queries:
                        started = time.perf_counter()
                        response = await client.get("/manager/tasks")
                        elapsed = (time.perf_counter() - started) * 1000
                    assert response.status_code == 200, response.text
                    assert len(response.json()["items"]) == session_count
                    assert len(queries) <= 6  # 1 identity read + 5 batched list reads.
                    if index:
                        samples.append(elapsed)
                        counts.append(len(queries))
            p95 = sorted(samples)[18]
            print(
                "SQL_BENCHMARK "
                + json.dumps(
                    {
                        "sessions": session_count,
                        "requests": len(samples),
                        "selects": max(counts),
                        "p50_ms": round(statistics.median(samples), 2),
                        "p95_ms": round(p95, 2),
                        "max_ms": round(max(samples), 2),
                    }
                )
            )
            assert p95 < 500, f"Local full HTTP request p95 was {p95:.1f} ms"


@contextmanager
def select_queries(storage):
    queries = []
    scope = object()
    token = _query_scope.set(scope)

    def capture(_conn, _cursor, statement, _params, _context, _many):
        if _query_scope.get() is not scope:
            return
        compiled = getattr(_context, "compiled", None)
        if getattr(
            getattr(compiled, "statement", None), "is_select", False
        ) or statement.lstrip().upper().startswith("SELECT"):
            queries.append(statement)

    event.listen(storage.engine.sync_engine, "before_cursor_execute", capture)
    try:
        yield queries
    finally:
        event.remove(storage.engine.sync_engine, "before_cursor_execute", capture)
        _query_scope.reset(token)


async def test_query_counter_excludes_background_scheduler_queries(storage):
    """预算只属于被测请求，不能把早已启动的调度循环算进去。"""
    from sqlalchemy import text

    begin, finished = asyncio.Event(), asyncio.Event()

    async def background():
        await begin.wait()
        async with storage.engine.connect() as conn:
            await conn.execute(text("SELECT 1"))
        finished.set()

    task = asyncio.create_task(background())
    try:
        with select_queries(storage) as queries:
            begin.set()
            await finished.wait()
            async with storage.engine.connect() as conn:
                await conn.execute(text("SELECT 2"))
        assert len(queries) == 1
        assert queries[0].strip() == "SELECT 2"
    finally:
        await task


@pytest.mark.parametrize("distinct_definitions", [False, True])
async def test_eight_sessions_use_bounded_selects_and_match_detail(
    storage, tmp_path, distinct_definitions
):
    _, _, alice, bob = await identities(storage)
    agents, service = AgentManager(storage), SessionService(storage, tmp_path)
    source = await agents.add_definition(alice, expert())
    sessions = []
    for index in range(8):
        if distinct_definitions:
            source = await agents.add_definition(alice, expert(name=f"writer_{index}"))
        sessions.append(await service.add_session(alice, source["id"], "private"))
    foreign = await agents.add_definition(bob, expert(owner="bob", name="bob_writer"))
    await service.add_session(bob, foreign["id"], "private")
    expected = [
        await service.get_session(alice, item["session_id"])
        for item in reversed(sessions)
    ]
    async with EgisMessageBus() as bus:
        from agentscope.app.message_bus import MessageBusKeys

        await bus.try_lock(MessageBusKeys.session_lock(sessions[0]["session_id"]))
        with select_queries(storage) as queries:
            result = await service.list_sessions(alice, bus)
        assert [
            {key: value for key, value in item.items() if key != "is_running"}
            for item in result
        ] == expected
        assert sum(item["is_running"] for item in result) == 1
        assert len(queries) <= 5, f"8 sessions executed {len(queries)} SELECTs"


async def test_listing_rechecks_group_membership_group_state_and_revoked_grants(
    storage, tmp_path
):
    from egis_agentkit.service.access._grant import GrantService
    from egis_agentkit.storage.model import Grant, Group

    users, admin, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(
        admin, expert(owner="platform", owner_type="system")
    )
    grants = GrantService(storage)
    grant = await grants.grant(
        admin,
        Grant(
            principal_type="group",
            principal_id="company",
            resource_type="agent",
            resource_id=source["id"],
            resource_view_type="system",
            resource_owner_type="system",
            resource_owner_id="platform",
        ),
    )
    service = SessionService(storage, tmp_path)
    for _ in range(8):
        await service.add_session(alice, source["id"], "system")
    with select_queries(storage) as queries:
        assert len(await service.list_sessions(alice)) == 8
    assert len(queries) <= 5
    await users.save_group(
        admin, Group(group_id="company", group_name="全公司", is_enabled=False)
    )
    assert await service.list_sessions(alice) == []
    await users.save_group(admin, Group(group_id="company", group_name="全公司"))
    assert len(await service.list_sessions(alice)) == 8
    await users.update_user(admin, alice.model_copy(update={"group_ids": []}))
    assert (
        await service.list_sessions(alice) == []
    )  # 入参仍带旧组，必须使用本次数据库身份。
    await users.update_user(admin, alice)
    assert len(await service.list_sessions(alice)) == 8
    await grants.revoke(admin, grant["id"])
    assert await service.list_sessions(alice) == []
    await users.update_user(admin, alice.model_copy(update={"is_enabled": False}))
    assert await service.list_sessions(alice) == []


async def test_listing_matches_pinned_versions_and_static_team_details(
    storage, tmp_path
):
    from test_domain import agent_changes

    from egis_agentkit.service.team._definition import TeamDefinition

    _, _, alice, _ = await identities(storage)
    agents, service = AgentManager(storage), SessionService(storage, tmp_path)
    source = await agents.add_definition(alice, expert())
    first = await service.add_session(alice, source["id"], "private")
    changed = expert()
    changed.id, changed.owner_version = source["id"], source["owner_version"]
    changed.agent_display_name = "新版专家"
    await agents.update_definition(alice, source["id"], agent_changes(changed))
    second = await service.add_session(alice, source["id"], "private")
    team = await TeamDefinition(storage).save(
        alice,
        {
            "team_name": "writers",
            "team_display_name": "写作团",
            "orchestrator_config": {
                "agent_name": "coordinator",
                "agent_display_name": "主理人",
                "payload": {},
            },
            "members": [
                {"definition_id": source["id"], "resource_view_type": "private"}
            ],
        },
    )
    third = await service.add_session(alice, team["id"], "private")
    expected = [
        await service.get_session(alice, task["session_id"])
        for task in (third, second, first)
    ]
    with select_queries(storage) as queries:
        assert await service.list_sessions(alice) == expected
    assert len(queries) <= 7
    assert [item["resource_version"] for item in expected[1:]] == ["2", "1"]


async def test_listing_keeps_shared_pinned_history_but_filters_deleted_definition(
    storage, tmp_path
):
    from test_domain import COMPANY, agent_changes

    from egis_agentkit.service.access._share import ShareService

    _, admin, alice, bob = await identities(storage)
    agents, shares = AgentManager(storage), ShareService(storage)
    source = await agents.add_definition(alice, expert())
    review = await shares.submit(alice, "agent", source["id"], target=COMPANY)
    await shares.decide(admin, review["review_id"], True, "通过")
    service = SessionService(storage, tmp_path)
    task = await service.add_session(bob, source["id"], "shared")
    changed = expert()
    changed.id, changed.owner_version = source["id"], source["owner_version"]
    changed.agent_display_name = "新版专家"
    await agents.update_definition(alice, source["id"], agent_changes(changed))
    review = await shares.submit(alice, "agent", source["id"])
    await shares.decide(admin, review["review_id"], True, "发布新版")
    expected = await service.get_session(bob, task["session_id"])
    assert expected["resource_version"] == "1"
    assert await service.list_sessions(bob) == [expected]
    await agents.delete_definition(alice, source["id"])
    assert await service.list_sessions(bob) == []


async def test_listing_excludes_session_bound_to_another_users_project(
    storage, tmp_path
):
    _, _, alice, bob = await identities(storage)
    agents, service = AgentManager(storage), SessionService(storage, tmp_path)
    source = await agents.add_definition(alice, expert())
    safe = await service.add_session(alice, source["id"], "private")
    forged = await service.add_session(alice, source["id"], "private")
    foreign = await agents.add_definition(bob, expert(owner="bob", name="foreign"))
    other = await service.add_session(bob, foreign["id"], "private")
    record = await storage.get_session(
        alice.user_id, forged["agent_id"], forged["session_id"]
    )
    record.state.middle_context["egis_agentkit"]["project_id"] = other["project_id"]
    await storage.update_session_state(
        alice.user_id, forged["agent_id"], forged["session_id"], record.state
    )
    assert [item["session_id"] for item in await service.list_sessions(alice)] == [
        safe["session_id"]
    ]


async def test_cluster_run_states_are_read_in_one_additional_query(storage, tmp_path):
    from agentscope.app.message_bus import MessageBusKeys

    _, _, alice, _ = await identities(storage)
    source = await AgentManager(storage).add_definition(alice, expert())
    service = SessionService(storage, tmp_path)
    tasks = [
        await service.add_session(alice, source["id"], "private") for _ in range(8)
    ]
    bus = EgisMessageBus(deployment_mode="cluster", engine=storage.engine)
    await bus.init_schema()
    try:
        await bus.try_lock(MessageBusKeys.session_lock(tasks[0]["session_id"]))
        with select_queries(storage) as queries:
            result = await service.list_sessions(alice, bus)
        assert {item["session_id"] for item in result if item["is_running"]} == {
            tasks[0]["session_id"]
        }
        assert len(queries) <= 6, f"cluster list executed {len(queries)} SELECTs"
    finally:
        await bus.aclose()
