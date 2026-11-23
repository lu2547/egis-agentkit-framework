import pytest
import pytest_asyncio
from agentscope.app.storage import AgentData, AgentRecord, SessionConfig
from agentscope.message import ToolCallBlock, ToolCallState
from test_domain import agent_changes, expert, identities

from egis_agentkit.core.session._context import session_execution_context
from egis_agentkit.errors import Forbidden
from egis_agentkit.service.agent._agent_manager import AgentManager
from egis_agentkit.service.session._preparation import SessionPreparationService
from egis_agentkit.service.session._session import SessionService


@pytest_asyncio.fixture
async def runtime_storage(storage):
    from egis_agentkit.storage._session_execution import (
        SessionExecutionStorage,
    )

    async with SessionExecutionStorage("postgresql+asyncpg://unused", storage.engine, files=storage.message_history().files) as projected:
        yield projected


@pytest.mark.asyncio
async def test_session_resolves_pinned_definition_on_every_turn(storage, runtime_storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    agents = AgentManager(storage)
    source = await agents.add_definition(alice, expert())
    tasks = SessionService(storage, tmp_path)
    task = await tasks.add_session(alice, source["id"], "private")
    session_id, agent_id = task["session_id"], task["agent_id"]
    native = await storage.get_agent("alice", agent_id)
    assert native.data.system_prompt == "旧版"
    native.data.system_prompt = "placeholder"
    await storage.upsert_agent("alice", native)
    coordinator = SessionPreparationService(storage, tmp_path)
    async with coordinator.bind(session_id):
        assert session_execution_context.get().session_id == session_id
        assert (
            await runtime_storage.get_agent("alice", agent_id)
        ).data.system_prompt == "旧版"
        current = await runtime_storage.get_session("alice", agent_id, session_id)
        binding = current.state.middle_context["egis_agentkit"]
        assert "snapshot" not in binding
        assert binding["definition_id"] == source["id"]
        assert binding["resource_version"] == "1"
        changed = expert()
        changed.id = source["id"]
        changed.payload["system_prompt"] = "新版"
        await agents.update_definition(alice, (changed).id, agent_changes(changed))
        assert (
            await runtime_storage.get_agent("alice", agent_id)
        ).data.system_prompt == "旧版"
        current.state.append_context(
            source["agent_name"],
            [ToolCallBlock(id="approval", name="Read", input="{}", state=ToolCallState.ASKING)],
        )
        await storage.update_session_state("alice", agent_id, session_id, current.state)
    assert session_execution_context.get() is None
    assert (await tasks.get_session(alice, session_id))["is_waiting"]
    async with coordinator.bind(session_id) as scope:
        assert (
            await runtime_storage.get_agent("alice", agent_id)
        ).data.system_prompt == "旧版"
        scope.session_record.state.context.clear()
        await storage.update_session_state(
            "alice", agent_id, session_id, scope.session_record.state
        )
    async with coordinator.bind(session_id):
        assert (
            await runtime_storage.get_agent("alice", agent_id)
        ).data.system_prompt == "旧版"
    assert session_execution_context.get() is None
    latest = await tasks.add_session(alice, source["id"], "private")
    async with coordinator.bind(latest["session_id"]):
        assert (
            await runtime_storage.get_agent("alice", latest["agent_id"])
        ).data.system_prompt == "新版"
    pinned = await tasks.add_session(alice, source["id"], "private", resource_version="1")
    async with coordinator.bind(pinned["session_id"]):
        assert (
            await runtime_storage.get_agent("alice", pinned["agent_id"])
        ).data.system_prompt == "旧版"


@pytest.mark.asyncio
async def test_persistence_reads_are_not_replaced_by_execution_scope(storage, tmp_path):
    from types import SimpleNamespace

    await identities(storage)
    native = AgentRecord(
        user_id="alice", data=AgentData(name="stored", system_prompt="persisted")
    )
    await storage.upsert_agent("alice", native)
    session = await storage.upsert_session("alice", native.id, SessionConfig(workspace_id="workspace"))
    projected = session.model_copy(deep=True)
    projected.config.name = "in-flight"
    token = session_execution_context.set(SimpleNamespace(
        user_id="alice", agent_id=native.id, session_id=session.id,
        session_record=projected,
        definition={"agent_name": "running", "payload": {"system_prompt": "runtime"}},
    ))
    try:
        assert (await storage.get_agent("alice", native.id)).data.system_prompt == "persisted"
        assert (await storage.get_session("alice", native.id, session.id)).config.name != "in-flight"
    finally:
        session_execution_context.reset(token)


@pytest.mark.asyncio
async def test_uninitialized_session_never_executes(storage, tmp_path):
    await identities(storage)
    native = AgentRecord(user_id="alice", data=AgentData(name="bad"))
    await storage.upsert_agent("alice", native)
    session = await storage.upsert_session(
        "alice", native.id, SessionConfig(workspace_id="workspace")
    )
    with pytest.raises(Forbidden):
        async with SessionPreparationService(storage, tmp_path).bind(session.id):
            pass
    assert session_execution_context.get() is None
