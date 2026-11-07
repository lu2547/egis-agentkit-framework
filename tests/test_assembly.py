import pytest
import pytest_asyncio
from agentscope.app.storage import AgentData, AgentRecord, SessionConfig
from agentscope.state import AgentState
from test_domain import expert, identities

from egis_agentkit.errors import Forbidden
from egis_agentkit.services import AgentService
from egis_agentkit.extensions.compatibility.execution import ExecutionCoordinator, execution_scope


@pytest_asyncio.fixture
async def runtime_storage(storage):
    from egis_agentkit.extensions.compatibility.storage import ExecutionStorage

    async with ExecutionStorage("postgresql+asyncpg://unused", storage.engine) as projected:
        yield projected


@pytest.mark.asyncio
async def test_context_precedes_agent_read_and_is_reset(storage, runtime_storage, tmp_path):
    _, _, alice, _ = await identities(storage)
    agents = AgentService(storage)
    source = await agents.save(alice, expert())
    from egis_agentkit.service.access.snapshot import SnapshotService

    snapshot = await SnapshotService(storage).capture(source["id"], "private")
    native = AgentRecord(
        user_id="alice", data=AgentData(name="writer", system_prompt="placeholder")
    )
    await storage.upsert_agent("alice", native)
    state = AgentState(
        middle_context={
            "egis_agentkit": {
                "is_initialized": True,
                "definition_id": source["id"],
                "resource_view_type": "private",
                "root_resource_type": "agent",
                "agent_node_key": snapshot["root_key"],
                "snapshot": snapshot,
                "is_team": False,
            }
        }
    )
    session = await storage.upsert_session(
        "alice", native.id, SessionConfig(workspace_id="workspace"), state=state
    )
    coordinator = ExecutionCoordinator(storage, tmp_path)
    async with coordinator.bind(session.id):
        assert execution_scope.get().session_id == session.id
        assert (
            await runtime_storage.get_agent("alice", native.id)
        ).data.system_prompt == "旧版"
        current = await runtime_storage.get_session("alice", native.id, session.id)
        assert (
            current.state.middle_context["egis_agentkit"]["snapshot"]["root"]["id"]
            == source["id"]
        )
        changed = expert()
        changed.id = source["id"]
        changed.payload["system_prompt"] = "新版"
        await agents.save(alice, changed)
        assert (
            await runtime_storage.get_agent("alice", native.id)
        ).data.system_prompt == "旧版"
    assert execution_scope.get() is None
    async with coordinator.bind(session.id):
        assert (
            await runtime_storage.get_agent("alice", native.id)
        ).data.system_prompt == "新版"
    assert execution_scope.get() is None


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
    token = execution_scope.set(SimpleNamespace(
        user_id="alice", agent_id=native.id, session_id=session.id,
        session_record=projected,
        definition={"agent_name": "running", "payload": {"system_prompt": "runtime"}},
    ))
    try:
        assert (await storage.get_agent("alice", native.id)).data.system_prompt == "persisted"
        assert (await storage.get_session("alice", native.id, session.id)).config.name != "in-flight"
    finally:
        execution_scope.reset(token)


@pytest.mark.asyncio
async def test_uninitialized_session_never_executes(storage, tmp_path):
    await identities(storage)
    native = AgentRecord(user_id="alice", data=AgentData(name="bad"))
    await storage.upsert_agent("alice", native)
    session = await storage.upsert_session(
        "alice", native.id, SessionConfig(workspace_id="workspace")
    )
    with pytest.raises(Forbidden):
        async with ExecutionCoordinator(storage, tmp_path).bind(session.id):
            pass
    assert execution_scope.get() is None
