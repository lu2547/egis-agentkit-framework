import asyncio
import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest
from agentscope.app.message_bus import InMemoryMessageBus, MessageBusKeys
from agentscope.app.middleware import InboxMiddleware
from agentscope.message import (
    AssistantMsg,
    HintBlock,
    TextBlock,
    ToolCallBlock,
    ToolResultBlock,
)
from agentscope.middleware import ReMeMiddleware
from config_helpers import sandbox_runtime
from runtime_helpers import session_middlewares

from egis_agentkit.client.nacos import AgentKitConfig
from egis_agentkit.client.nacos._settings import ProjectMemoryConfig
from egis_agentkit.core.session._context import (
    SessionExecutionContext,
    session_execution_context,
)
from egis_agentkit.errors import Forbidden
from egis_agentkit.middleware.agent._project_memory_middleware_override import (
    ProjectMemoryMiddlewareOverride,
)
from egis_agentkit.service.project._memory import ProjectMemoryService

original = importlib.util.find_spec


def memory_snapshot(tmp_path, **memory):
    return AgentKitConfig.model_validate(
        {
            "revision": 1,
            "workspace": {"runtime": sandbox_runtime(tmp_path)},
            "project": {"upload": {"max_bytes": 20_971_520}, "memory": memory},
            "user": {
                "identity": {
                    "issuer": "test",
                    "audience": "test",
                    "jwks_url": "https://identity.example.test/.well-known/jwks.json",
                },
            },
            "model": {
                "catalog": {
                    "models": [
                        {
                            "key": "model",
                            "display_name": "Offline",
                            "model": "offline-test",
                            "api_key_env": "MEMORY_TEST_MODEL_KEY",
                        }
                    ]
                },
                "selection": {"default_key": "model"},
            },
        }
    )


def test_memory_defaults_to_disabled():
    assert ProjectMemoryConfig().is_enabled is False


def test_enabled_memory_rejects_missing_dependency(monkeypatch, tmp_path):
    monkeypatch.setattr(
        "importlib.util.find_spec",
        lambda name: None if name == "reme" else original(name),
    )
    config = SimpleNamespace(snapshot=memory_snapshot(tmp_path, is_enabled=True))
    with pytest.raises(RuntimeError, match="reme-ai"):
        ProjectMemoryService(None, tmp_path, config)


@pytest.mark.asyncio
async def test_native_middleware_is_cached_per_user_and_project_and_closed(
    monkeypatch, tmp_path
):
    from egis_agentkit.service.project import _memory as memory
    from egis_agentkit.service.project._memory import ProjectMemoryService

    monkeypatch.setattr(memory.importlib.util, "find_spec", lambda name: object())
    calls = []

    async def resolve(self, user, model_key=None, **kwargs):
        calls.append(user.user_id)
        return "config"

    async def get_model(user_id, config, access):
        assert config == "config"
        return None

    monkeypatch.setattr(memory.ModelService, "resolve", resolve)
    monkeypatch.setattr(memory, "get_model", get_model)
    config = SimpleNamespace(
        snapshot=memory_snapshot(tmp_path, is_enabled=True, top_k=3)
    )
    service = ProjectMemoryService(None, tmp_path, config)
    alice = SimpleNamespace(user_id="alice")
    bob = SimpleNamespace(user_id="bob")
    one, same = await asyncio.gather(
        service.for_project(alice, "project-one"),
        service.for_project(alice, "project-one"),
    )
    another_project = await service.for_project(alice, "project-two")
    another_user = await service.for_project(bob, "project-one")
    assert isinstance(one, ReMeMiddleware) and one is same
    assert len({id(one), id(another_project), id(another_user)}) == 3
    assert one._parameters.top_k == 3
    assert (
        Path(one._workspace_dir) == tmp_path / "user/alice/projects/project-one/.memory"
    )
    assert Path(another_project._workspace_dir) == (
        tmp_path / "user/alice/projects/project-two/.memory"
    )
    assert Path(another_user._workspace_dir) == (
        tmp_path / "user/bob/projects/project-one/.memory"
    )
    assert not Path(one._workspace_dir).exists()
    assert calls == ["alice", "alice", "bob"]
    closed = []

    async def close():
        closed.append(True)

    for middleware in (one, another_project, another_user):
        monkeypatch.setattr(middleware, "close", close)
    await service.close()
    assert len(closed) == 3


@pytest.mark.asyncio
async def test_factory_shares_project_memory_across_local_agents_only(tmp_path):

    class Users:
        async def get(self, user_id):
            return SimpleNamespace(user_id=user_id)

    class Memory:
        async def for_project(self, user, project_id):
            calls.append((user.user_id, project_id))
            return marker

    class RemoteAgents:
        def get_client_for_definition(self, definition, *, configuration):
            return object()

    factory = session_middlewares(
        None,
        users=Users(),
        file_changes=object(),
        memory=Memory(),
        remote_agents=RemoteAgents(),
    )
    marker = object()
    calls = []
    for kind, project, expected in [
        ("personal", "project-one", True),
        ("expert", "project-one", True),
        ("orchestrator", "project-two", True),
        ("remote", "project-two", False),
    ]:
        scope = SessionExecutionContext(
            user_id="alice",
            agent_id="a",
            session_id="s",
            definition={"agent_type": kind, "payload": {}},
            root_definition={},
            binding={"is_team": False},
            session_record=SimpleNamespace(team_id=None),
            workdir=tmp_path / ".work",
            artifacts_dir=tmp_path / "artifacts",
            project_id=project,
        )
        token = session_execution_context.set(scope)
        try:
            result = await factory("alice", "a", "s", None)
            assert (marker in result) is expected
        finally:
            session_execution_context.reset(token)
    assert calls == [
        ("alice", "project-one"),
        ("alice", "project-one"),
        ("alice", "project-two"),
    ]


@pytest.mark.asyncio
async def test_collaboration_wakeup_searches_before_reasoning_and_saves_worker_result(
    monkeypatch,
    tmp_path,
):
    middleware = ProjectMemoryMiddlewareOverride(workspace_dir=str(tmp_path))
    state = SimpleNamespace(
        session_id="worker-session",
        reply_id="current-reply",
        context=[AssistantMsg(name="expert", content=[TextBlock(text="上次的回复")])],
    )
    agent = SimpleNamespace(name="expert", state=state)
    searches, writes = [], []

    async def search(query):
        searches.append(query)
        return ["历史报表格式"]

    async def write_back(messages, session_id):
        writes.append((messages, session_id))

    monkeypatch.setattr(middleware, "_search", search)
    monkeypatch.setattr(middleware, "_write_back", write_back)

    async def reason(**kwargs):
        assert searches == ["整理报表"]
        assert "历史报表格式" in str(state.context[-1].content)
        state.context.append(
            AssistantMsg(
                name="expert",
                content=[
                    ToolCallBlock(
                        id="report", name="TeamSay", input='{"content":"报表已整理"}'
                    ),
                    ToolResultBlock(
                        id="report", name="TeamSay", output="已发送", state="success"
                    ),
                ],
            )
        )
        yield "done"

    async with InMemoryMessageBus() as bus:
        inbox = InboxMiddleware(bus)
        for hint in [
            HintBlock(
                source='{"label":"team_message"}',
                hint='<team-message from="leader">\n整理报表\n</team-message>',
            ),
            HintBlock(source="tool", hint="工具已执行"),
        ]:
            await bus.queue_push(
                MessageBusKeys.inbox(state.session_id), hint.model_dump(mode="json")
            )

        async def run(**kwargs):
            async for event in inbox.on_reasoning(agent, {}, reason):
                yield event

        events = [
            item async for item in middleware.on_reply(agent, {"inputs": None}, run)
        ]

    assert len(events) == 3
    ((messages, session_id),) = writes
    assert session_id == "worker-session"
    assert [message.get_text_content() for message in messages] == [
        "整理报表",
        "报表已整理",
    ]


@pytest.mark.asyncio
async def test_concurrent_project_memory_jobs_start_reme_once(monkeypatch, tmp_path):
    middleware = ProjectMemoryMiddlewareOverride(workspace_dir=str(tmp_path))
    start_calls = []

    async def start():
        start_calls.append(None)
        await asyncio.sleep(0)

    monkeypatch.setattr(middleware, "_build_app", lambda: SimpleNamespace(start=start))

    await asyncio.gather(middleware._ensure_started(), middleware._ensure_started())

    assert len(start_calls) == 1


@pytest.mark.asyncio
async def test_enabled_service_uses_native_model_and_starts_reme(
    storage, tmp_path, monkeypatch
):
    pytest.importorskip("reme")
    monkeypatch.setenv("MEMORY_TEST_MODEL_KEY", "unused-offline-key")
    snapshot = memory_snapshot(tmp_path, is_enabled=True)
    service = ProjectMemoryService(
        storage, tmp_path, SimpleNamespace(snapshot=snapshot)
    )
    try:
        service.files.initialize_project("alice", "project-one")
        middleware = await service.for_project(
            SimpleNamespace(user_id="alice"), "project-one"
        )
        assert middleware._parameters.chat_model.model == "offline-test"
        directory = tmp_path / "user/alice/projects/project-one/.memory"
        assert not directory.exists()
        await middleware._ensure_started()
        assert middleware._started
        assert directory.is_dir()
        assert service.files.list("alice", "project-one", "uploads") == []
        with pytest.raises(Forbidden):
            service.files.read("alice", "project-one", "uploads", ".memory/example.md")
    finally:
        await service.close()
