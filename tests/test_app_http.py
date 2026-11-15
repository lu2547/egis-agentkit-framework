import asyncio
from contextlib import asynccontextmanager
from functools import partial

import httpx
import pytest
from config_helpers import sandbox_runtime
from egis_agentkit_app.bootstrap import build_app
from test_domain import identities

from egis_agentkit.client.nacos import AgentKitConfig, NacosBootstrap, NacosClient
from egis_agentkit.errors import Forbidden
from egis_agentkit.extension.runtime._agent_tools_callback import BoundToolFactory
from egis_agentkit.extension.runtime._session_storage_override import ExecutionStorage


class Config:
    def __init__(self, snapshot):
        self.snapshot, self.is_connected, self.restart_required = snapshot, True, False
        self.last_error = None

    async def start(self):
        pass

    async def close(self):
        pass


def app_snapshot(tmp_path, **domains):
    return AgentKitConfig.model_validate(
        {
            "revision": 1,
            "client": {"postgres": {"connection": {"url_env": "TEST_DB_URL"}}},
            "workspace": {"runtime": sandbox_runtime(tmp_path)},
            "project": {
                "upload": {"max_bytes": 20_971_520},
                "download": {"secret_env": "TEST_DOWNLOAD"},
            },
            "user": {
                "identity": {
                    "mode": "demo",
                    "issuer": "test",
                    "audience": "test",
                    "algorithm": "HS256",
                    "signing_secret_env": "TEST_JWT",
                    "demo_password_env": "TEST_LOGIN",
                }
            },
            "model": {
                "selection": {"default_key": "test"},
                "catalog": {
                    "models": [
                        {
                            "key": "test",
                            "display_name": "测试模型",
                            "model": "test",
                            "api_key_env": "TEST_MODEL_KEY",
                            "model_class": "openai_chat",
                        }
                    ]
                },
            },
            **domains,
        }
    )


@pytest.mark.asyncio
async def test_http_identity_native_write_seal_and_real_task_files(
    storage, tmp_path, monkeypatch
):
    _, admin, alice, bob = await identities(storage)
    monkeypatch.setenv("TEST_DB_URL", "postgresql+asyncpg://unused")
    monkeypatch.setenv(
        "TEST_DOWNLOAD", "download-secret-with-more-than-thirty-two-bytes"
    )
    monkeypatch.setenv("TEST_JWT", "jwt-secret-with-more-than-thirty-two-bytes")
    monkeypatch.setenv("TEST_LOGIN", "a-test-password-with-enough-entropy")
    monkeypatch.setenv("TEST_MODEL_KEY", "test-only-unused-model-key")
    config = Config(app_snapshot(tmp_path))
    nacos = NacosClient(
        NacosBootstrap(server_url="http://unused.test"),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(503))
        ),
    )
    app = await build_app(config, nacos, engine=storage.engine)
    services = app.state.egis_services
    assert services.sessions.projects is services.projects
    assert app.state.egis_bus.coordinator.projects is services.projects
    assert isinstance(app.state.storage, ExecutionStorage)
    assert app.state.storage is not services.sessions.storage
    assert (
        app.state.storage.engine is services.sessions.storage.engine is storage.engine
    )
    assert app.state.egis_bus.coordinator.storage is services.sessions.storage
    assert services.resources.tool_registry is not None
    assert isinstance(app.state.extra_agent_tools, BoundToolFactory)
    factory = app.state.extra_agent_middlewares
    assert isinstance(factory, partial)
    assert factory.func.__self__ is services.sessions
    assert factory.keywords["users"] is services.users
    assert app.state.egis_bus.coordinator.personal is services.personal
    assert services.sessions.personal is services.personal
    assert factory.keywords["file_changes"] is services.project_files
    async with app.router.lifespan_context(app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app, client=("127.0.0.1", 1234)),
            base_url="http://test",
        ) as client:
            assert (
                await client.get("/manager/me", headers={"X-User-ID": "admin"})
            ).status_code == 401
            assert (
                await client.get(
                    "/manager/me", headers={"Authorization": "Bearer invalid-token"}
                )
            ).status_code == 401
            login = await client.post(
                "/auth/login",
                json={
                    "user_id": "alice",
                    "password": "a-test-password-with-enough-entropy",
                },
            )
            assert login.status_code == 200, login.text
            client.headers["Authorization"] = "Bearer " + login.json()["access_token"]
            preferences = {"agent_md": "# 自定义规范", "soul_md": "", "user_md": "背景"}
            saved = await client.put("/manager/preferences", json=preferences)
            assert saved.status_code == 200, saved.text
            assert saved.json() == preferences
            assert (await client.get("/manager/preferences")).json() == preferences
            created_agent = await client.post(
                "/manager/agents",
                json={
                    "agent_name": "edit_contract",
                    "agent_display_name": "编辑契约",
                    "payload": {"system_prompt": "初始提示"},
                },
            )
            assert created_agent.status_code == 201, created_agent.text
            agent_id = created_agent.json()["id"]
            changes = {
                "agent_name": "edit_contract",
                "agent_display_name": "更新名称",
                "owner_version": 1,
                "payload": {"system_prompt": "更新提示"},
            }
            for immutable in ({"agent_type": "remote"}, {"owner_id": "bob"}):
                assert (
                    await client.put(
                        f"/manager/agents/{agent_id}", json={**changes, **immutable}
                    )
                ).status_code == 422
            updated_agent = await client.put(
                f"/manager/agents/{agent_id}", json=changes
            )
            assert updated_agent.status_code == 200, updated_agent.text
            assert updated_agent.json()["agent_type"] == "expert"
            assert updated_agent.json()["owner_id"] == "alice"
            assert updated_agent.json()["owner_version"] == 2
            oversized = {**preferences, "user_md": "中" * 64_000}
            assert (
                await client.put("/manager/preferences", json=oversized)
            ).status_code == 409
            assert (await client.get("/manager/preferences")).json() == preferences
            assert (await client.get("/health/ready")).status_code == 200
            original_health = app.state.egis_bus.health
            monkeypatch.setattr(
                app.state.egis_bus, "health", lambda: {"is_ready": False}
            )
            assert (await client.get("/health/ready")).status_code == 503
            monkeypatch.setattr(app.state.egis_bus, "health", original_health)
            assert (await client.get("/manager/me")).json()["role_type"] == "user"
            assert (await client.get("/manager/admin/users")).status_code == 403
            assert (
                await client.post("/agent/", json={"name": "bypass"})
            ).status_code == 403
            # A session setup failure can occur after HTTP accepted the turn.
            # The native setup handler must still publish a terminal reply.
            admin_login = await client.post(
                "/auth/login",
                json={
                    "user_id": "admin",
                    "password": "a-test-password-with-enough-entropy",
                },
            )
            client.headers["Authorization"] = (
                "Bearer " + admin_login.json()["access_token"]
            )
            controlled = await client.post(
                "/manager/agents",
                json={
                    "agent_name": "controlled",
                    "agent_display_name": "受控专家",
                    "payload": {},
                },
            )
            assert controlled.status_code == 201
            identifier = controlled.json()["id"]
            catalog = (await client.get("/manager/agents")).json()
            assert set(catalog) == {"items", "next_cursor"}
            assert [item["id"] for item in catalog["items"]] == [identifier]
            page = await client.get(
                "/manager/agents", params={"type": "expert", "limit": 1}
            )
            assert page.status_code == 200
            assert [item["id"] for item in page.json()["items"]] == [identifier]
            assert page.json()["next_cursor"] is None
            personal = (await client.get("/manager/assistant")).json()
            assert personal["agent_type"] == "personal"
            assert (
                await client.get(f"/manager/agents/{personal['id']}")
            ).status_code == 404
            pending = await client.post(
                "/manager/tasks",
                json={
                    "definition_id": controlled.json()["id"],
                    "resource_view_type": "private",
                },
            )
            assert pending.status_code == 201
            failed_task = pending.json()
            coordinator = app.state.egis_bus.coordinator
            bind = coordinator.bind
            failing_sessions = {failed_task["session_id"]}

            @asynccontextmanager
            async def bind_for_test(session_id):
                if session_id in failing_sessions:
                    raise Forbidden("会话初始化失败")
                async with bind(session_id) as scope:
                    yield scope

            monkeypatch.setattr(coordinator, "bind", bind_for_test)
            triggered = await client.post(
                "/chat/",
                json={
                    "session_id": failed_task["session_id"],
                    "agent_id": failed_task["agent_id"],
                    "input": {
                        "name": "user",
                        "role": "user",
                        "content": [{"type": "text", "text": "hello"}],
                    },
                },
            )
            assert triggered.status_code == 200
            from agentscope.app.message_bus import MessageBusKeys

            terminal = None
            async with asyncio.timeout(2):
                while terminal is None:
                    entries = await app.state.egis_bus.log_read(
                        MessageBusKeys.session_events(failed_task["session_id"])
                    )
                    terminal = next(
                        (
                            event
                            for _, event in entries
                            if event.get("type", "").upper() == "REPLY_END"
                        ),
                        None,
                    )
                    await asyncio.sleep(0.01)
            assert terminal["finished_reason"].upper() == "ERROR"
            team_response = await client.post(
                "/manager/teams",
                json={
                    "team_name": "controlled_team",
                    "team_display_name": "故障通知验收",
                    "orchestrator_config": {
                        "agent_name": "coordinator",
                        "agent_display_name": "主理人",
                        "payload": {},
                    },
                    "members": [
                        {
                            "definition_id": controlled.json()["id"],
                            "resource_view_type": "private",
                            "resource_version": controlled.json()["resource_version"],
                        }
                    ],
                },
            )
            assert team_response.status_code == 201, team_response.text
            team_task = await app.state.egis_services.sessions.add_session(
                admin, team_response.json()["id"], "private"
            )
            native_session = await storage.read_session_unscoped(
                admin.user_id, team_task["agent_id"], team_task["session_id"]
            )
            native_team = await storage.get_team(admin.user_id, native_session.team_id)
            worker = native_team.data.members[0]
            failing_sessions.add(worker.session_id)
            wakeups = []
            queue_push = app.state.egis_bus.queue_push

            async def capture_wakeup(key, payload, **kwargs):
                if key == MessageBusKeys.wakeup_queue():
                    wakeups.append(payload)
                    return "test-captured-wakeup"
                return await queue_push(key, payload, **kwargs)

            with monkeypatch.context() as patch:
                patch.setattr(app.state.egis_bus, "queue_push", capture_wakeup)
                await app.state.chat_service.run(
                    admin.user_id, worker.session_id, worker.agent_id
                )
            notification = await app.state.egis_bus.queue_drain(
                MessageBusKeys.inbox(team_task["session_id"])
            )
            assert notification and "controlled" in str(notification)
            assert wakeups, "native worker setup failure must wake its leader"
            client.headers["Authorization"] = "Bearer " + login.json()["access_token"]
            assert (
                await client.get("/hub/nacos-skills/cards/unaudited-latest")
            ).status_code == 403
            assert (await client.post("/chat/", json=[])).status_code == 400
            created = await client.post(
                "/manager/agents",
                json={
                    "agent_name": "writer",
                    "agent_display_name": "写作专家",
                    "payload": {},
                },
            )
            assert created.status_code == 201, created.text
            task = await client.post(
                "/manager/tasks",
                json={
                    "definition_id": created.json()["id"],
                    "resource_view_type": "private",
                },
            )
            assert task.status_code == 201, task.text
            sid, aid = task.json()["session_id"], task.json()["agent_id"]
            initial_config = config.snapshot
            config.snapshot = config.snapshot.model_copy(
                update={
                    "access": config.snapshot.access.model_copy(
                        update={"allow_private_self_use": False}
                    )
                }
            )
            assert (await client.get(f"/manager/tasks/{sid}")).status_code == 403
            config.snapshot = initial_config
            assert (
                await client.post(
                    "/chat/",
                    json={"session_id": sid, "agent_id": "other-agent", "input": None},
                )
            ).status_code == 403
            assert (
                await client.get(f"/sessions/{sid}/messages", params={"agent_id": aid})
            ).status_code == 200
            project_id = task.json()["project_id"]
            protected = await client.delete(f"/manager/projects/{project_id}")
            assert protected.status_code == 403
            assert protected.json()["detail"] == "默认项目不能删除"
            assert (await client.get(f"/manager/tasks/{sid}")).status_code == 200
            project_files = f"/manager/projects/{project_id}/files"
            upload = await client.post(
                project_files,
                files={"file": ("report.md", b"# Real artifact\n")},
            )
            assert upload.status_code == 201, upload.text
            assert upload.json()["area"] == "uploads"
            duplicate = await client.post(
                project_files, files={"file": ("report.md", b"# replacement\n")}
            )
            assert duplicate.status_code == 409
            assert duplicate.json()["code"] == "upload_name_conflict"
            file_url = f"/manager/projects/{project_id}/file"
            file_query = {"area": "uploads", "path": "report.md"}
            assert (
                await client.get(file_url, params=file_query)
            ).content == b"# Real artifact\n"
            replaced = await client.post(
                project_files,
                params={"overwrite": "true"},
                files={"file": ("report.md", b"# replacement\n")},
            )
            assert replaced.status_code == 201
            assert replaced.json()["path"] == "report.md"
            assert (
                await client.get(file_url, params=file_query)
            ).content == b"# replacement\n"
            # 页面编辑只替换读取时的文件版本；缺少摘要、并发覆盖都要拒绝。
            opened = await client.get(file_url, params=file_query)
            saved = await client.put(
                file_url,
                params=file_query,
                content=b"# Edited\n",
                headers={"If-Match": opened.headers["etag"]},
            )
            assert saved.status_code == 200, saved.text
            assert (
                await client.get(file_url, params=file_query)
            ).content == b"# Edited\n"
            assert (
                await client.put(
                    file_url,
                    params=file_query,
                    content=b"stale",
                    headers={"If-Match": opened.headers["etag"]},
                )
            ).status_code == 409
            assert (
                await client.put(file_url, params=file_query, content=b"no checksum")
            ).status_code == 422
            limits = await client.get(
                f"/manager/projects/{project_id}/workbook-options"
            )
            assert limits.status_code == 200 and limits.json()["maxReferenceCells"] > 0
            app.state.egis_services.project_files.files.write(
                alice.user_id,
                task.json()["project_id"],
                "report.md",
                b"# Real artifact\n",
            )
            assert (
                await client.post(
                    f"/manager/projects/{project_id}/deliver",
                    json={"path": "report.md"},
                )
            ).status_code == 201
            artifact = await client.get(
                f"/manager/projects/{project_id}/file",
                params={"area": "artifacts", "path": "report.md"},
            )
            assert artifact.content == b"# Real artifact\n"
            from pathlib import Path

            preview_pdf = (
                Path(__file__).parents[2]
                / "egis-agentkit-frontend/tests/fixtures/preview.pdf"
            ).read_bytes()

            class Preview:
                calls = 0

                async def convert(self, content, filename):
                    self.calls += 1
                    assert content == b"office fixture"
                    return preview_pdf

            preview = Preview()
            app.state.egis_services.project_files.preview = preview
            office = await client.post(
                project_files,
                files={"file": ("测试 幻灯片.pptx", b"office fixture")},
            )
            assert office.status_code == 201
            rendered = await client.get(
                f"/manager/projects/{project_id}/preview",
                params={"area": "uploads", "path": office.json()["path"]},
            )
            assert rendered.status_code == 200 and rendered.content.startswith(b"%PDF-")
            assert rendered.headers["content-type"].startswith("application/pdf")
            assert preview.calls == 1

            assert (
                await client.get(
                    f"/manager/projects/{project_id}/file",
                    params={"area": "work", "path": "../../../../etc/passwd"},
                )
            ).status_code == 403
            other = await client.post(
                "/auth/login",
                json={
                    "user_id": "bob",
                    "password": "a-test-password-with-enough-entropy",
                },
            )
            client.headers["Authorization"] = "Bearer " + other.json()["access_token"]
            assert (
                await client.put(
                    file_url,
                    params=file_query,
                    content=b"other user",
                    headers={"If-Match": opened.headers["etag"]},
                )
            ).status_code == 404
            assert (
                await client.get(
                    f"/manager/projects/{project_id}/preview",
                    params={"area": "uploads", "path": office.json()["path"]},
                )
            ).status_code == 404
            assert preview.calls == 1

            assert (
                await client.get(
                    f"/manager/projects/{project_id}/file",
                    params={"area": "work", "path": "report.md"},
                )
            ).status_code == 404
    await nacos.http.aclose()
