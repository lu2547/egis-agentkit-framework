import httpx
import pytest
from egis_agentkit_app.bootstrap import build_app
from test_domain import identities

from egis_agentkit.config import ConfigSnapshot, NacosBootstrap, NacosClient


class Config:
    def __init__(self, snapshot):
        self.snapshot, self.is_connected, self.restart_required = snapshot, True, False

    async def start(self):
        pass

    async def close(self):
        pass


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
    config = Config(
        ConfigSnapshot.model_validate(
            {
                "config_version": 1,
                "runtime": {
                    "config_version": 1,
                    "database_url_env": "TEST_DB_URL",
                    "download_secret_env": "TEST_DOWNLOAD",
                    "workspace_root": str(tmp_path),
                    "release_roots": [str(tmp_path / "releases")],
                    "auth": {
                        "mode": "demo",
                        "issuer": "test",
                        "audience": "test",
                        "algorithm": "HS256",
                        "signing_secret_env": "TEST_JWT",
                        "demo_password_env": "TEST_LOGIN",
                    },
                },
                "models": {
                    "config_version": 1,
                    "default_key": "test",
                    "models": [
                        {
                            "key": "test",
                            "display_name": "测试模型",
                            "model": "test",
                            "api_key_env": "TEST_MODEL_KEY",
                            "model_class": "openai_chat",
                        }
                    ],
                },
                "policies": {"config_version": 1},
            }
        )
    )
    nacos = NacosClient(
        NacosBootstrap(server_url="http://unused.test"),
        http_client=httpx.AsyncClient(
            transport=httpx.MockTransport(lambda r: httpx.Response(503))
        ),
    )
    app = await build_app(config, nacos, engine=storage.engine)
    assert app.state.egis_services.tasks.projects is app.state.egis_services.projects
    assert app.state.egis_bus.coordinator.projects is app.state.egis_services.projects
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
            # A retired/missing assembly can fail after HTTP accepted the turn.
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
                    "agent_name": "retired",
                    "agent_display_name": "已撤下装配",
                    "payload": {
                        "implementation": {
                            "assembly_ref": "not-registered",
                            "assembly_version": "1.0.0",
                        }
                    },
                },
            )
            assert controlled.status_code == 201
            pending = await client.post(
                "/manager/tasks",
                json={
                    "definition_id": controlled.json()["id"],
                    "resource_view_type": "private",
                },
            )
            assert pending.status_code == 201
            failed_task = pending.json()
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
            import asyncio

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
                    "team_name": "retired_team",
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
                        }
                    ],
                },
            )
            assert team_response.status_code == 201, team_response.text
            team_task = await app.state.egis_services.tasks.create(
                admin, team_response.json()["id"], "private"
            )
            native_session = await storage.read_session_unscoped(
                admin.user_id, team_task["agent_id"], team_task["session_id"]
            )
            native_team = await storage.get_team(admin.user_id, native_session.team_id)
            worker = native_team.data.members[0]
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
            assert notification and "retired" in str(notification)
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
                    "policies": config.snapshot.policies.model_copy(
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
            upload = await client.post(
                f"/manager/tasks/{sid}/files",
                files={"file": ("report.md", b"# Real artifact\n")},
            )
            assert upload.status_code == 201, upload.text
            assert upload.json()["area"] == "uploads"
            app.state.egis_services.workspace.files.write(
                alice.user_id, task.json()["project_id"], "report.md", b"# Real artifact\n"
            )
            assert (
                await client.post(
                    f"/manager/tasks/{sid}/deliver", json={"path": "report.md"}
                )
            ).status_code == 201
            artifact = await client.get(
                f"/manager/tasks/{sid}/file",
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
            app.state.egis_services.workspace.preview = preview
            office = await client.post(
                f"/manager/tasks/{sid}/files",
                files={"file": ("测试 幻灯片.pptx", b"office fixture")},
            )
            assert office.status_code == 201
            rendered = await client.get(
                f"/manager/tasks/{sid}/preview",
                params={"area": "uploads", "path": office.json()["path"]},
            )
            assert rendered.status_code == 200 and rendered.content.startswith(b"%PDF-")
            assert rendered.headers["content-type"].startswith("application/pdf")
            workspace_office = await client.post(
                "/manager/workspace/files",
                files={"file": ("测试 幻灯片.pptx", b"office fixture")},
            )
            assert workspace_office.status_code == 201
            assert (
                await client.get(
                    "/manager/workspace/preview",
                    params={"path": workspace_office.json()["path"]},
                )
            ).status_code == 200
            assert preview.calls == 2

            assert (
                await client.get(
                    f"/manager/tasks/{sid}/file",
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
                await client.get(
                    f"/manager/tasks/{sid}/preview",
                    params={"area": "uploads", "path": office.json()["path"]},
                )
            ).status_code == 403
            assert preview.calls == 2

            assert (
                await client.get(
                    f"/manager/tasks/{sid}/file",
                    params={"area": "work", "path": "report.md"},
                )
            ).status_code == 403
    await nacos.http.aclose()
