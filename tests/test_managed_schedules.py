from datetime import UTC, datetime

import httpx
from agentscope.app._bus_ops import publish_session_event
from egis_agentkit_app.bootstrap import build_app
from identity_helpers import configure_test_issuer, signed_token
from test_app_http import Config, app_snapshot
from test_domain import identities

from egis_agentkit.client.nacos import NacosBootstrap, NacosClient
from egis_agentkit.service.schedule._worker import _schedule_result


async def test_managed_schedule_crud_uses_owner_and_retains_running_history(
    storage, tmp_path, monkeypatch
):
    configure_test_issuer(monkeypatch)
    monkeypatch.setenv(
        "TEST_DOWNLOAD", "download-secret-with-more-than-thirty-two-bytes"
    )
    monkeypatch.setenv("TEST_MODEL_KEY", "model-key")
    _, _, alice, bob = await identities(storage)
    config = Config(app_snapshot(tmp_path))
    async with NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos:
        app = await build_app(
            config, nacos, engine=storage.engine, manage_logging=False
        )
        kit = app.state.egis_kit
        project = await kit.services.projects.default(alice)
        body = {
            "name": "每周项目进度",
            "description": "汇总本周进度",
            "project_id": project["id"],
            "rule": {"kind": "weekly", "weekdays": [4], "time": "17:00"},
        }
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app), base_url="http://test"
        ) as client:
            alice_headers = {"Authorization": "Bearer " + signed_token(alice.user_id)}
            bob_headers = {"Authorization": "Bearer " + signed_token(bob.user_id)}
            result = await client.post(
                "/manager/schedules", headers=alice_headers, json=body
            )
            assert result.status_code == 201, result.text
            item = result.json()
            assert item["project_id"] == project["id"] and item["next_run_at"]
            assert item["timezone"] == "Asia/Shanghai"
            assert datetime.fromisoformat(item["next_run_at"]).hour == 9
            url = "/manager/schedules/" + item["id"]
            denied = await client.post(url + "/run", headers=bob_headers)
            assert denied.status_code == 404
            trial = await client.post(url + "/run", headers=alice_headers)
            assert trial.status_code == 202, trial.text
            run = trial.json()
            assert run["trigger_type"] == "test" and run["scheduled_for"] is None
            listed = await client.get("/manager/schedules", headers=alice_headers)
            assert listed.json()["items"][0]["next_run_at"] == item["next_run_at"]
            assert (await client.delete(url, headers=alice_headers)).status_code == 204
            history = await client.get("/manager/schedule-runs", headers=alice_headers)
            assert [row["id"] for row in history.json()["items"]] == [run["id"]]
            others = await client.get("/manager/schedule-runs", headers=bob_headers)
            assert others.json()["items"] == []


async def test_run_tracking_matches_the_inbox_hint_not_another_reply(storage, tmp_path):
    from egis_agentkit.service.session._message_bus_override import SessionMessageBus

    bus = SessionMessageBus(engine=storage.engine)
    async with bus:
        events = [
            {
                "type": "REPLY_END",
                "reply_id": "unrelated",
                "finished_reason": "completed",
            },
            {"type": "HINT_BLOCK", "block_id": "run-123", "reply_id": "expected"},
            {"type": "TEXT_BLOCK_DELTA", "reply_id": "expected", "delta": "本周进度"},
            {
                "type": "REPLY_END",
                "reply_id": "expected",
                "finished_reason": "completed",
            },
        ]
        for event in events:
            await publish_session_event(bus, "session-test", event)
        reply, status, summary, _ = await _schedule_result(
            bus, "session-test", "run-123", since=None, started_at=datetime.now(UTC)
        )
        assert (reply, status, summary) == ("expected", "succeeded", "本周进度")


async def test_trial_reaches_native_dispatcher_and_persists_reply(
    storage, tmp_path, monkeypatch
):
    import asyncio

    from agentscope.app._service import _chat
    from agentscope.app.message_bus import MessageBusKeys
    from agentscope.formatter import OpenAIChatFormatter
    from agentscope.message import TextBlock
    from agentscope.model import ChatModelBase, ChatResponse
    from agentscope.permission import PermissionMode
    from runtime_helpers import stub_sandbox_provider

    stub_sandbox_provider(monkeypatch)
    configure_test_issuer(monkeypatch)
    monkeypatch.setenv(
        "TEST_DOWNLOAD", "download-secret-with-more-than-thirty-two-bytes"
    )
    monkeypatch.setenv("TEST_MODEL_KEY", "model-key")
    _, _, alice, _ = await identities(storage)
    hints = []

    class ScheduleModel(ChatModelBase):
        formatter = OpenAIChatFormatter()

        async def __call__(self, messages, tools=None, tool_choice=None):
            for message in messages:
                hints.extend(block.hint for block in message.get_content_blocks("hint"))
            return ChatResponse(is_last=True, content=[TextBlock(text="已整理周报")])

    async def get_model(*_):
        return ScheduleModel(
            None, "scheduled-test", ScheduleModel.Parameters(), stream=False
        )

    monkeypatch.setattr(_chat, "get_model", get_model)
    config = Config(app_snapshot(tmp_path))
    async with NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos:
        app = await build_app(
            config, nacos, engine=storage.engine, manage_logging=False
        )
        async with app.router.lifespan_context(app):
            project = await app.state.egis_services.projects.default(alice)
            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
            ) as client:
                assert (await client.get("/manager/schedules")).status_code == 401
                client.headers["Authorization"] = "Bearer " + signed_token(
                    alice.user_id
                )
                body = {
                    "name": "周报",
                    "description": "整理项目周报",
                    "project_id": project["id"],
                    "rule": {"kind": "daily", "time": "09:00"},
                }
                preview = await client.post(
                    "/manager/schedules/preview", json={"rule": body["rule"]}
                )
                assert preview.status_code == 200 and preview.json()["next_run_at"]
                created = await client.post("/manager/schedules", json=body)
                assert created.status_code == 201, created.text
                schedule_id = created.json()["id"]
                trial = await client.post(f"/manager/schedules/{schedule_id}/run")
                assert trial.status_code == 202
                run_id = trial.json()["id"]

                async def finished(run_id):
                    for _ in range(80):
                        row = await storage.schedules.get_run(alice.user_id, run_id)
                        if row["status"] in {"succeeded", "failed"}:
                            return row
                        await asyncio.sleep(0.1)
                    events = []
                    if row["session_id"]:
                        events = await app.state.egis_bus.log_read(
                            MessageBusKeys.session_events(row["session_id"])
                        )
                    raise AssertionError((row, events, hints))

                run = await finished(run_id)
                assert run["status"] == "succeeded", run
                assert run["reply_id"] and run["summary"] == "已整理周报"
                assert "整理项目周报" in hints
                session = await app.state.egis_services.sessions.get_session_record(
                    alice,
                    run["session_id"],
                )
                assert session.origin.type == "schedule"
                assert session.state.permission_context.mode == PermissionMode.DONT_ASK
                assert (
                    await client.get(f"/manager/tasks/{run['session_id']}")
                ).status_code == 200
                message = await app.state.storage.get_message(
                    alice.user_id, session.id, run["reply_id"]
                )
                assert message.get_text_content() == "已整理周报"
                archived = await client.patch(
                    f"/manager/schedule-runs/{run_id}", json={"archived": True}
                )
                assert archived.status_code == 200 and archived.json()["archived_at"]

                # Emulate a task whose planned time arrived while the service was down.
                planned = datetime.now(UTC).replace(second=0, microsecond=0)
                automatic = await client.post(
                    "/manager/schedules",
                    json={
                        **body,
                        "enabled": False,
                        "timezone": "UTC",
                        "rule": {
                            "kind": "once",
                            "date": planned.date().isoformat(),
                            "time": planned.strftime("%H:%M"),
                        },
                    },
                )
                assert automatic.status_code == 201, automatic.text
                auto_id = automatic.json()["id"]
                await storage.schedules.update(
                    alice.user_id,
                    auto_id,
                    {
                        "enabled": True,
                        "next_run_at": planned,
                    },
                )
                async with asyncio.timeout(8):
                    while not (
                        runs := await storage.schedules.list_runs(
                            alice.user_id, schedule_id=auto_id
                        )
                    ):
                        await asyncio.sleep(0.1)
                scheduled = await finished(runs[0]["id"])
                assert scheduled["status"] == "succeeded"
                assert scheduled["trigger_type"] == "scheduled"
                assert scheduled["scheduled_for"] == planned
                assert (await storage.schedules.get(alice.user_id, auto_id))[
                    "next_run_at"
                ] is None


async def test_chat_schedule_tools_share_service_and_require_confirmation(
    storage,
    tmp_path,
    monkeypatch,
):
    from agentscope.message import ToolResultState
    from agentscope.permission import (
        PermissionBehavior,
        PermissionEngine,
        PermissionMode,
    )

    from egis_agentkit.bootstrap._init import init_agentkit
    from egis_agentkit.tool._schedule_tools import SCHEDULE_TOOL_NAMES

    monkeypatch.setenv("TEST_MODEL_KEY", "model-key")
    _, _, alice, _ = await identities(storage)
    kit = init_agentkit(
        Config(app_snapshot(tmp_path)),
        None,
        engine=storage.engine,
        manage_logging=False,
    )
    services = kit.services
    async with services.sessions.storage:
        await services.resources.seed_builtin_tools()
        project = await services.projects.default(alice)
        other = await services.projects.create(alice, "其他项目")
        personal = await services.personal.get_or_add_definition(alice)
        session = await services.sessions.add_session(
            alice,
            personal["id"],
            "private",
            project_id=project["id"],
            permission_mode="bypass",
        )

        async def resolve(scope):
            return {
                tool.name: tool
                for tool in await kit.tools(
                    scope.user_id, scope.agent_id, scope.session_id
                )
            }

        for name in SCHEDULE_TOOL_NAMES:
            await services.resources.install(alice, "tool", name)
        async with kit.bus.coordinator.bind(session["session_id"]) as scope:
            tools = await resolve(scope)
            assert tools.keys() == SCHEDULE_TOOL_NAMES
            permission = PermissionEngine(scope.session_record.state.permission_context)
            for tool in tools.values():
                assert (
                    await permission.check_permission(tool, {})
                ).behavior == PermissionBehavior.ASK

            body = {
                "name": "周报",
                "description": "整理本周情况",
                "rule": {"kind": "daily", "time": "09:00"},
            }
            create = tools["CreateScheduledTask"]
            created = await create.call(**body)
            assert created.state == ToolResultState.SUCCESS
            task_id = created.metadata["schedule_id"]
            task = await services.schedules.get(alice, task_id)
            assert task["project_id"] == project["id"]
            assert task["cron_expression"] == "0 9 * * *"
            assert (
                await create.call(**body, project_id=other["id"])
            ).state == ToolResultState.ERROR

            from egis_agentkit.service.schedule._rules import ScheduleSettings

            other_task = await services.schedules.create(
                alice,
                ScheduleSettings(project_id=other["id"], **body),
            )
            assert (
                await tools["RunScheduledTask"].call(task=other_task["id"])
            ).state == ToolResultState.ERROR
            assert (
                await tools["DeleteScheduledTask"].call(task=other_task["id"])
            ).state == ToolResultState.ERROR
            assert (await services.schedules.get(alice, other_task["id"]))[
                "id"
            ] == other_task["id"]

            run = tools["RunScheduledTask"]
            trial = await run.call(task=body["name"])
            assert trial.state == ToolResultState.SUCCESS
            assert (await services.schedules.list_runs(alice))[0][
                "id"
            ] == trial.metadata["run_id"]
            assert (await services.schedules.get(alice, task_id))[
                "next_run_at"
            ] == task["next_run_at"]

            drop = tools["DeleteScheduledTask"]
            assert (await drop.call(task=task_id)).state == ToolResultState.SUCCESS
            assert [item["id"] for item in await services.schedules.list(alice)] == [
                other_task["id"]
            ]
            assert len(await services.schedules.list_runs(alice)) == 1
            await services.resources.update_installation(
                alice, "tool", drop.name, False
            )
            assert drop.name not in await resolve(scope)

        scheduled = await services.sessions.add_session(
            alice,
            personal["id"],
            "private",
            project_id=project["id"],
            schedule_id=task_id,
            permission_mode=PermissionMode.DONT_ASK,
        )
        async with kit.bus.coordinator.bind(scheduled["session_id"]) as scope:
            assert await resolve(scope) == {}


async def test_chat_model_sees_selected_schedule_tool_without_legacy_group(
    storage,
    tmp_path,
    monkeypatch,
):
    import asyncio
    import json
    from datetime import timedelta

    from agentscope.agent import InjectionConfig
    from agentscope.app._service import _chat
    from agentscope.formatter import OpenAIChatFormatter
    from agentscope.message import AssistantMsg, HintBlock, TextBlock, ToolCallBlock
    from agentscope.model import ChatModelBase, ChatResponse
    from runtime_helpers import stub_sandbox_provider

    stub_sandbox_provider(monkeypatch)
    configure_test_issuer(monkeypatch)
    monkeypatch.setenv(
        "TEST_DOWNLOAD", "download-secret-with-more-than-thirty-two-bytes"
    )
    monkeypatch.setenv("TEST_MODEL_KEY", "model-key")
    _, _, alice, _ = await identities(storage)
    calls = asyncio.Queue()

    class ScheduleModel(ChatModelBase):
        formatter = OpenAIChatFormatter()

        async def __call__(self, messages, tools=None, tool_choice=None):
            schemas = {
                entry["function"]["name"]: entry["function"]["parameters"]
                for entry in (tools or [])
            }
            times = [
                hint.hint
                for message in messages
                for hint in message.get_content_blocks("hint")
                if isinstance(hint.hint, str) and "<current-time>" in hint.hint
            ]
            calls.put_nowait((schemas, times))
            if "CreateScheduledTask" in schemas:
                return ChatResponse(
                    is_last=True,
                    content=[
                        ToolCallBlock(
                            id="create-scheduled-task-approval",
                            name="CreateScheduledTask",
                            input=json.dumps(
                                {
                                    "name": "项目总结会",
                                    "description": "提醒我开项目总结会",
                                    "rule": {"kind": "daily", "time": "17:00"},
                                }
                            ),
                        )
                    ],
                )
            return ChatResponse(is_last=True, content=[TextBlock(text="收到")])

    async def get_model(*_):
        return ScheduleModel(
            None, "schedule-tool-test", ScheduleModel.Parameters(), stream=False
        )

    monkeypatch.setattr(_chat, "get_model", get_model)
    config = Config(app_snapshot(tmp_path))
    async with NacosClient(NacosBootstrap(server_url="http://unused.test")) as nacos:
        app = await build_app(
            config, nacos, engine=storage.engine, manage_logging=False
        )
        async with app.router.lifespan_context(app):
            services = app.state.egis_services
            assistant = await services.personal.get_or_add_definition(alice)
            session = await services.sessions.add_session(
                alice,
                assistant["id"],
                "private",
                name="定时任务工具验收",
                permission_mode="bypass",
            )
            persisted = await services.sessions.get_session_record(
                alice, session["session_id"]
            )
            persisted.state.tool_context.activated_groups.append("schedule_tools")
            previous_clock = (datetime.now(UTC) - timedelta(minutes=1)).strftime(
                "%Y-%m-%dT%H:%M:%S"
            )
            persisted.state.context.append(
                AssistantMsg(
                    name="personal",
                    content=[
                        HintBlock(
                            source=InjectionConfig().injection_source,
                            hint=f"<current-time>{previous_clock}</current-time><timezone>UTC</timezone>",
                        )
                    ],
                )
            )
            await app.state.storage.update_session_state(
                alice.user_id,
                session["agent_id"],
                session["session_id"],
                persisted.state,
            )

            async with httpx.AsyncClient(
                transport=httpx.ASGITransport(app=app),
                base_url="http://test",
                headers={"Authorization": "Bearer " + signed_token(alice.user_id)},
            ) as client:

                async def tools_for_request():
                    response = await client.post(
                        "/chat/",
                        json={
                            "session_id": session["session_id"],
                            "agent_id": session["agent_id"],
                            "input": {
                                "name": "user",
                                "role": "user",
                                "content": [
                                    {
                                        "type": "text",
                                        "text": "每天17点提醒我开项目总结会",
                                    },
                                ],
                            },
                        },
                    )
                    assert response.status_code == 200, response.text
                    return await asyncio.wait_for(calls.get(), timeout=8)

                original, times = await tools_for_request()
                assert "<timezone>Asia/Shanghai</timezone>" in times[-1]
                timestamp = (
                    times[-1]
                    .split("<current-time>", 1)[1]
                    .split("</current-time>", 1)[0]
                )
                observed = datetime.fromisoformat(timestamp)
                assert observed.utcoffset() == timedelta(hours=8)
                assert abs(datetime.now(UTC) - observed) < timedelta(seconds=30)
                assert "schedule_tools" not in original.get("reset_tools", {}).get(
                    "properties", {}
                )
                assert not {
                    "ScheduleCreate",
                    "ScheduleDelete",
                    "ScheduleList",
                    "ScheduleView",
                }.intersection(original)
                assert "CreateScheduledTask" not in original

                await services.resources.install(alice, "tool", "CreateScheduledTask")
                selected, _ = await tools_for_request()
                assert "CreateScheduledTask" in selected
                assert not {"RunScheduledTask", "DeleteScheduledTask"}.intersection(
                    selected
                )
                async with asyncio.timeout(8):
                    while not (
                        await services.sessions.get_session(
                            alice, session["session_id"]
                        )
                    )["is_waiting"]:
                        await asyncio.sleep(0.05)
                assert await services.schedules.list(alice) == []

                catalog = await services.resources.catalog_page(alice, "tool")
                assert not {"ScheduleList", "ScheduleView"}.intersection(
                    card["resource_id"] for card in catalog["items"]
                )
