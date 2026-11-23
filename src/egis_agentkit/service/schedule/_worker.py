"""Claim due work in PostgreSQL and monitor replies from the native dispatcher."""

import asyncio
import logging
from datetime import UTC, datetime, timedelta

from agentscope.app._bus_ops import deliver_to_inbox
from agentscope.app.message_bus import MessageBusKeys
from agentscope.message import HintBlock

from egis_agentkit.errors import AgentKitError

from ._rules import ScheduleSettings, due_occurrence

logger = logging.getLogger(__name__)
_RUN_TIMEOUT = 1800
_POLL_SECONDS = 1
_WORKERS = 4
_SUMMARY_LIMIT = 4000
_TIMER_LOCK = "egis:schedules:timer"


async def _schedule_result(
    bus,
    session_id,
    run_id,
    *,
    since,
    started_at,
    on_reply=None,
    load_reply=None,
):
    key = MessageBusKeys.session_events(session_id)
    reply_id = None
    summary = ""
    terminal = {}
    remaining = max(0, _RUN_TIMEOUT - (datetime.now(UTC) - started_at).total_seconds())

    def result(event, text):
        reason = event["finished_reason"]
        status = "succeeded" if reason == "completed" else "failed"
        return (
            event["reply_id"],
            status,
            text[:_SUMMARY_LIMIT].strip() or None,
            (None if status == "succeeded" else reason),
        )

    try:
        async with asyncio.timeout(remaining):
            while True:
                events = await bus.log_read(key, since=since, max_count=200)
                for since, event in events:
                    event_type = event["type"]
                    if event_type == "HINT_BLOCK" and event.get("block_id") == run_id:
                        reply_id = event["reply_id"]
                        if on_reply is not None:
                            await on_reply(reply_id)
                    elif (
                        event_type == "TEXT_BLOCK_DELTA"
                        and event.get("reply_id") == reply_id
                    ):
                        summary += event["delta"][: _SUMMARY_LIMIT - len(summary)]
                    elif event_type == "REPLY_END":
                        if load_reply is not None:
                            terminal[event["reply_id"]] = event
                        elif event["reply_id"] == reply_id:
                            return result(event, summary)

                # Native ChatService may trim all but REPLY_END as soon as it commits.
                # Match the saved inbox hint before treating a reply as this run's result.
                if load_reply is not None:
                    for candidate, event in list(terminal.items()):
                        message = await load_reply(candidate)
                        if message is None:
                            continue
                        del terminal[candidate]
                        if candidate == reply_id or any(
                            block.id == run_id
                            for block in message.get_content_blocks("hint")
                        ):
                            return result(event, message.get_text_content())
                        if (
                            reply_id is None
                            and (event.get("error") or {}).get("type") == "setup"
                        ):
                            return candidate, "failed", None, "setup"
                if not events:
                    await asyncio.sleep(0.5)
    except TimeoutError:
        return reply_id, "failed", summary.strip() or None, "timeout"


def _plan(row, now):
    return due_occurrence(ScheduleSettings.from_record(row), row["next_run_at"], now)


class ScheduleWorker:
    def __init__(self, schedules, bus):
        self.schedules = schedules
        self.storage = schedules.storage
        self.bus = bus
        self._poller = None
        self._running = {}
        self._last_expiry = None

    async def start(self):
        self._poller = asyncio.create_task(self._poll(), name="egis-scheduled-tasks")

    async def close(self):
        tasks = [task for task in (self._poller, *self._running.values()) if task]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _poll(self):
        while True:
            try:
                await self.tick()
            except Exception:
                logger.exception("Scheduled task poll failed")
            await asyncio.sleep(_POLL_SECONDS)

    async def tick(self):
        now = datetime.now(UTC)
        if await self.bus.try_lock(_TIMER_LOCK, ttl_secs=30):
            try:
                if self._last_expiry is None or now - self._last_expiry > timedelta(
                    seconds=30
                ):
                    await self.storage.expire_running(
                        now - timedelta(seconds=_RUN_TIMEOUT + 60),
                        now,
                    )
                    self._last_expiry = now
                for schedule_id in await self.storage.due_ids(now):
                    try:
                        await self.storage.enqueue_due(
                            schedule_id,
                            now,
                            lambda row: _plan(row, now),
                        )
                    except Exception:
                        logger.exception(
                            "Scheduled task dispatch failed",
                            extra={"context": {"schedule_id": schedule_id}},
                        )
            finally:
                await self.bus.unlock(_TIMER_LOCK)
        for row in await self.storage.list_queued():
            if len(self._running) >= _WORKERS:
                break
            schedule_id = row["schedule_id"]
            if schedule_id not in self._running:
                task = asyncio.create_task(
                    self._execute(row["id"]), name=f"egis-schedule:{row['id']}"
                )
                self._running[schedule_id] = task
                task.add_done_callback(
                    lambda _, sid=schedule_id: self._running.pop(sid, None)
                )

    async def _execute(self, run_id):
        run = None
        context = {"context": {"run_id": run_id}}
        try:
            run = await self.storage.claim_run(run_id, datetime.now(UTC))
            if run is None:
                return
            logger.info("Scheduled run started", extra=context)
            async with asyncio.timeout(_RUN_TIMEOUT):
                reply, status, summary, code = await self._dispatch(run)
            message = {
                "setup": "运行环境准备失败，请查看会话",
                "error": "运行失败，请查看会话",
                "interrupted": "运行已中断",
                "exceed_max_iters": "运行达到最大迭代次数",
                "timeout": "运行等待超时",
            }.get(code)
            await self.storage.finish_run(
                run_id,
                datetime.now(UTC),
                status=status,
                reply_id=reply,
                summary=summary,
                error_code=code,
                error_message=message,
            )
            logger.info("Scheduled run ended (%s)", status, extra=context)
        except asyncio.CancelledError:
            if run is not None:
                await self._mark_failed(run_id, "interrupted", "运行中断，未重新执行")
            raise
        except AgentKitError as exc:
            logger.warning("Scheduled run rejected: %s", exc, extra=context)
            await self._mark_failed(run_id, "rejected", str(exc))
        except TimeoutError:
            logger.warning("Scheduled run timed out", extra=context)
            await self._mark_failed(run_id, "timeout", "运行等待超时")
        except Exception:
            logger.exception("Scheduled run failed", extra=context)
            await self._mark_failed(run_id, "internal", "运行失败，请查看服务日志")

    async def _mark_failed(self, run_id, code, message):
        try:
            await self.storage.finish_run(
                run_id,
                datetime.now(UTC),
                status="failed",
                error_code=code,
                error_message=message,
            )
        except Exception:
            logger.exception(
                "Scheduled run status update failed",
                extra={"context": {"run_id": run_id}},
            )

    async def _dispatch(self, run):
        session, instruction = await self.schedules.prepare_run(run)
        await self.storage.bind_run(run["id"], session.id)
        cursor = await self.bus.registry_get(
            MessageBusKeys.session_event_checkpoint(session.id),
            MessageBusKeys.SESSION_EVENT_CURSOR_FIELD,
        )
        await deliver_to_inbox(
            self.bus,
            user_id=run["user_id"],
            session_id=session.id,
            agent_id=session.agent_id,
            payload=HintBlock(
                id=run["id"], hint=instruction, source="schedule"
            ).model_dump(mode="json"),
        )

        async def bind_reply(reply_id):
            await self.storage.bind_run(run["id"], session.id, reply_id)

        async def load_reply(reply_id):
            return await self.schedules.sessions.storage.get_message(
                run["user_id"],
                session.id,
                reply_id,
            )

        return await _schedule_result(
            self.bus,
            session.id,
            run["id"],
            since=cursor,
            started_at=run["started_at"],
            on_reply=bind_reply,
            load_reply=load_reply,
        )
