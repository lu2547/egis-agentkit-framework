"""将受邀专家的公开运行状态投影到组长已有的会话事件流。"""

import json
import logging
from datetime import datetime, timezone

from agentscope.app._service._session_projection import SessionProjection
from agentscope.app.message_bus import MessageBusKeys
from agentscope.event import EventType, ReplyFinishedReason

logger = logging.getLogger(__name__)
EVENT = "collaborator_progress"
REMOVED = "collaborator_progress_removed"
MAX_ACTIVITIES = 20
FINISHED = {"completed", "failed", "interrupted"}
OBSERVED_EVENTS = {
    EventType.REPLY_START,
    EventType.REPLY_END,
    EventType.TOOL_CALL_START,
    EventType.TOOL_RESULT_START,
    EventType.TOOL_RESULT_END,
    EventType.REQUIRE_USER_CONFIRM,
    EventType.REQUIRE_EXTERNAL_EXECUTION,
}


class CollaboratorProgress:
    def __init__(self, bus):
        self.bus = bus
        self.projection = SessionProjection(bus)

    async def list(self, parent_id):
        return await self.projection.list(parent_id, EVENT)

    async def dispatched(self, parent_id, member, *, repeat=False):
        try:
            await self._update(
                parent_id,
                member["worker_session_id"],
                member["native_agent_id"],
                member["display_name"],
                None,
                repeat=repeat,
            )
        except Exception:
            logger.exception("Failed to project collaborator dispatch")

    async def observe(self, parent_id, worker_id, agent_id, name, event):
        await self._update(parent_id, worker_id, agent_id, name, event)

    async def forget(self, parent_id, worker_id):
        try:
            await self.projection.delete(parent_id, EVENT, worker_id)
            await self.projection.publish(
                parent_id, REMOVED, {"worker_session_id": worker_id}
            )
        except Exception:
            logger.exception("Failed to clear collaborator progress")

    async def _update(
        self, parent_id, worker_id, agent_id, name, event, *, repeat=False
    ):
        namespace = MessageBusKeys.projection_namespace(parent_id)
        field = MessageBusKeys.projection_field(EVENT, worker_id)
        while True:
            raw = await self.bus.registry_get(namespace, field)
            previous = json.loads(raw) if raw is not None else None
            current = self._transition(previous, event, repeat)
            if current is None:
                return
            value = {
                **current,
                "worker_session_id": worker_id,
                "worker_agent_id": agent_id,
                "display_name": name,
                "revision": previous["revision"] + 1 if previous else 1,
            }
            if await self.bus.registry_set_if(
                namespace, field, json.dumps(value, ensure_ascii=False), expected=raw
            ):
                await self.projection.publish(parent_id, EVENT, value)
                return

    @staticmethod
    def _transition(previous, event, repeat):
        if event is None:
            if previous is None or (repeat and previous["status"] in FINISHED):
                return {
                    "status": "queued",
                    "activities": [],
                    "started_at": datetime.now(timezone.utc).isoformat(),
                }
            return None
        kind, reply_id = event["type"], event["reply_id"]
        matching = previous is not None and previous.get("reply_id") == reply_id
        # Late tool/confirmation events from this reply cannot reopen its terminal state.
        if matching and previous["status"] in FINISHED:
            return None
        if kind == EventType.REPLY_START:
            if event.get("role") != "assistant":
                return None
            if matching:
                return (
                    {**previous, "status": "running"}
                    if previous["status"] == "waiting"
                    else None
                )
            return {
                "reply_id": reply_id,
                "status": "running",
                "activities": [],
                "started_at": previous.get("started_at", event["created_at"])
                if previous and previous["status"] == "queued"
                else event["created_at"],
            }
        if kind == EventType.REPLY_END:
            status = {
                ReplyFinishedReason.COMPLETED: "completed",
                ReplyFinishedReason.INTERRUPTED: "interrupted",
            }.get(event.get("finished_reason"), "failed")
            return {
                "reply_id": reply_id,
                "status": status,
                "activities": previous["activities"] if matching else [],
                "started_at": previous.get("started_at", event["created_at"])
                if matching
                else event["created_at"],
                "finished_at": event["created_at"],
            }
        if not matching:
            return None
        if kind in {
            EventType.REQUIRE_USER_CONFIRM,
            EventType.REQUIRE_EXTERNAL_EXECUTION,
        }:
            return {**previous, "status": "waiting"}
        activities = previous["activities"]
        tool_id = event["tool_call_id"]
        if kind == EventType.TOOL_CALL_START:
            activities = [
                *activities,
                {"id": tool_id, "name": event["tool_call_name"], "state": "running"},
            ][-MAX_ACTIVITIES:]
        elif kind == EventType.TOOL_RESULT_END:
            activities = [
                {**activity, "state": event["state"]}
                if activity["id"] == tool_id
                else activity
                for activity in activities
            ]
        return {**previous, "status": "running", "activities": activities}
