"""Persist transient execution progress alongside the existing session projections."""

import json
import logging

from agentscope.app._bus_ops import publish_session_event
from agentscope.app.message_bus import MessageBusKeys
from agentscope.event import CustomEvent

logger = logging.getLogger(__name__)
EVENT = "session_execution_phase"
FIELD = MessageBusKeys.projection_field(EVENT, "current")


async def get_execution_phase(bus, session_id):
    """Call only after ownership validation; expired worker leases hide stale progress."""
    context = {"session_id": session_id, "reply_id": None, "phase": None}
    try:
        raw = await bus.registry_get(
            MessageBusKeys.projection_namespace(session_id), FIELD
        )
        if raw is None or not await bus.is_locked(
            MessageBusKeys.session_lock(session_id)
        ):
            return None
        value = json.loads(raw)
        context.update(reply_id=value.get("reply_id"), phase=value.get("phase"))
        if (
            value.get("session_id") == session_id
            and value.get("phase") == "memory"
            and isinstance(value.get("reply_id"), str)
        ):
            return value
    except Exception:
        logger.exception(
            "Failed to read session execution phase", extra={"context": context}
        )
    return None


class SessionExecutionPhase:
    """Optional memory callback: registry and notification failures remain observational."""

    def __init__(self, bus):
        self.bus = bus

    async def __call__(self, session_id, reply_id, phase):
        if not session_id:
            return
        namespace = MessageBusKeys.projection_namespace(session_id)
        value = {"session_id": session_id, "reply_id": reply_id, "phase": phase}
        try:
            if phase is None:
                raw = await self.bus.registry_get(namespace, FIELD)
                if raw is not None:
                    previous = json.loads(raw)
                    if reply_id is not None and previous.get("reply_id") != reply_id:
                        return
                    value["reply_id"] = previous.get("reply_id")
                    if not await self.bus.registry_del_if(
                        namespace, FIELD, expected=raw
                    ):
                        return
                elif reply_id is None:
                    return
            else:
                await self.bus.registry_set(namespace, FIELD, json.dumps(value))
        except Exception:
            logger.exception(
                "Failed to persist session execution phase", extra={"context": value}
            )
        try:
            await publish_session_event(
                self.bus,
                session_id,
                CustomEvent(name=EVENT, value=value).model_dump(mode="json"),
            )
        except Exception:
            logger.exception(
                "Failed to publish session execution phase", extra={"context": value}
            )
