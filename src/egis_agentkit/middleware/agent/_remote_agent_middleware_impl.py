"""实现 MiddlewareBase，在模型调用时执行远程专家并将结果交给原生流程。"""

from __future__ import annotations

import hashlib
import json
import logging
import re
from typing import Any, AsyncIterator, Awaitable, Callable, Sequence

from agentscope.agent import Agent
from agentscope.app._bus_ops import publish_session_event
from agentscope.app._tool import TeamSay
from agentscope.event import AgentEvent, CustomEvent
from agentscope.message import Msg, TextBlock, ToolCallBlock, ToolResultState, URLSource
from agentscope.middleware import MiddlewareBase
from agentscope.model import ChatResponse
from agentscope.tool import ToolResponse
from sqlalchemy.engine import RowMapping

from egis_agentkit.client.remote_agent._base import RemoteAgentClientBase
from egis_agentkit.service.agent._remote_agent import (
    RemoteAgentService,
)
from egis_agentkit.storage.model._remote_agent import (
    RemoteAttachmentRef,
    RemoteRunRequest,
    RemoteRunResult,
)
from egis_agentkit.storage.model.agent._remote_config import validate_endpoint

logger = logging.getLogger(__name__)
_TEAM_MESSAGE = re.compile(r'<team-message from="[^"]+">\n(.*)\n</team-message>$', re.S)
_REPORT_PREFIX = "remote-report-"


class RemoteAgentMiddlewareImpl(MiddlewareBase):
    def __init__(
        self,
        remote: RemoteAgentClientBase,
        session_id: str,
        leader_name: str | None,
        *,
        runtime: RemoteAgentService,
    ) -> None:
        super().__init__()
        self.remote = remote
        self.runtime = runtime
        self.session_id, self.leader_name = session_id, leader_name
        self._messages: list[Msg] = []
        self._seen_sources: set[str] = set()
        self._request: RemoteRunRequest | None = None

    async def on_reply(
        self,
        agent: Agent,
        input_kwargs: dict[str, Any],
        next_handler: Callable[..., AsyncIterator[AgentEvent | Msg]],
    ) -> AsyncIterator[AgentEvent | Msg]:
        inputs = input_kwargs.get("inputs")
        messages = inputs if isinstance(inputs, list) else [inputs]
        # Native input preparation rewrites unsupported media into workspace
        # hints. Only the original user-selected text and safe references go out.
        self._messages = [
            message.model_copy(deep=True)
            for message in messages
            if isinstance(message, Msg) and message.role == "user"
        ]
        # Native Inbox appends hints to the last assistant message, which may
        # belong to the previous reply when a follow-up wakes an idle worker.
        self._seen_sources = {
            hint.id
            for message in agent.state.context
            if message.role == "assistant"
            for hint in message.get_content_blocks("hint")
        }
        self._request = None
        async for event in next_handler(**input_kwargs):
            yield event

    def _build_next_request(self, context: Sequence[Msg]) -> RemoteRunRequest | None:
        texts, source_ids, attachments = [], [], []
        for msg in self._messages:
            if msg.role != "user" or msg.id in self._seen_sources:
                continue
            source_ids.append(msg.id)
            text = msg.get_text_content()
            if text and text.strip():
                texts.append(text)
            for block in msg.get_content_blocks("data"):
                if not isinstance(block.source, URLSource):
                    continue
                url = str(block.source.url)
                if validate_endpoint(url, {"https"}):
                    attachments.append(
                        RemoteAttachmentRef(
                            ref=url,
                            name=block.name or "attachment",
                            media_type=block.source.media_type,
                        )
                    )
        if self.leader_name is not None:
            for msg in context:
                if msg.role != "assistant":
                    continue
                for hint in msg.get_content_blocks("hint"):
                    if hint.id in self._seen_sources or not isinstance(hint.hint, str):
                        continue
                    try:
                        source = json.loads(hint.source or "{}")
                    except ValueError:
                        continue
                    if not isinstance(source, dict) or source.get("label") not in {
                        "team",
                        "team_message",
                    }:
                        continue
                    match = _TEAM_MESSAGE.search(hint.hint)
                    if match and match[1].strip():
                        source_ids.append(hint.id)
                        texts.append(match[1])
        if not texts and not attachments:
            return None
        digest = hashlib.sha256(
            json.dumps(
                [self.session_id, self.remote.connection_id, source_ids]
            ).encode()
        ).hexdigest()
        self._seen_sources.update(source_ids)
        return RemoteRunRequest(
            run_id=digest,
            conversation_id=self.session_id,
            input={"text": "\n\n".join(texts) or "请处理附带的文件。"},
            attachments=attachments,
        )

    async def on_model_call(
        self,
        agent: Agent,
        input_kwargs: dict[str, Any],
        next_handler: Callable[..., Awaitable[ChatResponse]],
    ) -> ChatResponse:
        reply_id = agent.state.reply_id
        reply_messages = [
            message
            for message in agent.state.context
            if message.role == "assistant" and message.id == reply_id
        ]
        if self.leader_name is not None:
            report_call = next(
                (
                    call
                    for message in reversed(reply_messages)
                    for call in reversed(message.get_content_blocks("tool_call"))
                    if call.name == "TeamSay" and call.id.startswith(_REPORT_PREFIX)
                ),
                None,
            )
            if report_call is not None:
                row = await self._get_report_record(report_call)
                if not row or row["reported_at"] is None:
                    raise RuntimeError("远程专家结果未能通过 TeamSay 汇报给组长")
                result = RemoteRunResult.model_validate(row["result"])
                upcoming = self._build_next_request(agent.state.context)
                if upcoming is None:
                    return ChatResponse(
                        content=[TextBlock(text=result.output.text)], is_last=True
                    )
                self._request = upcoming
        if self._request is None:
            self._request = self._build_next_request(agent.state.context)
        if self._request is None:
            return ChatResponse(
                content=[TextBlock(text="请提供要交给远程专家的任务。")], is_last=True
            )

        if (
            self.leader_name is not None
            and type(await agent.toolkit.get_tool("TeamSay")) is not TeamSay
        ):
            raise RuntimeError("远程专家成员缺少已授权的原生 TeamSay")

        async def progress(message: str) -> None:
            try:
                await publish_session_event(
                    self.runtime.bus,
                    self.session_id,
                    CustomEvent(
                        name="remote_agent_progress",
                        value={
                            "reply_id": reply_id,
                            "run_id": self._request.run_id,
                            "message": message,
                        },
                    ).model_dump(mode="json"),
                )
            except Exception:
                logger.warning("Remote expert progress could not be published")

        result = await self.runtime.send_request(self.remote, self._request, progress)
        if result.status == "timed_out":
            raise TimeoutError(result.error.message)
        if result.output is None:
            raise RuntimeError(
                f"远程专家执行失败 [{result.error.code}]: {result.error.message}"
            )
        if self.leader_name is None:
            return ChatResponse(
                content=[TextBlock(text=result.output.text)], is_last=True
            )
        return ChatResponse(
            content=[
                ToolCallBlock(
                    id=_REPORT_PREFIX + result.run_id,
                    name="TeamSay",
                    input=json.dumps(
                        {"to": self.leader_name, "content": result.output.text},
                        ensure_ascii=False,
                    ),
                )
            ],
            is_last=True,
        )

    async def _get_report_record(self, tool_call: ToolCallBlock) -> RowMapping | None:
        run_id = tool_call.id.removeprefix(_REPORT_PREFIX)
        row = await self.runtime.runs.get_request(run_id)
        if row is not None and (
            row["conversation_id"] == self.session_id
            and row["connection_id"] == self.remote.connection_id
        ):
            return row
        return None

    async def on_acting(
        self,
        agent: Agent,
        input_kwargs: dict[str, Any],
        next_handler: Callable[..., AsyncIterator[ToolResponse]],
    ) -> AsyncIterator[ToolResponse]:
        call = input_kwargs["tool_call"]
        row = (
            await self._get_report_record(call)
            if self.leader_name is not None
            and call.name == "TeamSay"
            and call.id.startswith(_REPORT_PREFIX)
            else None
        )
        if (
            row is not None
            and type(await agent.toolkit.get_tool("TeamSay")) is not TeamSay
        ):
            raise RuntimeError("远程专家成员缺少已授权的原生 TeamSay")
        if row is not None and row["reported_at"] is not None:
            # A resumed reply still needs the native success block, but the
            # leader already received the previously confirmed report.
            yield ToolResponse(
                content=[TextBlock(text="TeamSay: 已汇报给组长。")],
                state=ToolResultState.SUCCESS,
            )
            return
        async for response in next_handler(**input_kwargs):
            if (
                row is not None
                and isinstance(response, ToolResponse)
                and response.state == ToolResultState.SUCCESS
            ):
                await self.runtime.runs.update_reported(row["run_id"])
            yield response
