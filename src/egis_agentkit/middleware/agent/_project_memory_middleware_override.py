"""让原生 ReMe 支持项目内 Agent 协作，并追踪记忆检索与回写。

核心能力
- 普通用户回合沿用原生 ReMe。
- Inbox 唤醒时先检索项目记忆，协作回合完成后回写本次产出的答复。
- 同项目的本地 Agent 并发首次使用时，只启动一次 ReMe。
"""

import asyncio
import json
import logging

from agentscope.app._tool import TeamSay
from agentscope.event import HintBlockEvent
from agentscope.message import AssistantMsg, TextBlock, ToolResultState, UserMsg
from agentscope.middleware import ReMeMiddleware

from egis_agentkit.client._tracing import trace_phase
from egis_agentkit.middleware.team._message import team_message_text

logger = logging.getLogger(__name__)


class ProjectMemoryMiddlewareOverride(ReMeMiddleware):
    """复用项目级 ReMe，为 Inbox 协作回合补齐检索和回写。"""

    def __init__(self, *, workspace_dir: str, parameters=None):
        super().__init__(workspace_dir=workspace_dir, parameters=parameters)
        self._startup_lock = asyncio.Lock()

    async def _ensure_started(self):
        """将同一项目的首次初始化串行化。"""
        async with self._startup_lock:
            await super()._ensure_started()

    async def on_reply(self, agent, input_kwargs, next_handler):
        """从 Inbox 事件读取本轮任务，不改变 Agent 的原生会话输入。"""
        if input_kwargs.get("inputs") is not None:
            async for event in super().on_reply(agent, input_kwargs, next_handler):
                yield event
            return

        previous_ids = {message.id for message in agent.state.context}
        tasks = []
        async for event in next_handler(**input_kwargs):
            yield event
            if isinstance(event, HintBlockEvent):
                task = team_message_text(event)
                if task is not None:
                    tasks.append(UserMsg(name="team", content=[TextBlock(text=task)]))
                    memory = await self._retrieve_for_team(task)
                    if memory is not None:
                        agent.state.context.append(memory)
                        previous_ids.add(memory.id)

        if tasks:
            replies = self._team_replies(
                message
                for message in agent.state.context
                if message.id not in previous_ids
            )
            if replies:
                await self._write_back(tasks + replies, self._session_id_of(agent))

    async def _retrieve_for_team(self, task):
        if self._parameters.mode == "agent_control":
            return None
        try:
            memories = await self._search(task)
        except Exception as exc:
            logger.warning("ReMe team search failed: %s", type(exc).__name__)
            return None
        return self._build_memory_message(memories) if memories else None

    @staticmethod
    def _team_replies(messages):
        """保存本轮回复和成功汇报的正文。"""
        messages = list(messages)
        delivered = {
            block.id
            for message in messages
            for block in message.get_content_blocks("tool_result")
            if block.name == TeamSay.name and block.state == ToolResultState.SUCCESS
        }
        replies = []
        for message in messages:
            if message.role != "assistant":
                continue
            text = message.get_text_content()
            if text:
                replies.append(
                    AssistantMsg(name=message.name, content=[TextBlock(text=text)])
                )
            for call in message.get_content_blocks("tool_call"):
                if call.name == TeamSay.name and call.id in delivered:
                    report = json.loads(call.input)["content"]
                    replies.append(
                        AssistantMsg(
                            name=message.name, content=[TextBlock(text=report)]
                        )
                    )
        return replies

    async def _run_job(self, name, **kwargs):
        """检索、回写分别建立阶段，具体执行完整复用父类。"""
        with trace_phase(f"memory.{name}", required=False):
            return await super()._run_job(name, **kwargs)
