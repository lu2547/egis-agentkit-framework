"""覆盖原生 Agent 和 Session 读取，提供锁内已解析的配置。"""

import asyncio

from egis_agentkit.client._tracing import trace_phase
from egis_agentkit.core.session._context import (
    build_agent_record,
    session_execution_context,
    verified_chat_authorization,
)
from egis_agentkit.storage._postgres import AgentKitStorage


class SessionExecutionStorage(AgentKitStorage):
    """为原生读取提供本轮上下文副本，业务持久化继续由原有 Storage 执行。"""

    async def get_session(self, user_id, agent_id, session_id):
        """身份匹配时返回锁内会话副本，其余情况查询原生记录。"""
        scope = session_execution_context.get()
        if scope and (scope.user_id, scope.agent_id, scope.session_id) == (
            user_id,
            agent_id,
            session_id,
        ):
            return scope.session_record.model_copy(deep=True)
        verified = verified_chat_authorization.get()
        if (
            verified
            and verified.session_record is not None
            and verified.request_task is asyncio.current_task()
            and (verified.user_id, verified.agent_id, verified.session_id)
            == (user_id, agent_id, session_id)
        ):
            return verified.session_record.model_copy(deep=True)
        return await super().get_session(user_id, agent_id, session_id)

    async def get_agent(self, user_id, agent_id):
        """读取原生记录，再由 Core 按本轮固定定义装配返回内容。"""
        record = await super().get_agent(user_id, agent_id)
        scope = session_execution_context.get()
        if (
            record is not None
            and scope
            and (scope.user_id, scope.agent_id) == (user_id, agent_id)
        ):
            return build_agent_record(record, scope)
        return record

    async def update_session_state(self, user_id, agent_id, session_id, state):
        """观察运行收尾的状态持久化，SQL 与事务仍由父类执行。"""
        with trace_phase("session.persist"):
            return await super().update_session_state(
                user_id, agent_id, session_id, state
            )
