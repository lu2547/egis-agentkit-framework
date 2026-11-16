"""覆盖原生 Agent 和 Session 读取，提供锁内已解析的配置。"""

from egis_agentkit.core.session._context import (
    build_agent_record,
    session_execution_context,
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
