"""覆盖原生 Agent 和 Session 读取，提供锁内已解析的配置。"""

from egis_agentkit.storage._postgres import AgentKitStorage

from ._session_lock_override import execution_scope, projected_agent


class ExecutionStorage(AgentKitStorage):
    async def get_session(self, user_id, agent_id, session_id):
        scope = execution_scope.get()
        if scope and (scope.user_id, scope.agent_id, scope.session_id) == (
            user_id,
            agent_id,
            session_id,
        ):
            return scope.session_record.model_copy(deep=True)
        return await super().get_session(user_id, agent_id, session_id)

    async def get_agent(self, user_id, agent_id):
        record = await super().get_agent(user_id, agent_id)
        scope = execution_scope.get()
        if (
            record is not None
            and scope
            and (scope.user_id, scope.agent_id) == (user_id, agent_id)
        ):
            return projected_agent(record, scope)
        return record
