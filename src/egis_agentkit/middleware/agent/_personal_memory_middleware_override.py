"""覆盖原生 ReMe 作业入口，分别追踪记忆检索与回写的实际耗时。

核心能力
- _run_job 只包裹原生作业调用，保留其参数、结果、并发和错误处理方式。
- 记忆失败只标记自身阶段；是否影响回复继续由原生中间件决定。
- 埋点不记录记忆正文、输入消息或模型凭据。
"""

from agentscope.middleware import ReMeMiddleware

from egis_agentkit.client._tracing import trace_phase


class PersonalMemoryMiddlewareOverride(ReMeMiddleware):
    """复用原生记忆能力，仅在已有作业边界增加观测。"""

    async def _run_job(self, name, **kwargs):
        """检索、回写分别建立阶段，具体执行完整复用父类。"""
        with trace_phase(f"memory.{name}", required=False):
            return await super()._run_job(name, **kwargs)
