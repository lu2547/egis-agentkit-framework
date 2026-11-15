"""实现原生 MiddlewareBase，转发管理服务注入的运行回调。

核心能力
- on_reply / on_acting 转发事件流，保留异常、取消和事件顺序。
- on_system_prompt 转发提示词回调；未提供的回调直接使用原生流程。
- 不读取项目、文件、权限、配置或数据库。
"""

from agentscope.middleware import MiddlewareBase


class RuntimeCallbackMiddleware(MiddlewareBase):
    """通用回调接入点；业务对象及其生命周期由调用方管理。"""

    def __init__(self, *, reply=None, acting=None, system_prompt=None):
        self.reply = reply
        self.acting = acting
        self.system_prompt = system_prompt

    async def on_reply(self, agent, input_kwargs, next_handler):
        """按原顺序转发回复事件，不提前结束或缓存事件流。"""
        source = (
            self.reply(agent, input_kwargs, next_handler)
            if self.reply is not None
            else next_handler(**input_kwargs)
        )
        async for event in source:
            yield event

    async def on_acting(self, agent, input_kwargs, next_handler):
        """按原顺序转发工具事件，业务事务由注入回调完成。"""
        source = (
            self.acting(agent, input_kwargs, next_handler)
            if self.acting is not None
            else next_handler(**input_kwargs)
        )
        async for event in source:
            yield event

    async def on_system_prompt(self, agent, current_prompt):
        """有回调时返回其结果，否则保留已有提示词。"""
        if self.system_prompt is None:
            return current_prompt
        return await self.system_prompt(agent, current_prompt)
