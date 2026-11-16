"""工作区就绪失败的安全分类，保留失败层次而不暴露上游正文。"""

from egis_agentkit.errors import AgentKitError


class WorkspaceProbeError(AgentKitError):
    """标记实例、租约、网络、运行时或存储失败，供管理层决定重试和短路。"""

    def __init__(self, stage: str, reason: str, *, retryable: bool):
        self.stage, self.reason, self.retryable = stage, reason, retryable
        messages = {
            "instance": "沙箱实例状态或归属不正确",
            "lease": "工作区正在被其他实例使用",
            "network": "沙箱执行通道暂时不可用",
            "runtime": "沙箱运行环境未就绪",
            "storage": "项目存储未就绪",
        }
        super().__init__(messages[stage], 503)
