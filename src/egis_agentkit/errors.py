class AgentKitError(Exception):
    """A safe domain error that may be returned to an API caller."""

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


class InvalidRequest(AgentKitError, ValueError):
    """输入不符合当前业务限制。"""

    def __init__(self, message: str):
        super().__init__(message, 422)


class Forbidden(AgentKitError):
    def __init__(self, message: str = "无权执行此操作"):
        super().__init__(message, 403)


class NotFound(AgentKitError):
    def __init__(self, message: str = "资源不存在或不可访问"):
        super().__init__(message, 404)


class Conflict(AgentKitError):
    def __init__(self, message: str = "配置已经变化，请刷新后重试"):
        super().__init__(message, 409)


class UploadNameConflict(Conflict):
    def __init__(self):
        super().__init__("同名文件已存在。")


class CollaborationSessionBusy(Conflict):
    """会话正在执行或收尾，释放协作操作所需的锁后可重试。"""

    code = "collaboration_session_busy"

    def __init__(self, *, session_id: str, agent_id: str):
        super().__init__("对话正在运行，请等待本轮完成后调整协作专家")
        self.session_id = session_id
        self.agent_id = agent_id


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
