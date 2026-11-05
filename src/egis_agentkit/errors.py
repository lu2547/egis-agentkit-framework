class AgentKitError(Exception):
    """A safe domain error that may be returned to an API caller."""

    def __init__(self, message: str, status_code: int = 400):
        super().__init__(message)
        self.status_code = status_code


class Forbidden(AgentKitError):
    def __init__(self, message: str = "无权执行此操作"):
        super().__init__(message, 403)


class NotFound(AgentKitError):
    def __init__(self, message: str = "资源不存在或不可访问"):
        super().__init__(message, 404)


class Conflict(AgentKitError):
    def __init__(self, message: str = "配置已经变化，请刷新后重试"):
        super().__init__(message, 409)
