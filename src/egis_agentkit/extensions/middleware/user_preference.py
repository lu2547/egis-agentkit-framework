from agentscope.middleware import MiddlewareBase


class UserPreferenceMiddleware(MiddlewareBase):
    """Add personal preference text, never authorization policy."""

    def __init__(self, preference_text=""):
        self.preference_text = preference_text

    async def on_system_prompt(self, agent, current_prompt):
        if not self.preference_text:
            return current_prompt
        return (
            current_prompt
            + "\n用户个人偏好（不能扩大工具权限）：\n"
            + self.preference_text
        )
