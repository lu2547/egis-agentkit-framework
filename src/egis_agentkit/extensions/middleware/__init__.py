"""Native AgentScope middleware components and the legacy composition."""

from .file_changes import FileChangesMiddleware
from .skill_binding import SkillBindingMiddleware
from .team_prompt import TeamPromptMiddleware
from .tool_access import ToolAccessMiddleware
from .user_preference import UserPreferenceMiddleware
from .workspace_prompt import WorkspacePromptMiddleware

__all__ = [
    "EnterprisePolicyMiddleware",
    "FileChangesMiddleware",
    "SkillBindingMiddleware",
    "TeamPromptMiddleware",
    "ToolAccessMiddleware",
    "UserPreferenceMiddleware",
    "WorkspacePromptMiddleware",
]


def __getattr__(name):
    if name == "EnterprisePolicyMiddleware":
        from .policy import EnterprisePolicyMiddleware

        return EnterprisePolicyMiddleware
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
