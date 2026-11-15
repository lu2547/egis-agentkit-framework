"""Named services exposed to the hosting application, without HTTP dependencies."""

from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..service._composer import ComposerService
    from ..service._model import ModelService
    from ..service._session import SessionService
    from ..service._session_feedback import SessionFeedbackService
    from ..service._user import UserService
    from ..service.access._grant import GrantService
    from ..service.access._share import ShareService
    from ..service.agent._agent_manager import AgentManager
    from ..service.agent._personal_manager import PersonalManager
    from ..service.knowledge import KnowledgeManager
    from ..service.project._file_manager import ProjectFileManager
    from ..service.project._presentation_manager import ProjectPresentationManager
    from ..service.project._project_manager import ProjectManager
    from ..service.resource._catalog import ResourceService
    from ..service.resource._skill_import import SkillImportService
    from ..service.team._definition import TeamDefinition


@dataclass(slots=True)
class AgentKitServices:
    users: "UserService"
    agents: "AgentManager"
    personal: "PersonalManager"
    teams: "TeamDefinition"
    grants: "GrantService"
    sharing: "ShareService"
    models: "ModelService"
    projects: "ProjectManager"
    resources: "ResourceService"
    sessions: "SessionService"
    project_files: "ProjectFileManager"
    presentations: "ProjectPresentationManager"

    skill_imports: "SkillImportService | None" = None
    composer: "ComposerService | None" = None
    session_feedback: "SessionFeedbackService | None" = None
    knowledge: "KnowledgeManager | None" = None
